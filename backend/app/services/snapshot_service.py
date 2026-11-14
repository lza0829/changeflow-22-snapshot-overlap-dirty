from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from app.config import FIXTURES_DIR
from app.db import connect
from app.engines.parse_change import parse_change_fixture
from app.modules.snapshot_catchup_race.engine import (
    OVERLAP_DISPOSITION,
    SNAPSHOT_LOADED_DISPOSITION,
    catchup_status,
    classify_catchup_event,
    fold_catchup_sink,
    fold_expected_sink,
    overlap_only_keys,
    parse_snapshot_fixture,
)
from app.repositories import cursors as cursor_repo
from app.repositories import ledger as ledger_repo
from app.repositories import runs as runs_repo
from app.repositories import sink as sink_repo
from app.repositories import snapshots as snapshot_repo
from app.repositories import streams as streams_repo

# 对外公布的重叠区消化规则（同时写进快照记录 report 与界面）
PUBLISHED_RULES = (
    "1) seq <= watermark_seq 的 live 事件已折叠进快照，一律记 "
    "skipped_snapshot_overlap 跳过，不重放（快照中缺席=已删除，重放会造脏行/复活）；"
    "2) watermark 之后且已应用过的 seq 幂等吞掉（skipped_duplicate）；"
    "3) 其余 live 变更按 upsert/delete 幂等落到 UNIQUE(stream_id, entity_key) 的同一实体键，"
    "禁止同键多出脏行；游标推进到 live 最大 seq 才标记 caught_up。"
)


def list_snapshot_fixtures() -> list[str]:
    # 快照夹具以 _snapshot.jsonl 结尾；CDC 夹具（*_cdc.jsonl）不进快照下拉
    return sorted(p.name for p in FIXTURES_DIR.glob("*_snapshot.jsonl"))


def _fixture_path(fixture_name: str) -> Path:
    # 只取文件名，杜绝路径穿越
    return FIXTURES_DIR / Path(fixture_name).name


def import_snapshot(stream_id: int, fixture_name: str) -> dict[str, Any]:
    conn = connect()
    try:
        stream = streams_repo.get_stream(conn, stream_id)
        if not stream:
            raise ValueError("stream not found")
        if stream.get("snapshot_import_id"):
            raise ValueError("该流已有快照导入记录；请先重置该 Lane 再导入新快照")
        # 只有空 Lane 才能导入快照：若该流已经跑过常规 Apply（账本/applied_seqs/
        # Sink 有痕迹），快照态会和已应用态打架，重叠区规则也无从裁决。
        if _ledger_distinct_seqs(conn, stream_id, "applied") or applied_seqs_for(conn, stream_id):
            raise ValueError("该流已有 live 应用痕迹（账本/applied_seqs）；请先重置该 Lane 再导入快照")
        if sink_repo.count_for_stream(conn, stream_id):
            raise ValueError("该流 Sink 非空（已有实体行）；请先重置该 Lane 再导入快照")

        snapshot = parse_snapshot_fixture(_fixture_path(fixture_name))
        if snapshot["entity_type"] != stream["entity_type"]:
            raise ValueError(
                f"快照实体类型 {snapshot['entity_type']} 与流类型 {stream['entity_type']} 不一致"
            )

        import_id = snapshot_repo.create_import(
            conn,
            stream_id=stream_id,
            snapshot_id=snapshot["snapshot_id"],
            fixture_path=Path(fixture_name).name,
            snapshot_ts=snapshot["snapshot_ts"],
            watermark_seq=snapshot["watermark_seq"],
            row_count=len(snapshot["rows"]),
        )

        # 快照态直接灌进同一 Sink 底座；UNIQUE(stream_id, entity_key) 兜底防脏行
        loaded_keys: list[str] = []
        for row in snapshot["rows"]:
            sink_repo.upsert(
                conn,
                stream_id=stream_id,
                entity_key=row["entity_key"],
                payload=row["payload"],
                seq=row["last_seq"],
                event_ts=row["last_event_ts"],
            )
            ledger_repo.insert_ledger(
                conn,
                stream_id=stream_id,
                run_id=None,
                event={
                    "seq": row["last_seq"],
                    "op": "upsert",
                    "entity_key": row["entity_key"],
                    "event_ts": row["last_event_ts"],
                    "payload": row["payload"],
                },
                disposition=SNAPSHOT_LOADED_DISPOSITION,
            )
            loaded_keys.append(row["entity_key"])

        # 游标不预支：仍从 0 开始，重叠区由 watermark 规则显式跳过并记账
        streams_repo.set_snapshot_import(conn, stream_id, import_id)
        record = snapshot_repo.get_import(conn, import_id)
        record["report"] = {
            "rules": PUBLISHED_RULES,
            "note": snapshot["note"],
            "loaded_entity_keys": loaded_keys,
            "sink_count_after_import": sink_repo.count_for_stream(conn, stream_id),
        }
        snapshot_repo.update_catchup_progress(
            conn,
            import_id=import_id,
            status="imported",
            live_candidate_count=0,
            overlap_skipped_count=0,
            applied_count=0,
            duplicate_skipped_count=0,
            cursor_after=0,
            caught_up=False,
            report=record["report"],
        )
        conn.commit()
        return snapshot_repo.get_import(conn, import_id) or {}
    finally:
        conn.close()


def catch_up(stream_id: int, limit: int | None = None) -> dict[str, Any]:
    conn = connect()
    try:
        stream = streams_repo.get_stream(conn, stream_id)
        if not stream:
            raise ValueError("stream not found")
        import_id = stream.get("snapshot_import_id")
        if not import_id:
            raise ValueError("该流没有快照导入，请先导入快照再追 live")
        record = snapshot_repo.get_import(conn, import_id)
        if not record:
            raise ValueError("snapshot import record missing")
        if record["status"] == "caught_up":
            raise ValueError("已追上（caught_up），无需重复追平；如需重跑请先重置该 Lane")

        watermark = int(record["watermark_seq"])
        events = parse_change_fixture(FIXTURES_DIR / stream["fixture_path"])
        if limit is not None and limit >= 0:
            batch = events[:limit]
        else:
            batch = events
        live_max_seq = max(int(e["seq"]) for e in events)

        cursor_before = int(stream["apply_cursor"] or 0)
        run_id = runs_repo.create_run(conn, stream_id, cursor_before)

        run_applied = 0
        run_overlap = 0
        run_duplicate = 0
        cursor = cursor_before
        dispositions: list[str] = []

        for event in batch:
            seq = int(event["seq"])
            already = cursor_repo.is_applied(conn, stream_id, seq)
            decision = classify_catchup_event(
                seq=seq, already_applied=already, watermark_seq=watermark
            )

            if decision == OVERLAP_DISPOSITION:
                # 重叠区只记账不动 Sink：快照态已含该 seq 的结论，重放旧 upsert
                # 会覆盖更新态/让已删除实体复活，重放旧 delete 又会抹掉快照行，
                # 同一键在 Sink 里多出脏行。跳过是纯粹读侧裁决，幂等且无副作用。
                ledger_repo.insert_ledger(
                    conn, stream_id=stream_id, run_id=run_id, event=event, disposition=decision
                )
                # 该 seq 已被快照折叠、不需要再回放，位置算「已消费」，游标可随之
                # 前移到快照界内；但它绝不是「已追上」，追上仍要求 live 全部消化。
                cursor = max(cursor, seq)
                run_overlap += 1
                dispositions.append(decision)
                continue

            if decision == "skipped_duplicate":
                ledger_repo.insert_ledger(
                    conn, stream_id=stream_id, run_id=run_id, event=event, disposition=decision
                )
                run_duplicate += 1
                dispositions.append(decision)
                continue

            if event["op"] == "upsert":
                sink_repo.upsert(
                    conn,
                    stream_id=stream_id,
                    entity_key=event["entity_key"],
                    payload=event.get("payload") or {},
                    seq=seq,
                    event_ts=event["event_ts"],
                )
            else:
                sink_repo.delete(conn, stream_id=stream_id, entity_key=event["entity_key"])

            cursor_repo.mark_applied(conn, stream_id, seq, run_id)
            cursor = max(cursor, seq)
            ledger_repo.insert_ledger(
                conn, stream_id=stream_id, run_id=run_id, event=event, disposition="applied"
            )
            run_applied += 1
            dispositions.append("applied")

        # 游标只在事件真正被消费（applied / 重叠跳过）后推进；重复吞掉不推进，
        # 更不会预支到 live 末端。
        streams_repo.set_cursor(conn, stream_id, cursor)

        all_live_seqs = {int(e["seq"]) for e in events}
        consumed_seqs = _consumed_seqs(conn, stream_id)
        caught_up = catchup_status(
            cursor=cursor,
            live_max_seq=live_max_seq,
            processed_seqs=consumed_seqs,
            all_live_seqs=all_live_seqs,
        ) == "caught_up"
        status = "caught_up" if caught_up else "catching_up"

        run_report = {
            "snapshot_import_id": import_id,
            "watermark_seq": watermark,
            "overlap_skipped": run_overlap,
            "duplicate_skipped": run_duplicate,
            "consumed_seqs": sorted(consumed_seqs),
            "unconsumed_seqs": sorted(all_live_seqs - consumed_seqs),
            "caught_up": caught_up,
            "live_max_seq": live_max_seq,
            "cursor_after": cursor,
            "limit": limit,
            "dispositions": dispositions,
            "rules": PUBLISHED_RULES,
        }
        runs_repo.finish_run(
            conn,
            run_id,
            status=status,
            loaded=len(batch),
            applied=run_applied,
            quarantined=0,
            skipped=run_overlap + run_duplicate,
            cursor_after=cursor,
            report=run_report,
        )

        # 快照记录上的计数一律按账本 distinct seq 重算：分批重扫不会重复计数，
        # 记录数天然能和账本（去重 seq 集合）、applied_seqs 对上号
        distinct_overlap = _ledger_distinct_seqs(conn, stream_id, OVERLAP_DISPOSITION)
        distinct_duplicate = _ledger_distinct_seqs(conn, stream_id, "skipped_duplicate")
        distinct_applied = applied_seqs_for(conn, stream_id)
        distinct_catchup_seqs = (
            distinct_overlap | distinct_duplicate | distinct_applied
        ) & all_live_seqs
        merged_report = dict(record.get("report") or {})
        history = merged_report.get("catchup_runs", [])
        history.append({"run_id": run_id, **run_report})
        merged_report["catchup_runs"] = history
        snapshot_repo.update_catchup_progress(
            conn,
            import_id=import_id,
            status=status,
            live_candidate_count=len(distinct_catchup_seqs),
            overlap_skipped_count=len(distinct_overlap),
            applied_count=len(distinct_applied),
            duplicate_skipped_count=len(distinct_duplicate),
            cursor_after=cursor,
            caught_up=caught_up,
            report=merged_report,
        )
        conn.commit()
        return {
            "import": snapshot_repo.get_import(conn, import_id),
            "run": runs_repo.get_run(conn, run_id),
            "reconciliation": reconcile(conn, stream_id),
        }
    finally:
        conn.close()


def reset_lane(stream_id: int) -> dict[str, Any]:
    """清空某路流的快照/Live 痕迹，便于重新演示竞态。"""
    conn = connect()
    try:
        stream = streams_repo.get_stream(conn, stream_id)
        if not stream:
            raise ValueError("stream not found")
        conn.execute("DELETE FROM change_ledger WHERE stream_id=?", (stream_id,))
        conn.execute("DELETE FROM quarantine WHERE stream_id=?", (stream_id,))
        conn.execute("DELETE FROM applied_seqs WHERE stream_id=?", (stream_id,))
        conn.execute("DELETE FROM sink_records WHERE stream_id=?", (stream_id,))
        conn.execute("DELETE FROM apply_runs WHERE stream_id=?", (stream_id,))
        conn.execute("DELETE FROM snapshot_imports WHERE stream_id=?", (stream_id,))
        streams_repo.set_cursor(conn, stream_id, 0)
        streams_repo.set_snapshot_import(conn, stream_id, None)
        conn.commit()
        return {"stream_id": stream_id, "reset": True}
    finally:
        conn.close()


def _ledger_disposition_counts(conn, stream_id: int) -> dict[str, int]:
    rows = conn.execute(
        "SELECT disposition, COUNT(*) c FROM change_ledger WHERE stream_id=? GROUP BY disposition",
        (stream_id,),
    ).fetchall()
    return {r["disposition"]: int(r["c"]) for r in rows}


def _ledger_distinct_seqs(conn, stream_id: int, disposition: str) -> set[int]:
    rows = conn.execute(
        "SELECT DISTINCT seq FROM change_ledger WHERE stream_id=? AND disposition=?",
        (stream_id, disposition),
    ).fetchall()
    return {int(r["seq"]) for r in rows}


def applied_seqs_for(conn, stream_id: int) -> set[int]:
    return {
        int(r["seq"])
        for r in conn.execute(
            "SELECT seq FROM applied_seqs WHERE stream_id=?", (stream_id,)
        ).fetchall()
    }


def _consumed_seqs(conn, stream_id: int) -> set[int]:
    """已被真正消化（applied 或重叠跳过）的 seq；重复吞掉不算推进。"""
    rows = conn.execute(
        "SELECT DISTINCT seq FROM change_ledger WHERE stream_id=? AND disposition IN (?,?)",
        (stream_id, "applied", OVERLAP_DISPOSITION),
    ).fetchall()
    return {int(r["seq"]) for r in rows}


def _ledger_snapshot_state(conn, stream_id: int) -> dict[str, dict[str, Any]]:
    """从账本快照装载行重建快照实体态（持久化事实，不依赖夹具文件）。"""
    rows = conn.execute(
        "SELECT seq, entity_key, payload_json FROM change_ledger "
        "WHERE stream_id=? AND disposition=?",
        (stream_id, SNAPSHOT_LOADED_DISPOSITION),
    ).fetchall()
    return {
        str(r["entity_key"]): json.loads(r["payload_json"] or "{}")
        for r in rows
    }


def _status_consistent(
    *, status: str, derived_status: str, consumed_seqs: set[int], cursor: int
) -> bool:
    """状态机互证：``imported`` 只能出现在未消化任何 live 事件时，其余必须与推出值一致。"""
    if status == "imported":
        return derived_status == "catching_up" and not consumed_seqs and cursor == 0
    return status == derived_status


def reconcile(conn, stream_id: int) -> dict[str, Any]:
    """四方对账：快照记录 / 账本 / Sink / 游标（追平中与追上后都成立）。"""
    stream = streams_repo.get_stream(conn, stream_id)
    if not stream:
        raise ValueError("stream not found")
    import_id = stream.get("snapshot_import_id")
    record = snapshot_repo.get_import(conn, import_id) if import_id else None
    events = parse_change_fixture(FIXTURES_DIR / stream["fixture_path"])
    live_max_seq = max(int(e["seq"]) for e in events)
    all_live_seqs = {int(e["seq"]) for e in events}

    ledger_counts = _ledger_disposition_counts(conn, stream_id)
    applied_seqs = applied_seqs_for(conn, stream_id)
    overlap_seqs = _ledger_distinct_seqs(conn, stream_id, OVERLAP_DISPOSITION)
    duplicate_seqs = _ledger_distinct_seqs(conn, stream_id, "skipped_duplicate")
    loaded_seqs = _ledger_distinct_seqs(conn, stream_id, SNAPSHOT_LOADED_DISPOSITION)
    consumed_seqs = applied_seqs | overlap_seqs

    actual_sink = {
        r["entity_key"]: r["payload"]
        for r in sink_repo.list_sink_for_stream(conn, stream_id)
    }
    actual_keys = set(actual_sink)
    snapshot_state = _ledger_snapshot_state(conn, stream_id)
    snapshot_keys = set(snapshot_state)

    # 追平中/追上后应逐键一致的预期 Sink：快照态 + 仅已 applied 的 live 折叠。
    # 重叠区跳过若不碰 Sink，实际态就该与它严格相等（键集 + payload 全等）。
    expected_now = fold_catchup_sink(snapshot_state, events, applied_seqs=applied_seqs)
    expected_final = fold_expected_sink(events)

    checks: list[dict[str, Any]] = []

    def _check(name: str, ok: bool, detail: str) -> None:
        checks.append({"name": name, "ok": bool(ok), "detail": detail})

    cursor = int(stream["apply_cursor"])
    status = record["status"] if record else "no_snapshot"
    max_applied = max(applied_seqs) if applied_seqs else 0
    # 游标语义 = 「已消费到哪」：applied 或按公布规则重叠跳过都算消费，
    # 重复吞掉不算（它没有推进任何新位置）。
    expected_cursor = max(consumed_seqs) if consumed_seqs else 0

    if record:
        watermark = int(record["watermark_seq"])
        expected_overlap = {int(e["seq"]) for e in events if int(e["seq"]) <= watermark}
        expected_apply = {int(e["seq"]) for e in events if int(e["seq"]) > watermark}
        # 状态机事实：只有「全量 live seq 已消化 + 游标到末端」才允许 caught_up
        derived_status = catchup_status(
            cursor=cursor,
            live_max_seq=live_max_seq,
            processed_seqs=consumed_seqs,
            all_live_seqs=all_live_seqs,
        )

        _check(
            "快照行数 = 账本 snapshot_loaded 行数",
            int(record["row_count"]) == ledger_counts.get(SNAPSHOT_LOADED_DISPOSITION, 0),
            f"记录 {record['row_count']} / 账本 {ledger_counts.get(SNAPSHOT_LOADED_DISPOSITION, 0)}",
        )
        _check(
            "快照记录状态 = 状态机按游标/已消化 seq 推出的状态",
            _status_consistent(
                status=status,
                derived_status=derived_status,
                consumed_seqs=consumed_seqs,
                cursor=cursor,
            ),
            f"记录 {status} / 推出 {derived_status}（游标 {cursor}，"
            f"已消化 {len(consumed_seqs)}/{len(all_live_seqs)}）",
        )
        _check(
            "追平中/已导入不得有追上时间，caught_up 时必须有",
            (record["caught_up_at"] is None) == (status != "caught_up"),
            f"状态 {status} / caught_up_at {record['caught_up_at']}",
        )
        _check(
            "重叠跳过 seq 全部 <= watermark（没有误把 live 变更当重叠）",
            overlap_seqs <= expected_overlap,
            f"跳过 {sorted(overlap_seqs)} / 快照界 {watermark}",
        )
        _check(
            "applied seq 全部 > watermark（重叠区绝不重放）",
            applied_seqs <= expected_apply,
            f"applied {sorted(applied_seqs)} / 需 > {watermark}",
        )
        _check(
            "重叠跳过与 applied 的 seq 集合互斥",
            not (overlap_seqs & applied_seqs),
            f"交集 {sorted(overlap_seqs & applied_seqs)}",
        )
        _check(
            "快照记录 overlap 累计 = 账本去重 overlap seq 数",
            int(record["overlap_skipped_count"]) == len(overlap_seqs),
            f"记录 {record['overlap_skipped_count']} / 账本 {len(overlap_seqs)}",
        )
        _check(
            "快照记录 applied 累计 = applied_seqs 数",
            int(record["applied_count"]) == len(applied_seqs),
            f"记录 {record['applied_count']} / applied_seqs {len(applied_seqs)}",
        )
        _check(
            "快照记录游标 = 流 apply_cursor",
            int(record["cursor_after"]) == cursor,
            f"记录 {record['cursor_after']} / 流 {cursor}",
        )
        # 游标相对快照界的互证：重叠区已消费的 seq 只到快照界为止，live applied
        # 的 seq 全在快照界之后。因此游标必须等于「已消费最大 seq」，且一旦有
        # applied 就必须越过 watermark；一个 applied 都没有时只能停在界内。
        _check(
            "游标 = 已消费最大 seq（applied/重叠跳过，重复不算；不得伪造）",
            cursor == expected_cursor,
            f"游标 {cursor} / 已消费最大 {expected_cursor} / max_applied {max_applied}",
        )
        if applied_seqs:
            _check(
                "有 applied 时游标越过快照界（watermark）",
                cursor > watermark,
                f"游标 {cursor} / watermark {watermark} / max_applied {max_applied}",
            )
        else:
            _check(
                "无 applied 时游标仍在快照界内（未靠跳过预支到 live）",
                cursor <= watermark,
                f"游标 {cursor} / watermark {watermark}",
            )

        if status == "caught_up":
            _check(
                "追上时重叠区 seq 全部至少跳过一次",
                overlap_seqs == expected_overlap,
                f"预期 {sorted(expected_overlap)} / 实际 {sorted(overlap_seqs)}",
            )
            _check(
                "追上时 applied seq 集合 = watermark 之后的 live seq",
                applied_seqs == expected_apply,
                f"预期 {sorted(expected_apply)} / 实际 {sorted(applied_seqs)}",
            )
            _check(
                "追上时游标已到 live 末端",
                cursor == live_max_seq,
                f"游标 {cursor} / live_max {live_max_seq}",
            )
            _check(
                "追上时 live 候选 = live 夹具 distinct seq 总数",
                int(record["live_candidate_count"]) == len(all_live_seqs),
                f"记录 {record['live_candidate_count']} / 夹具 {len(all_live_seqs)}",
            )
            _check(
                "追上时 Sink 实际态 = 全量 CDC 折叠预期态",
                actual_sink == expected_final,
                f"实际键 {sorted(actual_keys)} / 预期键 {sorted(expected_final)}",
            )
        else:
            _check(
                "未追上时游标不得到达 live 末端",
                cursor < live_max_seq,
                f"游标 {cursor} / live_max {live_max_seq}",
            )
            _check(
                "未追上时仍有 live seq 未消化",
                consumed_seqs < all_live_seqs,
                f"已消化 {sorted(consumed_seqs)} / live {sorted(all_live_seqs)}",
            )
            _check(
                "未追上时 live 候选 = 已消化 live seq 数（不虚报）",
                int(record["live_candidate_count"])
                == len(consumed_seqs & all_live_seqs),
                f"记录 {record['live_candidate_count']} / 已消化 "
                f"{len(consumed_seqs & all_live_seqs)}",
            )

    # 核心：Sink 实际态 = 快照态 + 已 applied 折叠。重叠区若重放/误删，
    # 键集或 payload 会立刻偏离，这条在追平中与追上后都必须为真。
    _check(
        "Sink 实际态 = 快照态 + 已 applied live 折叠（重叠区零副作用）",
        actual_sink == expected_now,
        f"实际键 {sorted(actual_keys)} / 预期键 {sorted(expected_now)}",
    )

    if record:
        watermark = int(record["watermark_seq"])
        overlap_only = overlap_only_keys(events, watermark_seq=watermark)
        dirty = (actual_keys & overlap_only) - snapshot_keys
        allow_apply_keys = {str(e["entity_key"]) for e in events if int(e["seq"]) > watermark}
        stray = actual_keys - snapshot_keys - allow_apply_keys
        _check(
            "Sink 无重叠区脏键（快照缺席的键不得被旧事件带出）",
            not dirty,
            f"脏键 {sorted(dirty)}（只被重叠区触碰且快照中缺席）",
        )
        _check(
            "Sink 键集 ⊆ 快照键 ∪ watermark 之后的 live 键",
            not stray,
            f"越界键 {sorted(stray)}",
        )
    else:
        dirty = set()

    _check(
        "游标 = 已消费最大 seq",
        cursor == expected_cursor,
        f"游标 {cursor} / 已消费最大 {expected_cursor} / max_applied {max_applied}",
    )
    _check(
        "游标不超过 live 末端",
        cursor <= live_max_seq,
        f"游标 {cursor} / live_max {live_max_seq}",
    )

    return {
        "stream_id": stream_id,
        "status": status,
        "all_match": all(c["ok"] for c in checks),
        "snapshot_record": {
            "row_count": record["row_count"] if record else 0,
            "watermark_seq": record["watermark_seq"] if record else None,
            "live_candidate_count": record["live_candidate_count"] if record else 0,
            "overlap_skipped_count": record["overlap_skipped_count"] if record else 0,
            "applied_count": record["applied_count"] if record else 0,
            "duplicate_skipped_count": record["duplicate_skipped_count"] if record else 0,
            "cursor_after": record["cursor_after"] if record else 0,
            "caught_up_at": record["caught_up_at"] if record else None,
        },
        "ledger": {
            "cumulative_by_disposition": ledger_counts,
            "distinct_overlap_seqs": sorted(overlap_seqs),
            "distinct_duplicate_seqs": sorted(duplicate_seqs),
            "distinct_applied_seqs": sorted(applied_seqs),
            "distinct_consumed_seqs": sorted(consumed_seqs),
            "snapshot_loaded_seqs": sorted(loaded_seqs),
        },
        "sink": {
            "actual": actual_sink,
            # 追平中的预期 = 快照 + 已 applied；追上后与全量折叠一致
            "expected": expected_now,
            "expected_final": expected_final,
            "actual_count": len(actual_sink),
            "expected_count": len(expected_now),
            "dirty_overlap_keys": sorted(dirty),
        },
        "cursor": {
            "apply_cursor": cursor,
            "max_applied_seq": max_applied,
            "live_max_seq": live_max_seq,
            "watermark_seq": int(record["watermark_seq"]) if record else None,
        },
        "checks": checks,
        "rules": PUBLISHED_RULES,
    }

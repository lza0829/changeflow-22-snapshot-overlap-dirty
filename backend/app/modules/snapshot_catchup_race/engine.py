"""snapshot_catchup_race：快照导入与 live 追平竞态的纯函数引擎。

主线只处理「同一流上，一版快照 + 一路 live CDC」的竞态：
- 快照把 watermark_seq（含）之前的变更折叠成实体当前态；
- live 追平时，落在重叠区（seq <= watermark_seq）的事件按公布规则跳过/幂等吞掉；
- 只有 watermark 之后的变更才真正 apply。

不做多源汇入裁决，也不做乱序缓冲超时。
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

# 重叠区事件的账本处置标记（对外公布的规则名，界面直接展示）
OVERLAP_DISPOSITION = "skipped_snapshot_overlap"
# 快照装载行的账本处置标记
SNAPSHOT_LOADED_DISPOSITION = "snapshot_loaded"


def parse_snapshot_fixture(path: Path) -> dict[str, Any]:
    """解析快照夹具：第一行是快照清单（manifest），其余每行一个实体快照态。"""
    if not path.exists():
        raise FileNotFoundError(f"snapshot fixture missing: {path}")
    lines = [ln.strip() for ln in path.read_text(encoding="utf-8").splitlines() if ln.strip()]
    if not lines:
        raise ValueError("empty snapshot fixture")
    try:
        manifest = json.loads(lines[0])
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid snapshot manifest: {exc}") from exc
    for key in ("snapshot_id", "snapshot_ts", "watermark_seq", "entity_type"):
        if key not in manifest:
            raise ValueError(f"snapshot manifest missing {key}")

    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for line_no, raw in enumerate(lines[1:], start=2):
        try:
            row = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid jsonl at snapshot line {line_no}: {exc}") from exc
        for key in ("entity_key", "last_seq", "last_event_ts"):
            if key not in row:
                raise ValueError(f"snapshot row at line {line_no} missing {key}")
        entity_key = str(row["entity_key"])
        if entity_key in seen:
            raise ValueError(f"duplicate entity_key in snapshot: {entity_key}")
        seen.add(entity_key)
        rows.append(
            {
                "entity_key": entity_key,
                "last_seq": int(row["last_seq"]),
                "last_event_ts": str(row["last_event_ts"]),
                "payload": row.get("payload") or {},
            }
        )

    watermark = int(manifest["watermark_seq"])
    if rows and any(r["last_seq"] > watermark for r in rows):
        raise ValueError("snapshot row last_seq cannot exceed watermark_seq")
    return {
        "snapshot_id": str(manifest["snapshot_id"]),
        "snapshot_ts": str(manifest["snapshot_ts"]),
        "watermark_seq": watermark,
        "entity_type": str(manifest["entity_type"]),
        "note": str(manifest.get("note") or ""),
        "rows": rows,
    }


def classify_catchup_event(
    *,
    seq: int,
    already_applied: bool,
    watermark_seq: int,
) -> str:
    """追平态事件裁决。

    公布的重叠消化规则（按优先级）：
      1. ``seq <= watermark_seq``：变更已折叠进快照，一律跳过
         （``skipped_snapshot_overlap``）。哪怕 sink 里没有该键也不重放——
         快照的缺席本身就是结论（例如该键在快照前已被删除），重放旧事件会
         造出脏行/让已删除实体复活；
      2. watermark 之后、但本流已经应用过该 seq（重跑）：幂等跳过
         （``skipped_duplicate``）；
      3. 其余是 live 新变更：``apply``，按 upsert/delete 幂等落到同一实体键，
         sink 的 UNIQUE(stream_id, entity_key) 保证不会悄悄多出脏行。
    """
    if int(seq) <= int(watermark_seq):
        return OVERLAP_DISPOSITION
    if already_applied:
        return "skipped_duplicate"
    return "apply"


def is_caught_up(*, cursor: int, live_max_seq: int) -> bool:
    """游标真正推进到 live 夹具的最大 seq，才算追上。

    只要求游标 >= live 末端：分批追平（或只扫到重叠区）时游标仍在快照界内，
    不得被当成已追平；调用方还必须保证 live 事件已全部消化，见 catchup_status。
    """
    return int(cursor) >= int(live_max_seq)


def catchup_status(
    *,
    cursor: int,
    live_max_seq: int,
    processed_seqs: set[int],
    all_live_seqs: set[int],
) -> str:
    """追平状态机唯一裁决点：``caught_up`` 只能由「已处理全部 live seq + 游标到末端」共同推出。

    任一条件不满足都停留在 ``catching_up``，绝不提前标记完成。
    """
    if is_caught_up(cursor=cursor, live_max_seq=live_max_seq) and set(processed_seqs) >= set(
        all_live_seqs
    ):
        return "caught_up"
    return "catching_up"


def fold_expected_sink(events: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """把一路 CDC 从头折叠出预期最终态，供对账时与真实 sink 比对。"""
    expected: dict[str, dict[str, Any]] = {}
    for event in sorted(events, key=lambda e: int(e["seq"])):
        key = str(event["entity_key"])
        if event["op"] == "delete":
            expected.pop(key, None)
        else:
            expected[key] = event.get("payload") or {}
    return expected


def fold_catchup_sink(
    snapshot_state: dict[str, dict[str, Any]],
    events: list[dict[str, Any]],
    *,
    applied_seqs: set[int],
) -> dict[str, dict[str, Any]]:
    """追平进行中的预期 Sink 态：快照实体态 + 仅折叠已真正 applied 的 live 事件。

    重叠区事件已被快照折叠，跳过时不得碰 Sink；因此无论追到哪一批，
    Sink 都应与「快照 + 已 applied seq」逐键一致。
    """
    state: dict[str, dict[str, Any]] = {
        str(key): payload for key, payload in snapshot_state.items()
    }
    applied = {int(s) for s in applied_seqs}
    for event in sorted(events, key=lambda e: int(e["seq"])):
        if int(event["seq"]) not in applied:
            continue
        key = str(event["entity_key"])
        if event["op"] == "delete":
            state.pop(key, None)
        else:
            state[key] = event.get("payload") or {}
    return state


def overlap_only_keys(events: list[dict[str, Any]], *, watermark_seq: int) -> set[str]:
    """只被重叠区事件触碰、且快照中没有的实体键。

    这些键要么在快照前已删除，要么根本没存在过：重叠区跳过时绝不允许它们
    出现在 Sink 里，否则就是同键多出来的脏行（典型如被 delete 后又被旧
    upsert 复活的键）。watermark 之后还有事件的键不在此列——它由 live
    apply 合法重建。
    """
    touched_overlap: set[str] = set()
    touched_live: set[str] = set()
    for event in events:
        key = str(event["entity_key"])
        if int(event["seq"]) <= int(watermark_seq):
            touched_overlap.add(key)
        else:
            touched_live.add(key)
    return touched_overlap - touched_live

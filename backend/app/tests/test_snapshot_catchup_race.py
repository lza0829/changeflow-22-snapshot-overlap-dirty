import os
import shutil
import tempfile

import pytest

# 服务级测试需要独立 DATA_DIR，必须在导入 app.db 之前设置
_TMP_DATA = tempfile.mkdtemp(prefix="changeflow_snapshot_test_")
os.environ.setdefault("DATA_DIR", _TMP_DATA)

from app.config import FIXTURES_DIR  # noqa: E402
from app.db import connect  # noqa: E402
from app.modules.snapshot_catchup_race.engine import (  # noqa: E402
    OVERLAP_DISPOSITION,
    catchup_status,
    classify_catchup_event,
    fold_catchup_sink,
    fold_expected_sink,
    is_caught_up,
    overlap_only_keys,
    parse_snapshot_fixture,
)
from app.repositories import streams as streams_repo  # noqa: E402
from app.seed import init_db  # noqa: E402
from app.services import snapshot_service as ss  # noqa: E402


def test_parse_snapshot_fixture():
    snap = parse_snapshot_fixture(FIXTURES_DIR / "orders_snapshot.jsonl")
    assert snap["snapshot_id"] == "ORD-SNAP-001"
    assert snap["watermark_seq"] == 5
    assert [r["entity_key"] for r in snap["rows"]] == ["ORD-1001", "ORD-1003"]
    assert all(r["last_seq"] <= snap["watermark_seq"] for r in snap["rows"])


def test_classify_rules():
    # 重叠区：无论是否应用过都跳过
    assert (
        classify_catchup_event(seq=5, already_applied=False, watermark_seq=5)
        == OVERLAP_DISPOSITION
    )
    assert (
        classify_catchup_event(seq=1, already_applied=True, watermark_seq=5)
        == OVERLAP_DISPOSITION
    )
    # watermark 之后：已应用幂等吞掉，未应用才 apply
    assert (
        classify_catchup_event(seq=6, already_applied=True, watermark_seq=5)
        == "skipped_duplicate"
    )
    assert (
        classify_catchup_event(seq=6, already_applied=False, watermark_seq=5) == "apply"
    )
    assert classify_catchup_event(seq=6, already_applied=False, watermark_seq=5) == "apply"


def test_fold_and_caught_up():
    events = [
        {"seq": 1, "op": "upsert", "entity_key": "A", "payload": {"v": 1}},
        {"seq": 2, "op": "delete", "entity_key": "A", "payload": {}},
        {"seq": 3, "op": "upsert", "entity_key": "B", "payload": {"v": 2}},
    ]
    assert fold_expected_sink(events) == {"B": {"v": 2}}
    assert is_caught_up(cursor=3, live_max_seq=3) is True
    assert is_caught_up(cursor=2, live_max_seq=3) is False
    # 空路/未开始也不得被判成追上（旧实现恒真）
    assert is_caught_up(cursor=0, live_max_seq=10) is False


def test_catchup_status_requires_both_conditions():
    all_live = {1, 2, 3}
    assert (
        catchup_status(
            cursor=3, live_max_seq=3, processed_seqs={1, 2, 3}, all_live_seqs=all_live
        )
        == "caught_up"
    )
    # 游标到末端但还有 live seq 没消化：不许标完成
    assert (
        catchup_status(
            cursor=3, live_max_seq=3, processed_seqs={1, 2}, all_live_seqs=all_live
        )
        == "catching_up"
    )
    # 全消化但游标没到末端（理论上不应出现）：也不许标完成
    assert (
        catchup_status(
            cursor=2, live_max_seq=3, processed_seqs={1, 2, 3}, all_live_seqs=all_live
        )
        == "catching_up"
    )


def test_overlap_only_keys_excludes_live_touched_keys():
    events = [
        {"seq": 1, "op": "upsert", "entity_key": "GONE_BEFORE_SNAPSHOT"},
        {"seq": 3, "op": "upsert", "entity_key": "IN_SNAPSHOT"},
        {"seq": 7, "op": "upsert", "entity_key": "IN_SNAPSHOT"},
        {"seq": 9, "op": "upsert", "entity_key": "LIVE_NEW"},
    ]
    assert overlap_only_keys(events, watermark_seq=5) == {"GONE_BEFORE_SNAPSHOT"}


def test_fold_catchup_sink_ignores_overlap_events():
    snapshot_state = {"KEEP": {"v": "snapshot"}, "TEMPSHOT": {"v": 1}}
    events = [
        # 重叠区的旧 upsert / 旧 delete 都不能影响任何键
        {"seq": 2, "op": "upsert", "entity_key": "DELETED_BEFORE", "payload": {"v": 1}},
        {"seq": 4, "op": "delete", "entity_key": "TEMPSHOT", "payload": {}},
        {"seq": 8, "op": "upsert", "entity_key": "KEEP", "payload": {"v": "live"}},
    ]
    assert fold_catchup_sink(snapshot_state, events, applied_seqs=set()) == snapshot_state
    assert fold_catchup_sink(snapshot_state, events, applied_seqs={8}) == {
        "KEEP": {"v": "live"},
        "TEMPSHOT": {"v": 1},
    }


def _lane_stream_id():
    conn = connect()
    try:
        rows = streams_repo.list_streams(conn)
        return next(s["id"] for s in rows if "快照" in s["name"])
    finally:
        conn.close()


def test_import_then_batched_catchup_reconciles():
    shutil.rmtree(_TMP_DATA, ignore_errors=True)
    os.makedirs(_TMP_DATA, exist_ok=True)
    init_db()
    sid = _lane_stream_id()

    record = ss.import_snapshot(sid, "orders_snapshot.jsonl")
    assert record["status"] == "imported"
    assert record["row_count"] == 2
    assert record["watermark_seq"] == 5

    # 导入后：Sink 只有快照 2 行，游标不预支，对账已全绿
    conn = connect()
    try:
        after_import = ss.reconcile(conn, sid)
    finally:
        conn.close()
    assert set(after_import["sink"]["actual"]) == {"ORD-1001", "ORD-1003"}
    assert after_import["cursor"]["apply_cursor"] == 0
    assert after_import["all_match"] is True

    # 第一批只处理前 3 条（全在重叠区）：必须显式处于 catching_up。
    # 重叠区跳过是纯读侧裁决：不得往 Sink 塞 ORD-1002（快照中缺席=已删除），
    # 也不得靠「跳过」把游标预支到 live 末端。
    batch1 = ss.catch_up(sid, limit=3)
    assert batch1["import"]["status"] == "catching_up"
    assert batch1["import"]["caught_up_at"] is None
    assert batch1["import"]["overlap_skipped_count"] == 3
    assert batch1["import"]["applied_count"] == 0
    # 游标只消费到重叠区 seq3（快照界内），绝不预支到 live 末端
    assert batch1["import"]["cursor_after"] == 3
    assert batch1["reconciliation"]["cursor"]["apply_cursor"] < 10
    assert set(batch1["reconciliation"]["sink"]["actual"]) == {"ORD-1001", "ORD-1003"}
    assert batch1["reconciliation"]["sink"]["dirty_overlap_keys"] == []
    assert batch1["reconciliation"]["all_match"] is True

    # 第二批停在刚跨过快照界的位置：仍是 catching_up，Sink 无脏键
    batch2 = ss.catch_up(sid, limit=7)
    mid = batch2["reconciliation"]
    assert batch2["import"]["status"] == "catching_up"
    assert batch2["import"]["caught_up_at"] is None
    assert mid["all_match"] is True
    assert mid["sink"]["dirty_overlap_keys"] == []
    assert set(mid["sink"]["actual"]) == {"ORD-1001", "ORD-1003", "ORD-1004"}
    assert mid["sink"]["actual"]["ORD-1001"]["status"] == "shipped"
    assert mid["cursor"]["apply_cursor"] == 7
    assert mid["cursor"]["apply_cursor"] > mid["cursor"]["watermark_seq"]

    # 追完：重扫不重复计数，四方对账全绿
    done = ss.catch_up(sid)
    imp = done["import"]
    assert imp["status"] == "caught_up"
    assert imp["caught_up_at"] is not None
    assert imp["live_candidate_count"] == 10
    assert imp["overlap_skipped_count"] == 5
    assert imp["applied_count"] == 5
    assert imp["cursor_after"] == 10
    assert done["reconciliation"]["all_match"] is True

    sink_keys = set(done["reconciliation"]["sink"]["actual"])
    assert sink_keys == {"ORD-1001", "ORD-1004", "ORD-1005"}
    assert done["reconciliation"]["sink"]["actual"]["ORD-1001"]["status"] == "shipped"
    assert done["reconciliation"]["sink"]["dirty_overlap_keys"] == []
    # 已删除的 ORD-1002 绝不因重叠区旧 upsert 复活
    assert "ORD-1002" not in sink_keys

    # 关掉浏览器再开 = 重新连接，状态仍在
    conn = connect()
    try:
        again = streams_repo.get_stream(conn, sid)
        assert again["apply_cursor"] == 10
        assert again["snapshot_import_id"] == imp["id"]
    finally:
        conn.close()


def test_catching_up_never_marks_done_or_fake_cursor():
    """未追上时状态机、游标与快照界的互证（逐批核对，不靠全量重扫一次蒙对）。"""
    sid = _lane_stream_id()
    ss.reset_lane(sid)
    ss.import_snapshot(sid, "orders_snapshot.jsonl")

    # 每批上限逐步放到 1..10，直到全部追完；前 9 批都不许提前标完成
    expected_status = ["catching_up"] * 9 + ["caught_up"]
    for step, want in enumerate(expected_status, start=1):
        out = ss.catch_up(sid, limit=step)
        imp = out["import"]
        recon = out["reconciliation"]
        if want == "catching_up":
            assert imp["status"] == "catching_up", f"step {step} 提前标完成"
            assert imp["caught_up_at"] is None
            assert recon["cursor"]["apply_cursor"] <= 10
            if imp["applied_count"] == 0:
                # 尚未 apply 任何 live 变更：游标必须还在快照界内
                assert recon["cursor"]["apply_cursor"] <= recon["cursor"]["watermark_seq"]
        else:
            assert imp["status"] == "caught_up"
            assert imp["caught_up_at"] is not None
            assert recon["cursor"]["apply_cursor"] == 10
        assert recon["all_match"] is True, f"step {step} 对账未全绿: {recon['checks']}"
        assert recon["sink"]["dirty_overlap_keys"] == []
        # 追平中的预期 Sink 也必须是「快照键 ∪ watermark 之后的 live 键」
        assert set(recon["sink"]["actual"]) <= {"ORD-1001", "ORD-1003", "ORD-1004", "ORD-1005"}
        assert "ORD-1002" not in recon["sink"]["actual"]


def test_import_refuses_lane_with_existing_apply_traces():
    """常规 Apply 跑过的流不能导快照，否则快照态与已应用态打架。"""
    sid = _lane_stream_id()
    ss.reset_lane(sid)
    from app.services import apply_service  # 局部导入：避免测试模块级副作用

    apply_service.run_apply(sid)
    with pytest.raises(ValueError):
        ss.import_snapshot(sid, "orders_snapshot.jsonl")
    ss.reset_lane(sid)
    assert ss.import_snapshot(sid, "orders_snapshot.jsonl")["status"] == "imported"


def test_reset_allows_reimport_and_one_shot_catchup():
    sid = _lane_stream_id()
    ss.reset_lane(sid)
    ss.import_snapshot(sid, "orders_snapshot.jsonl")
    done = ss.catch_up(sid)
    assert done["import"]["status"] == "caught_up"
    assert done["reconciliation"]["all_match"] is True

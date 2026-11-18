"""核验逻辑湖的分支修订、关联验证、安全提升与固定快照查询。"""
from pathlib import Path
import json
import multiprocessing
import tempfile
import unittest

from lakekit.lake import Lake, LakeError, delete, insert, update


def seed(root):
    Lake.initialize(
        root,
        [{"observation_id": "O1", "map_key": "T1:v1", "score": "0.5"}],
        [{"map_key": "T1:v1", "canonical_target": "G1"}],
    )
    return Lake(root)


def _promote_worker(root, validation_id, barrier, queue):
    lake = Lake(root)
    barrier.wait()
    try:
        attempt = lake.promote(validation_id)
        queue.put((validation_id, attempt["promoted"], attempt["reason"]))
    except Exception as exc:  # pragma: no cover - 仅用于并发结果回传
        queue.put((validation_id, False, f"{type(exc).__name__}:{exc}"))


class BranchLifecycle(unittest.TestCase):
    def test_branch_is_pinned_to_explicit_main_snapshot(self):
        with tempfile.TemporaryDirectory() as directory:
            lake = seed(directory)
            first = lake.snapshot_manifest()["snapshot"]
            lake.revise("main", [insert("observations", {"observation_id": "O2", "map_key": "T1:v1", "score": "0.7"})])
            branch = lake.create_branch("review", first)
            self.assertEqual(branch["base_snapshot"], first)
            self.assertEqual(branch["base_main_version"], 1)
            with self.assertRaises(LakeError):
                lake.create_branch("other", "f" * 64)
            with self.assertRaises(LakeError):
                lake.create_branch("main")
            self.assertEqual(lake.show("review")["snapshot"], first)
            self.assertEqual(len(lake.show("review")["tables"]["observations"]), 1)

    def test_revise_stage_then_keep_concurrent_main_writes(self):
        with tempfile.TemporaryDirectory() as directory:
            lake = seed(directory)
            lake.create_branch("review")
            lake.revise(
                "review",
                [
                    update("observations", "O1", {"score": "0.9"}),
                    insert("observations", {"observation_id": "O3", "map_key": "T1:v1", "score": "0.2"}),
                ],
            )
            rows = lake.show("review")["tables"]["observations"]
            self.assertEqual(rows[0]["score"], "0.9")
            self.assertEqual({row["observation_id"] for row in rows}, {"O1", "O3"})
            lake.revise("main", [insert("observations", {"observation_id": "O2", "map_key": "T1:v1", "score": "0.7"})])
            record = lake.validate("review")
            self.assertTrue(record["passed"], record["conflicts"])
            attempt = lake.promote(record["validation_id"])
            self.assertTrue(attempt["promoted"])
            rows = {row["observation_id"] for row in lake.show("main")["tables"]["observations"]}
            self.assertEqual(rows, {"O1", "O2", "O3"})
            # 提升保留主线自分叉后的 O2，并且记录的是原始行。
            main_changes = {item["key"] for item in attempt["main_changes_since_fork"]}
            self.assertIn("O2", main_changes)

    def test_delete_versus_modify_is_conflict_and_main_unchanged(self):
        with tempfile.TemporaryDirectory() as directory:
            lake = seed(directory)
            main_before = lake.snapshot_manifest()["snapshot"]
            lake.create_branch("review")
            lake.revise("review", [delete("observations", "O1")])
            lake.revise("main", [update("observations", "O1", {"score": "0.8"})])
            record = lake.validate("review")
            self.assertFalse(record["passed"])
            kinds = {(item["table"], item["key"], item["kind"]) for item in record["conflicts"]}
            self.assertIn(("observations", "O1", "delete_vs_modify"), kinds)
            attempt = lake.promote(record["validation_id"])
            self.assertFalse(attempt["promoted"])
            self.assertEqual(attempt["reason"], "validation_failed")
            self.assertEqual(lake.show("main")["tables"]["observations"][0]["score"], "0.8")
            self.assertNotEqual(lake.snapshot_manifest()["snapshot"], main_before)

    def test_same_final_content_can_merge_but_different_maps_conflict(self):
        with tempfile.TemporaryDirectory() as directory:
            lake = seed(directory)
            lake.create_branch("review")
            same = {"observation_id": "O2", "map_key": "T1:v1", "score": "0.4"}
            lake.revise("review", [insert("observations", same)])
            lake.revise("main", [insert("observations", same)])
            record = lake.validate("review")
            self.assertTrue(record["passed"], record["conflicts"])
            self.assertEqual(record["counts"]["merged"]["observations"], 2)

            lake.create_branch("mapreview")
            lake.revise("mapreview", [update("target_map", "T1:v1", {"canonical_target": "G2"})])
            lake.revise("main", [update("target_map", "T1:v1", {"canonical_target": "G3"})])
            record = lake.validate("mapreview")
            kinds = {(item["table"], item["key"], item["kind"]) for item in record["conflicts"]}
            self.assertIn(("target_map", "T1:v1", "both_modified"), kinds)
            self.assertFalse(record["passed"])

    def test_mapping_key_is_source_target_and_version_not_display_name(self):
        with tempfile.TemporaryDirectory() as directory:
            lake = seed(directory)
            lake.revise(
                "main",
                [
                    insert("target_map", {"map_key": "T1:v2", "canonical_target": "G1"}),
                    insert("observations", {"observation_id": "O2", "map_key": "T1:v2", "score": "0.3"}),
                ],
            )
            rows = lake.show("main")["tables"]["target_map"]
            self.assertEqual([row["map_key"] for row in rows], ["T1:v1", "T1:v2"])
            for bad in ["T1", "T1:v1:x", ":v1", "T1:"]:
                with self.assertRaises(ValueError):
                    lake.revise("main", [insert("target_map", {"map_key": bad, "canonical_target": "G"})])
            lake.create_branch("review")
            lake.revise("review", [delete("target_map", "T1:v2")])
            record = lake.validate("review")
            # T1:v1 与 T1:v2 的显示名都是 G1，但 O2 绑定的是被删除的版本。
            self.assertFalse(record["passed"])
            dangling = [item for item in record["conflicts"] if item["kind"] == "dangling_reference"]
            self.assertEqual(dangling[0]["observation_ids"], ["O2"])

    def test_main_new_observation_makes_deleted_reference_unsafe(self):
        with tempfile.TemporaryDirectory() as directory:
            lake = seed(directory)
            lake.revise("main", [insert("target_map", {"map_key": "T2:v1", "canonical_target": "G2"})])
            lake.create_branch("review")
            lake.revise("review", [delete("target_map", "T2:v1")])
            lake.revise(
                "main",
                [insert("observations", {"observation_id": "O9", "map_key": "T2:v1", "score": "0.1"})],
            )
            record = lake.validate("review")
            self.assertFalse(record["passed"])
            self.assertFalse(record["checks"]["references_resolved"])
            dangling = [item for item in record["conflicts"] if item["kind"] == "dangling_reference"]
            self.assertEqual(dangling[0]["observation_ids"], ["O9"])
            attempt = lake.promote(record["validation_id"])
            self.assertFalse(attempt["promoted"])
            self.assertEqual(lake.snapshot_manifest()["counts"]["observations"], 2)

    def test_mapping_content_change_rechecks_all_merged_references(self):
        with tempfile.TemporaryDirectory() as directory:
            lake = seed(directory)
            lake.revise("main", [insert("target_map", {"map_key": "T2:v1", "canonical_target": "G2"})])
            lake.create_branch("review")
            lake.revise(
                "review",
                [
                    update("target_map", "T1:v1", {"canonical_target": "GX"}),
                    delete("target_map", "T2:v1"),
                ],
            )
            lake.revise(
                "main",
                [insert("observations", {"observation_id": "O9", "map_key": "T2:v1", "score": "0.1"})],
            )
            # 分支改了 T1:v1 的内容并删了 T2:v1；主线的写入在另一个主键上。
            # 合并后仍要整体复核两张表的引用，不能只看逐主键冲突。
            record = lake.validate("review")
            self.assertFalse(record["passed"])
            self.assertFalse(record["checks"]["references_resolved"])
            self.assertTrue(any(item["kind"] == "dangling_reference" for item in record["conflicts"]))
            self.assertEqual(record["merged_snapshot"], None)

    def test_revision_is_atomic_and_rejects_bad_operations(self):
        with tempfile.TemporaryDirectory() as directory:
            lake = seed(directory)
            lake.create_branch("review")
            head = lake.show("review")["snapshot"]
            with self.assertRaises(LakeError):
                lake.revise(
                    "review",
                    [
                        insert("target_map", {"map_key": "T2:v1", "canonical_target": "G2"}),
                        insert("observations", {"observation_id": "O2", "map_key": "T9:v1", "score": "0.1"}),
                        delete("observations", "missing"),
                    ],
                )
            self.assertEqual(lake.show("review")["snapshot"], head)
            self.assertEqual(lake.show("review")["tables"]["target_map"][0]["map_key"], "T1:v1")


class ValidationAndPromotion(unittest.TestCase):
    def test_validation_is_bound_to_branch_and_main_snapshots(self):
        with tempfile.TemporaryDirectory() as directory:
            lake = seed(directory)
            lake.create_branch("review")
            lake.revise(
                "review",
                [
                    insert("target_map", {"map_key": "T2:v1", "canonical_target": "G2"}),
                    insert("observations", {"observation_id": "O2", "map_key": "T2:v1", "score": "0.2"}),
                ],
            )
            record = lake.validate("review")
            self.assertTrue(record["passed"])
            self.assertEqual(record["main_version"], 1)
            lake.revise("main", [insert("observations", {"observation_id": "O3", "map_key": "T1:v1", "score": "0.3"})])
            attempt = lake.promote(record["validation_id"])
            self.assertFalse(attempt["promoted"])
            self.assertEqual(attempt["reason"], "stale_validation")
            self.assertTrue(any(item["validation_id"] == record["validation_id"] for item in lake.history("review")["validations"]))
            self.assertEqual(lake.history("main")["history"][-1]["version"], 2)
            self.assertTrue(lake.validate("review")["passed"])

    def test_branch_change_after_validation_blocks_promotion(self):
        with tempfile.TemporaryDirectory() as directory:
            lake = seed(directory)
            lake.create_branch("review")
            lake.revise("review", [update("observations", "O1", {"score": "0.6"})])
            record = lake.validate("review")
            lake.revise("review", [update("observations", "O1", {"score": "0.7"})])
            attempt = lake.promote(record["validation_id"])
            self.assertFalse(attempt["promoted"])
            self.assertEqual(attempt["reason"], "branch_changed")
            self.assertEqual(lake.show("main")["tables"]["observations"][0]["score"], "0.5")

    def test_revalidation_reruns_checks_on_new_merged_content(self):
        with tempfile.TemporaryDirectory() as directory:
            lake = seed(directory)
            lake.create_branch("review")
            lake.revise(
                "review",
                [
                    insert("target_map", {"map_key": "T2:v1", "canonical_target": "G2"}),
                    insert("observations", {"observation_id": "O2", "map_key": "T2:v1", "score": "0.2"}),
                ],
            )
            first = lake.validate("review")
            lake.revise("main", [insert("observations", {"observation_id": "O3", "map_key": "T1:v1", "score": "0.3"})])
            second = lake.revalidate(first["validation_id"])
            self.assertEqual(second["supersedes"], first["validation_id"])
            self.assertNotEqual(second["merged_snapshot"], first["merged_snapshot"])
            self.assertFalse(second["merged_content_unchanged"])
            self.assertEqual(second["main_version"], 2)
            self.assertTrue(second["passed"])
            self.assertTrue(lake.promote(second["validation_id"])["promoted"])
            rows = {row["observation_id"] for row in lake.show("main")["tables"]["observations"]}
            self.assertEqual(rows, {"O1", "O2", "O3"})

    def test_revalidation_can_fail_on_new_reference_problem(self):
        with tempfile.TemporaryDirectory() as directory:
            lake = seed(directory)
            lake.revise("main", [insert("target_map", {"map_key": "T2:v1", "canonical_target": "G2"})])
            lake.create_branch("review")
            lake.revise("review", [delete("target_map", "T2:v1")])
            first = lake.validate("review")
            self.assertTrue(first["passed"])
            # 主线新增的观察绑定到分支已经删除的映射；旧记录通过时并没有它。
            lake.revise(
                "main",
                [insert("observations", {"observation_id": "O9", "map_key": "T2:v1", "score": "0.1"})],
            )
            second = lake.revalidate(first["validation_id"])
            self.assertFalse(second["passed"])
            self.assertEqual(second["main_version"], 3)
            self.assertFalse(second["checks"]["references_resolved"])
            # 旧记录仍在目录中，但主线已经变化，不再授权新的合并结果。
            stale = lake.promote(first["validation_id"])
            self.assertFalse(stale["promoted"])
            self.assertEqual(stale["reason"], "stale_validation")

    def test_failed_promotion_keeps_branch_and_reports_details(self):
        with tempfile.TemporaryDirectory() as directory:
            lake = seed(directory)
            lake.revise("main", [insert("target_map", {"map_key": "T2:v1", "canonical_target": "G2"})])
            lake.create_branch("review")
            lake.revise(
                "review",
                [
                    update("target_map", "T1:v1", {"canonical_target": "G9"}),
                    delete("target_map", "T2:v1"),
                ],
            )
            lake.revise(
                "main",
                [insert("observations", {"observation_id": "O9", "map_key": "T2:v1", "score": "0.1"})],
            )
            record = lake.validate("review")
            self.assertFalse(record["passed"])
            attempt = lake.promote(record["validation_id"])
            self.assertFalse(attempt["promoted"])
            self.assertEqual(attempt["reason"], "validation_failed")
            self.assertTrue(attempt["conflicts"])
            self.assertEqual(len(lake.history("review")["revisions"]), 2)
            self.assertEqual(lake.show("review")["tables"]["target_map"][0]["canonical_target"], "G9")
            # 保留分支与冲突详情后，研究人员修订分支，再验证并提升。
            lake.revise("review", [insert("target_map", {"map_key": "T2:v1", "canonical_target": "G2"})])
            record = lake.validate("review")
            self.assertTrue(record["passed"], record["conflicts"])
            self.assertTrue(lake.promote(record["validation_id"])["promoted"])

    def test_used_validation_cannot_promote_twice(self):
        with tempfile.TemporaryDirectory() as directory:
            lake = seed(directory)
            lake.create_branch("review")
            lake.revise("review", [update("observations", "O1", {"score": "0.6"})])
            record = lake.validate("review")
            self.assertTrue(lake.promote(record["validation_id"])["promoted"])
            with self.assertRaises(LakeError):
                lake.promote(record["validation_id"])

    def test_promotion_without_branch_changes_is_not_upload_only(self):
        with tempfile.TemporaryDirectory() as directory:
            lake = seed(directory)
            lake.create_branch("review")
            record = lake.validate("review")
            self.assertTrue(record["passed"])
            attempt = lake.promote(record["validation_id"])
            self.assertFalse(attempt["promoted"])
            self.assertEqual(attempt["reason"], "no_branch_changes")
            self.assertEqual(lake.snapshot_manifest()["snapshot"], record["main_snapshot"])

    def test_success_must_be_confirmed_after_catalog_replace(self):
        with tempfile.TemporaryDirectory() as directory:
            lake = seed(directory)
            lake.create_branch("review")
            lake.revise("review", [update("observations", "O1", {"score": "0.6"})])
            record = lake.validate("review")
            original = lake._write_catalog
            lake._write_catalog = lambda catalog: None
            try:
                with self.assertRaises(LakeError):
                    lake.promote(record["validation_id"])
            finally:
                lake._write_catalog = original
            self.assertEqual(lake.show("main")["tables"]["observations"][0]["score"], "0.5")

    def test_competing_promotions_produce_one_deterministic_winner(self):
        with tempfile.TemporaryDirectory() as directory:
            lake = seed(directory)
            lake.create_branch("alpha")
            lake.revise(
                "alpha",
                [
                    insert("target_map", {"map_key": "A:v1", "canonical_target": "GA"}),
                    insert("observations", {"observation_id": "OA", "map_key": "A:v1", "score": "0.1"}),
                ],
            )
            lake.create_branch("beta")
            lake.revise(
                "beta",
                [
                    insert("target_map", {"map_key": "B:v1", "canonical_target": "GB"}),
                    insert("observations", {"observation_id": "OB", "map_key": "B:v1", "score": "0.2"}),
                ],
            )
            alpha = lake.validate("alpha")
            beta = lake.validate("beta")
            self.assertTrue(alpha["passed"] and beta["passed"])
            context = multiprocessing.get_context("fork")
            barrier = context.Barrier(2)
            queue = context.Queue()
            workers = [
                context.Process(target=_promote_worker, args=(directory, alpha["validation_id"], barrier, queue)),
                context.Process(target=_promote_worker, args=(directory, beta["validation_id"], barrier, queue)),
            ]
            for worker in workers:
                worker.start()
            results = [queue.get(timeout=60) for _ in workers]
            for worker in workers:
                worker.join(timeout=60)
            self.assertEqual(sum(1 for _, promoted, _ in results if promoted), 1, results)
            loser = [item for item in results if not item[1]][0]
            self.assertEqual(loser[2], "stale_validation")
            # 失败者没有被上传一半的内容；重新检查后仍可修订与提升。
            main_rows = {row["observation_id"] for row in lake.show("main")["tables"]["observations"]}
            self.assertIn("O1", main_rows)
            self.assertEqual(len(main_rows), 2)
            loser_record = lake.revalidate(loser[0])
            self.assertTrue(loser_record["passed"])
            self.assertTrue(lake.promote(loser_record["validation_id"])["promoted"])
            self.assertEqual(len(lake.show("main")["tables"]["observations"]), 3)


class QueriesAndSnapshots(unittest.TestCase):
    def test_duckdb_queries_pinned_snapshot_of_either_lineage(self):
        with tempfile.TemporaryDirectory() as directory:
            lake = seed(directory)
            base = lake.snapshot_manifest()["snapshot"]
            lake.revise("main", [insert("observations", {"observation_id": "O2", "map_key": "T1:v1", "score": "0.7"})])
            lake.create_branch("review", base)
            lake.revise("review", [insert("observations", {"observation_id": "O3", "map_key": "T1:v1", "score": "0.2"})])
            pinned_branch = lake.snapshot_manifest("review")["snapshot"]
            lake.revise("review", [insert("observations", {"observation_id": "O4", "map_key": "T1:v1", "score": "0.4"})])
            main = lake.query("SELECT observation_id FROM observations ORDER BY observation_id")
            self.assertEqual([row["observation_id"] for row in main["rows"]], ["O1", "O2"])
            old = lake.query("SELECT count(*) AS n FROM observations", "review", pinned_branch)
            self.assertEqual(old["rows"][0]["n"], 2)
            head = lake.query("SELECT count(*) AS n FROM observations", "review")
            self.assertEqual(head["rows"][0]["n"], 3)
            joined = lake.query(
                "SELECT o.observation_id, m.canonical_target FROM observations o "
                "JOIN target_map m USING (map_key) ORDER BY o.observation_id"
            )
            self.assertEqual([row["canonical_target"] for row in joined["rows"]], ["G1", "G1"])
            with self.assertRaises(LakeError):
                lake.query("SELECT 1", "review", "e" * 64)
            lake.revise("main", [delete("observations", "O2")])
            pinned = lake.show("main", snapshot=main["snapshot"])["tables"]["observations"]
            self.assertEqual([row["observation_id"] for row in pinned], ["O1", "O2"])

    def test_duckdb_reads_real_parquet_and_parameterized_sql(self):
        with tempfile.TemporaryDirectory() as directory:
            lake = seed(directory)
            result = lake.query(
                "SELECT observation_id FROM observations WHERE map_key = ?",
                parameters=["T1:v1"],
            )
            self.assertEqual(result["rows"], [{"observation_id": "O1"}])
            path = lake.objects.path(lake.snapshot_manifest()["tables"]["observations"])
            self.assertTrue(path.read_bytes().startswith(b"PAR1"))

    def test_diff_and_status_show_real_rows_and_conflicts(self):
        with tempfile.TemporaryDirectory() as directory:
            lake = seed(directory)
            lake.create_branch("review")
            lake.revise("review", [update("observations", "O1", {"score": "0.8"})])
            lake.revise("main", [update("observations", "O1", {"score": "0.9"})])
            payload = lake.diff("review")
            change = payload["changes"][0]
            self.assertEqual(change["before"]["score"], "0.5")
            self.assertEqual(change["after"]["score"], "0.8")
            kinds = [item["kind"] for item in payload["conflicts"]]
            self.assertIn("both_modified", kinds)
            status = lake.status("review")
            self.assertEqual(status["main"]["counts"]["observations"], 1)
            self.assertEqual(status["branches"]["review"]["counts"]["observations"], 1)
            self.assertEqual(status["branches"]["review"]["conflicts"][0]["key"], "O1")

    def test_preview_does_not_write(self):
        with tempfile.TemporaryDirectory() as directory:
            lake = seed(directory)
            lake.create_branch("review")
            head = lake.show("review")["snapshot"]
            result = lake.preview(
                "review",
                [insert("observations", {"observation_id": "O2", "map_key": "T1:v1", "score": "0.2"})],
            )
            self.assertEqual(result["changes"][0]["key"], "O2")
            self.assertEqual(lake.show("review")["snapshot"], head)


class DataDrivenFlow(unittest.TestCase):
    def test_seed_and_events_flow(self):
        root = Path(__file__).resolve().parents[1]
        seed_data = json.loads((root / "data/seed.json").read_text(encoding="utf-8"))
        events = json.loads((root / "data/events.json").read_text(encoding="utf-8"))
        with tempfile.TemporaryDirectory() as directory:
            Lake.initialize(directory, seed_data["observations"], seed_data["target_map"])
            lake = Lake(directory)
            lake.create_branch("review")
            lake.apply_events(events)
            status = lake.status("review")
            self.assertEqual(status["main"]["counts"]["observations"], 2)
            conflicts = status["branches"]["review"]["conflicts"]
            # 主线新增 O2 仍引用 review 删除的 T1:v1；O3 引用从未存在的 T2:v1。
            dangling = {item["key"] for item in conflicts if item["kind"] == "dangling_reference"}
            self.assertEqual(dangling, {"T1:v1", "T2:v1"})
            record = lake.validate("review")
            self.assertFalse(record["passed"])
            self.assertFalse(lake.promote(record["validation_id"])["promoted"])


if __name__ == "__main__":
    unittest.main()

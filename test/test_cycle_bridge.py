import datetime as dt
import unittest

from cli import cycle_bridge


class CycleBridgeTests(unittest.TestCase):
    def test_display_item_name_includes_project_and_cycle_id(self) -> None:
        self.assertEqual(
            cycle_bridge.display_item_name(2169, "chatsearch"),
            "[CYCLE] chatsearch CYCLE-2169",
        )

    def test_dataset_marker_is_queryable_and_stable(self) -> None:
        self.assertEqual(
            cycle_bridge.dataset_marker(2169, 5348),
            "source=chatsearch.cycle_detections; cycle_id=2169; session_id=5348",
        )

    def test_objective_prefers_description_when_present(self) -> None:
        cycle = {
            "CYCLE_ID": 10,
            "SESSION_ID": 20,
            "CYCLE_TYPE": "intra-session",
            "DESC_TEXT": "Specific operator loop detected.",
            "DETECTED_AT": dt.datetime(2026, 3, 6, 1, 2, 3, tzinfo=dt.timezone.utc),
        }
        self.assertEqual(cycle_bridge.objective_text(cycle), "Specific operator loop detected.")

    def test_build_properties_uses_cycle_projection_title(self) -> None:
        cycle = {
            "CYCLE_ID": 2169,
            "SESSION_ID": 5348,
            "CYCLE_TYPE": "intra-session",
            "DESC_TEXT": None,
            "DETECTED_AT": dt.datetime(2026, 3, 6, 1, 2, 3, tzinfo=dt.timezone.utc),
        }
        notion_cfg = cycle_bridge.NotionConfig(
            token="test-token",
            project_id="project-123",
            project_label="chatsearch",
            dispatch_via="Claude Code",
        )
        properties = cycle_bridge.build_properties(
            cycle,
            marker=cycle_bridge.dataset_marker(2169, 5348),
            notion_cfg=notion_cfg,
        )

        self.assertEqual(
            properties["Item Name"]["title"][0]["text"]["content"],
            "[CYCLE] chatsearch CYCLE-2169",
        )
        self.assertEqual(properties["Type"]["select"]["name"], "Feasibility Analysis")
        self.assertEqual(properties["Project"]["relation"], [{"id": "project-123"}])
        self.assertEqual(properties["Dispatch Via"]["select"]["name"], "Claude Code")
        self.assertIn("cycle_id=2169", properties["Dataset"]["rich_text"][0]["text"]["content"])

    def test_cycle_sort_key_orders_by_timestamp_then_cycle_id(self) -> None:
        base = dt.datetime(2026, 3, 6, 1, 2, 3, tzinfo=dt.timezone.utc)
        a = {"DETECTED_AT": base, "CYCLE_ID": 10}
        b = {"DETECTED_AT": base, "CYCLE_ID": 11}
        self.assertLess(cycle_bridge.cycle_sort_key(a), cycle_bridge.cycle_sort_key(b))


if __name__ == "__main__":
    unittest.main()

import sys
import unittest
from pathlib import Path
from unittest import mock
from datetime import datetime, timezone, timedelta

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "cli"))

import notion_api
import github_return
import lab_auditor

class LabLoopTests(unittest.TestCase):
    def setUp(self):
        self.client = notion_api.NotionAPIClient("fake-token")
        self.mock_page_id = "page-123"

    def test_atomic_consume_sends_correct_payload(self):
        with mock.patch.object(self.client, "_request") as mock_request:
            mock_request.return_value = {"id": self.mock_page_id}
            
            self.client.atomic_consume(
                self.mock_page_id, 
                "Dispatch Requested", 
                "DRCA",
                extra_properties={"Status": {"status": {"name": "In Progress"}}}
            )
            
            # Verify the top-level _request was called via update_page
            self.assertTrue(mock_request.called)
            args = mock_request.call_args
            self.assertEqual(args.args[0], "PATCH")
            self.assertEqual(args.args[1], f"pages/{self.mock_page_id}")
            
            payload = args.args[2]["properties"]
            self.assertEqual(payload["Dispatch Requested"]["checkbox"], False)
            self.assertIn("DRCA", payload)
            self.assertEqual(payload["Status"]["status"]["name"], "In Progress")

    def _work_item(self, name, *, status="Done", created=None, last_edited=None,
                   dispatch_received=None, dispatch_consumed=None,
                   librarian_received=None, librarian_consumed=None,
                   synthesis_complete=False, github_issue_url="https://github.com/org/repo/issues/1"):
        created = created or datetime(2026, 3, 8, tzinfo=timezone.utc)
        last_edited = last_edited or datetime.now(timezone.utc) - timedelta(days=10)
        return {
            "id": f"id-{name}",
            "created_time": created.isoformat().replace("+00:00", "Z"),
            "last_edited_time": last_edited.isoformat().replace("+00:00", "Z"),
            "properties": {
                "Item Name": {"title": [{"plain_text": name}]},
                "Status": {"status": {"name": status}},
                "Dispatch Requested Received At": {"date": {"start": dispatch_received}} if dispatch_received else {"date": None},
                "Dispatch Requested Consumed At": {"date": {"start": dispatch_consumed}} if dispatch_consumed else {"date": None},
                "Librarian Request Received At": {"date": {"start": librarian_received}} if librarian_received else {"date": None},
                "Librarian Request Consumed At": {"date": {"start": librarian_consumed}} if librarian_consumed else {"date": None},
                "Synthesis Complete": {"checkbox": synthesis_complete},
                "GitHub Issue URL": {"url": github_issue_url},
                "Project": {"relation": []},
                "Outcome": {"rich_text": []},
            },
        }

    def test_completed_librarian_cycle_is_informational_not_blocking(self):
        page = self._work_item(
            "LR-CYCLE-1",
            librarian_received="2026-03-08T00:00:00Z",
            librarian_consumed="2026-03-08T00:05:00Z",
        )
        violations, counts = lab_auditor.check_lab_loop(
            self.client,
            [page],
            {},
            lab_auditor.Counter({"id-LR-CYCLE-1": 2}),
        )

        self.assertEqual(lab_auditor._blocking_violation_count(violations), 0)
        self.assertEqual(counts.get("e1", 0), 0)
        self.assertEqual(counts.get("e1_info", 0), 1)
        self.assertTrue(any(v.severity == "INFO" and v.code == "E.1" for v in violations))

    def test_stale_terminal_unsynthesized_counts_done_passed_and_kill(self):
        stale = datetime.now(timezone.utc) - timedelta(days=5)
        pages = [
            self._work_item("DONE-1", status="Done", last_edited=stale),
            self._work_item("PASSED-1", status="Passed", last_edited=stale),
            self._work_item("KILL-1", status="Kill Condition Met", last_edited=stale),
        ]
        violations, counts = lab_auditor.check_lab_loop(
            self.client,
            pages,
            {},
            lab_auditor.Counter({"id-DONE-1": 2, "id-PASSED-1": 2, "id-KILL-1": 2}),
        )

        e4_nice = [v for v in violations if v.code == "E.4" and v.severity == "NICE-TO-HAVE"]
        self.assertEqual(len(e4_nice), 3)
        self.assertEqual(counts.get("e4_nice", 0), 3)

    def test_dispatch_invariants_follow_drra_signal(self):
        # DRRA set and DRCA set → E.1 (not cleared after consume)
        # Status Done and DRRA set but no DRCA → E.3, but here DRCA is set
        # so E.3 should NOT fire; E.1 should fire for the orphan receive.
        page = self._work_item(
            "DISPATCH-1",
            status="Done",
            dispatch_received="2026-03-08T00:00:00Z",
            dispatch_consumed="2026-03-08T01:00:00Z",
        )
        violations, counts = lab_auditor.check_lab_loop(
            self.client,
            [page],
            {},
            lab_auditor.Counter({"id-DISPATCH-1": 2}),
        )

        self.assertEqual(counts.get("e1", 0), 1)
        self.assertEqual(counts.get("e3", 0), 0)
        self.assertEqual(counts.get("e7", 0), 0)
        self.assertTrue(any(v.code == "E.1" for v in violations))

    def test_post_epoch_gate_applies_to_e1_and_e3(self):
        page = self._work_item(
            "LEGACY-1",
            status="Done",
            created=datetime(2026, 3, 5, tzinfo=timezone.utc),
            dispatch_received="2026-03-08T00:00:00Z",
            dispatch_consumed="2026-03-08T01:00:00Z",
        )
        violations, counts = lab_auditor.check_lab_loop(
            self.client,
            [page],
            {},
            lab_auditor.Counter({"id-LEGACY-1": 2}),
        )

        self.assertEqual(counts.get("e1", 0), 0)
        self.assertEqual(counts.get("e3", 0), 0)
        self.assertFalse(any(v.code in {"E.1", "E.3"} and v.severity == "MUST-FIX" for v in violations))

    def test_github_return_flow(self):
        with mock.patch.object(self.client, "_request") as mock_request:
            # 1. update_page (PATCH)
            # 2. append_block_children (PATCH)
            # 3. create_page (POST)
            mock_request.side_effect = [
                {"id": self.mock_page_id}, # update
                {}, # append blocks
                {}, # create audit log
            ]
            
            github_return.perform_return(self.client, self.mock_page_id, "Closing summary")
            
            # Verify calls
            # Call 1: update_page
            self.assertEqual(mock_request.call_args_list[0].args[0], "PATCH")
            self.assertEqual(mock_request.call_args_list[0].args[1], f"pages/{self.mock_page_id}")
            
            # Call 2: append_block_children
            self.assertEqual(mock_request.call_args_list[1].args[0], "PATCH")
            self.assertIn("children", mock_request.call_args_list[1].args[1])
            
            # Call 3: create_page (audit log)
            self.assertEqual(mock_request.call_args_list[2].args[0], "POST")
            self.assertEqual(mock_request.call_args_list[2].args[1], "pages")
            self.assertEqual(mock_request.call_args_list[2].args[2]["parent"]["database_id"], github_return.CFG.audit_log_db_id)

if __name__ == "__main__":
    unittest.main()

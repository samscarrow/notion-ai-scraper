import sys
import unittest
from pathlib import Path
from unittest import mock
from datetime import datetime, timezone

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

    def test_auditor_detects_safety_violation(self):
        with mock.patch.object(self.client, "_request") as mock_request:
            # Setup: Mock the query response
            mock_request.return_value = {
                "object": "list",
                "results": [{
                    "properties": {
                        "Item Name": {"title": [{"plain_text": "ZOMBIE-1"}]},
                        "Dispatch Requested": {"checkbox": True},
                        "Dispatch Requested Consumed At": {"date": {"start": "2026-03-07T00:00:00Z"}},
                        "Status": {"status": {"name": "In Progress"}}
                    }
                }],
                "has_more": False
            }
            
            violations = lab_auditor.check_invariants(self.client)
            self.assertEqual(violations, 1)

    def test_auditor_detects_exclusive_ownership_violation(self):
        with mock.patch.object(self.client, "_request") as mock_request:
            # Setup: Mock the query response
            mock_request.return_value = {
                "object": "list",
                "results": [{
                    "properties": {
                        "Item Name": {"title": [{"plain_text": "DANGLING-1"}]},
                        "Status": {"status": {"name": "Done"}},
                        "GitHub Issue URL": {"url": "https://github.com/org/repo/issues/1"},
                        "Active GitHub Issue": {"url": "https://github.com/org/repo/issues/1"}
                    }
                }],
                "has_more": False
            }
            
            violations = lab_auditor.check_invariants(self.client)
            self.assertEqual(violations, 1)

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
            self.assertEqual(mock_request.call_args_list[2].args[2]["parent"]["database_id"], github_return.AUDIT_LOG_DB_ID)

if __name__ == "__main__":
    unittest.main()

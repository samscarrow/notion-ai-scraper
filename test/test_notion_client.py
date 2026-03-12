import sys
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "cli"))

import notion_client  # noqa: E402


class NotionClientTests(unittest.TestCase):
    def test_list_workflow_threads_follows_next_cursor(self) -> None:
        first_page = {
            "threadIds": ["thread-1", "thread-2"],
            "transcripts": [
                {
                    "id": "thread-1",
                    "created_at": 101,
                    "created_by_display_name": "Sam",
                    "type": "workflow",
                },
                {
                    "id": "thread-2",
                    "title": "Skip me",
                    "type": "workflow",
                },
            ],
            "recordMap": {
                "thread": {
                    "thread-1": {
                        "value": {
                            "value": {
                                "id": "thread-1",
                                "alive": True,
                                "data": {"title": "Recovered title", "trigger_id": "trigger-1"},
                                "updated_time": 202,
                                "type": "workflow",
                            }
                        }
                    },
                    "thread-2": {
                        "value": {
                            "value": {
                                "id": "thread-2",
                                "alive": False,
                                "data": {"title": "Deleted thread"},
                            }
                        }
                    },
                }
            },
            "nextCursor": "cursor-1",
        }
        second_page = {
            "threadIds": ["thread-3"],
            "transcripts": [
                {
                    "id": "thread-3",
                    "title": "Page two thread",
                    "run_id": "run-3",
                    "type": "workflow",
                }
            ],
            "recordMap": {},
        }

        with mock.patch.object(
            notion_client,
            "_post",
            side_effect=[first_page, second_page],
        ) as post_mock:
            threads = notion_client.list_workflow_threads(
                "workflow-1",
                "space-1",
                "token",
                "user-1",
                limit=2,
            )

        self.assertEqual(
            threads,
            [
                {
                    "id": "thread-1",
                    "title": "Recovered title",
                    "created_at": 101,
                    "updated_at": 202,
                    "created_by_display_name": "Sam",
                    "trigger_id": "trigger-1",
                    "type": "workflow",
                },
                {
                    "id": "thread-3",
                    "title": "Page two thread",
                    "run_id": "run-3",
                    "type": "workflow",
                },
            ],
        )
        first_payload = post_mock.call_args_list[0].args[1]
        second_payload = post_mock.call_args_list[1].args[1]
        self.assertEqual(first_payload, {"workflowId": "workflow-1", "spaceId": "space-1", "limit": 2, "userId": "user-1"})
        self.assertEqual(
            second_payload,
            {
                "workflowId": "workflow-1",
                "spaceId": "space-1",
                "limit": 2,
                "userId": "user-1",
                "cursor": "cursor-1",
            },
        )

    def test_archive_threads_uses_delete_chat_transaction_shape(self) -> None:
        with mock.patch.object(notion_client, "_post", return_value={}) as post_mock:
            archived = notion_client.archive_threads(
                ["thread-1", "thread-2", "thread-1"],
                "space-1",
                "token",
                "user-1",
            )

        self.assertEqual(archived, ["thread-1", "thread-2"])
        endpoint, payload, token, user_id, dry_run = post_mock.call_args.args
        self.assertEqual(endpoint, "saveTransactionsFanout")
        self.assertEqual(token, "token")
        self.assertEqual(user_id, "user-1")
        self.assertFalse(dry_run)
        self.assertEqual(payload["unretryable_error_behavior"], "continue")
        tx = payload["transactions"][0]
        self.assertEqual(
            tx["debug"]["userAction"],
            "assistantChatHistoryItem.deleteInferenceChatTranscript",
        )
        self.assertEqual(
            [op["pointer"]["id"] for op in tx["operations"]],
            ["thread-1", "thread-2"],
        )
        self.assertEqual(
            tx["operations"][0]["args"],
            {
                "alive": False,
                "current_inference_id": None,
                "current_inference_lease_expiration": None,
            },
        )

    def test_publish_agent_archives_threads_after_success(self) -> None:
        with mock.patch.object(
            notion_client,
            "_post",
            return_value={"workflowArtifactId": "artifact-1", "version": 7},
        ) as post_mock, mock.patch.object(
            notion_client,
            "archive_workflow_threads",
            return_value={"count": 2, "threadIds": ["thread-1", "thread-2"], "threads": []},
        ) as cleanup_mock:
            result = notion_client.publish_agent("workflow-1", "space-1", "token", "user-1")

        self.assertEqual(result["workflowArtifactId"], "artifact-1")
        self.assertEqual(result["version"], 7)
        self.assertEqual(result["archivedThreadCount"], 2)
        self.assertEqual(result["archivedThreadIds"], ["thread-1", "thread-2"])
        post_mock.assert_called_once_with(
            "publishCustomAgentVersion",
            {"workflowId": "workflow-1", "spaceId": "space-1"},
            "token",
            "user-1",
            False,
        )
        cleanup_mock.assert_called_once_with("workflow-1", "space-1", "token", "user-1")

    def test_publish_agent_keeps_warning_and_cleanup(self) -> None:
        with mock.patch.object(
            notion_client,
            "_post",
            side_effect=RuntimeError("publishCustomAgentVersion returned incomplete_ancestor_path"),
        ), mock.patch.object(
            notion_client,
            "archive_workflow_threads",
            return_value={"count": 1, "threadIds": ["thread-1"], "threads": []},
        ):
            result = notion_client.publish_agent("workflow-1", "space-1", "token", "user-1")

        self.assertEqual(result["warning"], "incomplete_ancestor_path")
        self.assertEqual(result["archivedThreadCount"], 1)
        self.assertEqual(result["archivedThreadIds"], ["thread-1"])

    def test_publish_agent_reports_cleanup_failure(self) -> None:
        with mock.patch.object(
            notion_client,
            "_post",
            return_value={"workflowArtifactId": "artifact-1", "version": 7},
        ), mock.patch.object(
            notion_client,
            "archive_workflow_threads",
            side_effect=RuntimeError("cleanup failed"),
        ):
            result = notion_client.publish_agent("workflow-1", "space-1", "token", "user-1")

        self.assertEqual(result["workflowArtifactId"], "artifact-1")
        self.assertEqual(result["threadCleanupWarning"], "cleanup failed")


if __name__ == "__main__":
    unittest.main()

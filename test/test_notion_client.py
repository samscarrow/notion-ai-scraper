import sys
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "cli"))

import notion_client  # noqa: E402
import notion_agent_config  # noqa: E402
import notion_threads  # noqa: E402


class NotionClientTests(unittest.TestCase):
    def test_get_thread_conversation_surfaces_trigger_system_messages(self) -> None:
        thread_id = "thread-1"
        with mock.patch.object(
            notion_threads,
            "read_records",
            side_effect=[
                {
                    thread_id: {
                        "id": thread_id,
                        "space_id": "space-1",
                        "messages": ["msg-config", "msg-context", "msg-trigger"],
                        "data": {"title": "Untitled chat"},
                        "created_time": 100,
                    }
                },
                {
                    "msg-config": {
                        "id": "msg-config",
                        "step": {
                            "type": "config",
                            "value": {"type": "workflow", "workflowId": "workflow-1"},
                        },
                        "created_time": 101,
                    },
                    "msg-context": {
                        "id": "msg-context",
                        "step": {
                            "type": "context",
                            "value": {
                                "surface": "custom_agent",
                                "agentName": "Lab Dispatcher",
                                "context_page_id": "page-1",
                            },
                        },
                        "created_time": 102,
                    },
                    "msg-trigger": {
                        "id": "msg-trigger",
                        "step": {
                            "type": "agent-trigger",
                            "triggerId": "trigger-1",
                            "workflowId": "workflow-1",
                            "data": {
                                "update": {
                                    "after": {
                                        "Item Name": "Adaptive Comparative Adjudication Engine",
                                        "date:Lab Dispatch Requested At:start": "2026-03-24T14:00:00.000Z",
                                    }
                                }
                            },
                        },
                        "created_time": 103,
                    },
                },
            ],
        ):
            convo = notion_client.get_thread_conversation(thread_id, "token", "user-1")

        self.assertEqual(convo["threadId"], thread_id)
        self.assertEqual(
            [turn["stepType"] for turn in convo["turns"]],
            ["config", "context", "agent-trigger"],
        )
        self.assertEqual(
            convo["turns"][2]["trigger"]["itemName"],
            "Adaptive Comparative Adjudication Engine",
        )

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
            notion_threads,
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
        self.assertEqual(first_payload, {"workflowId": "workflow-1", "spaceId": "space-1", "limit": 2})
        self.assertEqual(
            second_payload,
            {
                "workflowId": "workflow-1",
                "spaceId": "space-1",
                "limit": 2,
                "cursor": "cursor-1",
            },
        )

    def test_list_workflow_threads_ignores_user_id_to_keep_trigger_runs_visible(self) -> None:
        with mock.patch.object(
            notion_threads,
            "_post",
            return_value={
                "threadIds": ["thread-trigger-1"],
                "transcripts": [
                    {
                        "id": "thread-trigger-1",
                        "created_at": 101,
                        "created_by_display_name": "Lab Dispatcher",
                        "trigger_id": "trigger-1",
                        "type": "workflow",
                    }
                ],
                "recordMap": {},
            },
        ) as post_mock:
            threads = notion_client.list_workflow_threads(
                "workflow-1",
                "space-1",
                "token",
                "user-1",
                limit=10,
            )

        self.assertEqual(
            threads,
            [
                {
                    "id": "thread-trigger-1",
                    "created_at": 101,
                    "created_by_display_name": "Lab Dispatcher",
                    "trigger_id": "trigger-1",
                    "type": "workflow",
                }
            ],
        )
        payload = post_mock.call_args.args[1]
        self.assertNotIn("userId", payload)

    def test_archive_threads_uses_delete_chat_transaction_shape(self) -> None:
        with mock.patch.object(notion_threads, "_post", return_value={}) as post_mock:
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
            notion_agent_config,
            "_post",
            return_value={"workflowArtifactId": "artifact-1", "version": 7},
        ) as post_mock, mock.patch.object(
            notion_agent_config.notion_threads,
            "archive_workflow_threads",
            return_value={"count": 2, "threadIds": ["thread-1", "thread-2"], "threads": []},
        ) as cleanup_mock:
            result = notion_agent_config.publish_agent("workflow-1", "space-1", "token", "user-1")

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
        cleanup_mock.assert_called_once_with(
            "workflow-1",
            "space-1",
            "token",
            "user-1",
        )

    def test_publish_agent_skips_archive_on_warning(self) -> None:
        with mock.patch.object(
            notion_agent_config,
            "_post",
            side_effect=RuntimeError("publishCustomAgentVersion returned incomplete_ancestor_path"),
        ), mock.patch.object(
            notion_agent_config.notion_threads,
            "archive_workflow_threads",
        ) as cleanup_mock:
            result = notion_agent_config.publish_agent("workflow-1", "space-1", "token", "user-1")

        self.assertEqual(result["warning"], "incomplete_ancestor_path")
        # Archive must NOT run when publish failed
        cleanup_mock.assert_not_called()
        self.assertNotIn("archivedThreadCount", result)

    def test_publish_agent_reports_cleanup_failure(self) -> None:
        with mock.patch.object(
            notion_agent_config,
            "_post",
            return_value={"workflowArtifactId": "artifact-1", "version": 7},
        ), mock.patch.object(
            notion_agent_config.notion_threads,
            "archive_workflow_threads",
            side_effect=RuntimeError("cleanup failed"),
        ):
            result = notion_agent_config.publish_agent("workflow-1", "space-1", "token", "user-1")

        self.assertEqual(result["workflowArtifactId"], "artifact-1")
        self.assertEqual(result["threadCleanupWarning"], "cleanup failed")

    def test_find_stale_trigger_threads_filters_by_artifact_mismatch(self) -> None:
        threads = [
            {"id": "manual-1"},
            {"id": "trigger-stale", "trigger_id": "trigger-a"},
            {"id": "trigger-current", "trigger_id": "trigger-b"},
        ]
        with mock.patch.object(
            notion_threads,
            "list_workflow_threads",
            return_value=threads,
        ), mock.patch.object(
            notion_threads.notion_agent_config,
            "get_workflow_record",
            return_value={"data": {"published_artifact_pointer": {"id": "artifact-current"}}},
        ), mock.patch.object(
            notion_threads,
            "get_thread_conversation",
            return_value={"turns": []},
        ), mock.patch.object(
            notion_threads,
            "read_records",
            side_effect=[
                {
                    "trigger-stale": {"data": {"workflow_artifact_pointer": {"id": "artifact-old"}}},
                },
                {
                    "trigger-current": {"data": {"workflow_artifact_pointer": {"id": "artifact-current"}}},
                },
            ],
        ):
            result = notion_threads.find_stale_trigger_threads(
                "workflow-1",
                "space-1",
                "token",
                "user-1",
            )

        self.assertEqual(result["currentArtifactId"], "artifact-current")
        self.assertEqual(result["threadIds"], ["trigger-stale"])
        self.assertEqual(result["count"], 1)

    def test_archive_selected_workflow_threads_is_explicit(self) -> None:
        with mock.patch.object(
            notion_threads,
            "archive_threads",
            return_value=["trigger-1"],
        ) as archive_mock:
            result = notion_threads.archive_selected_workflow_threads(
                ["trigger-1"],
                "space-1",
                "token",
                "user-1",
            )

        archive_mock.assert_called_once_with(["trigger-1"], "space-1", "token", "user-1")
        self.assertEqual(result["threadIds"], ["trigger-1"])
        self.assertEqual(result["count"], 1)


if __name__ == "__main__":
    unittest.main()

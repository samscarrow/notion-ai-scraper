import os
import sys
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "cli"))
import github_return


def test_perform_return_preserves_github_issue_url() -> None:
    client = MagicMock()

    with patch.object(github_return.notion_api, "now_iso", return_value="2026-03-24T17:13:00Z"):
        github_return.perform_return(client, "page-123", "merged successfully")

    _, kwargs = client.update_page.call_args
    props = kwargs["properties"]
    assert props["Status"]["status"]["name"] == "Awaiting Intake"
    assert props["Return Received At"]["date"]["start"] == "2026-03-24T17:13:00Z"
    assert "GitHub Issue URL" not in props

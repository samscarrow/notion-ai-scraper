import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "cli"))

import block_builder  # noqa: E402


class BlockBuilderIRTests(unittest.TestCase):
    def test_markdown_to_ir_is_sparse(self) -> None:
        md = "\n".join([
            "# Heading",
            "",
            "- hello **world** {{page:12345678-1234-1234-1234-123456789abc}}",
            "  1. nested",
            "> 📌 note",
        ])

        blocks = block_builder.markdown_to_ir(md)

        self.assertEqual(blocks[0]["type"], "heading")
        self.assertNotIn("properties", blocks[0])
        self.assertEqual(blocks[1]["type"], "list_item")
        self.assertEqual(blocks[1]["list_kind"], "bulleted")

        spans = blocks[1]["spans"]
        self.assertEqual(spans[0], {"type": "text", "text": "hello "})
        self.assertEqual(spans[1], {"type": "text", "text": "world", "marks": ["bold"]})
        self.assertEqual(
            spans[2],
            {"type": "text", "text": " "},
        )
        self.assertEqual(
            spans[3],
            {"type": "mention", "kind": "page", "id": "12345678-1234-1234-1234-123456789abc"},
        )

        nested = blocks[1]["children"][0]
        self.assertEqual(nested["type"], "list_item")
        self.assertEqual(nested["list_kind"], "numbered")

    def test_ir_round_trip_through_notion_blocks(self) -> None:
        md = "\n".join([
            "## Hello",
            "- item",
            "  - nested `code`",
            "> 📌 note",
            "```py",
            "print('hi')",
            "```",
            "---",
        ])

        ir_blocks = block_builder.markdown_to_ir(md)
        notion_blocks = block_builder.ir_to_notion_blocks(ir_blocks)

        root_id = "root"
        blocks_map = {
            root_id: {"value": {"content": []}},
        }

        counter = 0

        def add_block(parent_id: str, block: dict) -> None:
            nonlocal counter
            counter += 1
            block_id = f"block-{counter}"
            blocks_map[root_id if parent_id == root_id else parent_id]["value"].setdefault("content", []).append(block_id)
            value = {**block, "id": block_id}
            children = value.pop("children", None)
            blocks_map[block_id] = {"value": value}
            if children:
                blocks_map[block_id]["value"]["content"] = []
                for child in children:
                    add_block(block_id, child)

        for notion_block in notion_blocks:
            add_block(root_id, notion_block)

        rebuilt_ir = block_builder.notion_blocks_to_ir(blocks_map, root_id)
        rebuilt_md = block_builder.blocks_to_markdown(blocks_map, root_id)

        self.assertEqual(rebuilt_ir, ir_blocks)
        self.assertEqual(rebuilt_md, md)


if __name__ == "__main__":
    unittest.main()

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from export_agent_docs import BASE, render, split_document


class ExportTests(unittest.TestCase):
    def test_frontmatter_and_liquid_links_become_literal_markdown(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "example.md"
            path.write_text('---\nlayout: post\ntitle: "Example"\n---\n\n'
                            '[Guide]({{ "/compatibility/" | relative_url }})\n'
                            '[Post]({% post_url 2026-10-15-example %})\n'
                            '[Source](https://example.org/issue)\n')
            title, output = render(path)
        self.assertEqual(title, "Example")
        self.assertTrue(output.startswith("# Example\n"))
        self.assertNotIn("layout: post", output)
        self.assertNotIn("{{", output)
        self.assertNotIn("{%", output)
        self.assertIn(BASE + "/compatibility/", output)
        self.assertIn(BASE + "/markdown/2026-10-15-example.md", output)
        self.assertIn("https://example.org/issue", output)

    def test_plain_markdown_is_preserved(self):
        metadata, body = split_document("# Hello\n\nBody\n")
        self.assertEqual(metadata, {})
        self.assertEqual(body, "# Hello\n\nBody\n")


if __name__ == "__main__":
    unittest.main()

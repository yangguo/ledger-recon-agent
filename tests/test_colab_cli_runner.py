import unittest
from pathlib import Path


class ColabCliRunnerTests(unittest.TestCase):
    def test_build_session_commands_provisions_uploads_and_opens_console(self):
        from scripts.colab_qwen_api_cli import build_session_commands

        commands = build_session_commands(
            session_name="qwen-api",
            gpu="L4",
            launcher_path=Path("colab/qwen36_27b_api.py"),
        )

        self.assertEqual(commands[0], ["colab", "new", "-s", "qwen-api", "--gpu", "L4"])
        self.assertEqual(
            commands[1],
            [
                "colab",
                "upload",
                "-s",
                "qwen-api",
                "colab/qwen36_27b_api.py",
                "/content/qwen36_27b_api.py",
            ],
        )
        self.assertEqual(commands[2], ["colab", "console", "-s", "qwen-api"])


if __name__ == "__main__":
    unittest.main()

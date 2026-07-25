import unittest


class ColabQwenLauncherTests(unittest.TestCase):
    def test_server_command_uses_configured_model_and_api_key(self):
        from colab.qwen36_27b_api import build_llama_server_command

        command = build_llama_server_command("/content/model.gguf", "secret", 8192)

        self.assertIn("--model /content/model.gguf", command)
        self.assertIn("--api-key secret", command)
        self.assertIn("--port 8000", command)


if __name__ == "__main__":
    unittest.main()

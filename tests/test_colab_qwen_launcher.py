import unittest


class ColabQwenLauncherTests(unittest.TestCase):
    def test_server_arguments_preserve_api_key_as_one_argument(self):
        from colab.qwen36_27b_api import build_llama_server_arguments

        arguments = build_llama_server_arguments("/content/model.gguf", "key with spaces", 8192)

        self.assertEqual(arguments[arguments.index("--api-key") + 1], "key with spaces")

    def test_server_command_uses_configured_model_and_api_key(self):
        from colab.qwen36_27b_api import build_llama_server_command

        command = build_llama_server_command("/content/model.gguf", "secret", 8192)

        self.assertIn("--model /content/model.gguf", command)
        self.assertIn("--api-key secret", command)
        self.assertIn("--port 8000", command)


if __name__ == "__main__":
    unittest.main()

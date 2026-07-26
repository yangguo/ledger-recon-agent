import unittest
from unittest.mock import patch
from urllib.error import HTTPError


class KaggleQwenLauncherTests(unittest.TestCase):
    def test_model_constants_select_official_qwen3_14b_q4(self):
        from kaggle.qwen3_14b_api import MINIMUM_FREE_VRAM_MIB, MODEL_FILE, MODEL_REPO

        self.assertEqual(MODEL_REPO, "Qwen/Qwen3-14B-GGUF")
        self.assertEqual(MODEL_FILE, "Qwen3-14B-Q4_K_M.gguf")
        self.assertEqual(MINIMUM_FREE_VRAM_MIB, 14_000)

    def test_server_arguments_bind_only_to_loopback(self):
        from kaggle.qwen3_14b_api import build_llama_server_arguments

        arguments = build_llama_server_arguments("/kaggle/model.gguf", "key with spaces", 8192)

        self.assertEqual(arguments[arguments.index("--host") + 1], "127.0.0.1")
        self.assertEqual(arguments[arguments.index("--api-key") + 1], "key with spaces")

    def test_required_secrets_are_loaded_without_optional_hf_token(self):
        from kaggle.qwen3_14b_api import load_secrets

        class Secrets:
            def get_secret(self, name):
                return {"LLM_API_KEY": "llm-key", "NGROK_AUTHTOKEN": "ngrok-token"}[name]

        self.assertEqual(load_secrets(Secrets()), ("llm-key", "ngrok-token", None))

    def test_ngrok_command_targets_loopback_port(self):
        from kaggle.qwen3_14b_api import build_ngrok_arguments

        self.assertEqual(build_ngrok_arguments("/usr/local/bin/ngrok"), ["/usr/local/bin/ngrok", "http", "127.0.0.1:8000"])

    def test_profile_never_echoes_api_key(self):
        from kaggle.qwen3_14b_api import render_local_env_profile

        profile = render_local_env_profile("https://demo.ngrok.app", "super-secret")

        self.assertIn("LLM_BASE_URL=https://demo.ngrok.app/v1", profile)
        self.assertNotIn("super-secret", profile)

    def test_extract_ngrok_url_prefers_https_tunnel(self):
        from kaggle.qwen3_14b_api import extract_ngrok_public_base_url

        self.assertEqual(
            extract_ngrok_public_base_url({"tunnels": [{"public_url": "https://demo.ngrok.app"}]}),
            "https://demo.ngrok.app/v1",
        )

    def test_missing_required_secret_has_safe_error(self):
        from kaggle.qwen3_14b_api import load_secrets

        class Secrets:
            def get_secret(self, name):
                return {"LLM_API_KEY": "secret", "NGROK_AUTHTOKEN": ""}[name]

        with self.assertRaisesRegex(RuntimeError, "NGROK_AUTHTOKEN") as error:
            load_secrets(Secrets())
        self.assertNotIn("secret", str(error.exception))

    def test_authentication_gate_accepts_401(self):
        from kaggle.qwen3_14b_api import require_authentication

        with patch("kaggle.qwen3_14b_api.urlopen", side_effect=HTTPError("url", 401, "Unauthorized", {}, None)):
            require_authentication()

    def test_27b_config_uses_qwen36_gguf_and_requires_two_gpus(self):
        from kaggle.qwen3_14b_api import model_config, require_model_vram

        config = model_config("27B")

        self.assertEqual(config["repo"], "unsloth/Qwen3.6-27B-GGUF")
        self.assertEqual(config["file"], "Qwen3.6-27B-Q4_K_M.gguf")
        with self.assertRaisesRegex(RuntimeError, "two GPUs"):
            require_model_vram(config, [30_000])
        require_model_vram(config, [14_000, 14_000])

    def test_27b_server_arguments_use_equal_layer_split(self):
        from kaggle.qwen3_14b_api import build_llama_server_arguments

        arguments = build_llama_server_arguments("/kaggle/model.gguf", "key", 8192, model_size="27B")

        self.assertEqual(arguments[arguments.index("--split-mode") + 1], "layer")
        self.assertEqual(arguments[arguments.index("--tensor-split") + 1], "1,1")


if __name__ == "__main__":
    unittest.main()

import unittest
from unittest.mock import patch
from urllib.error import HTTPError


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

    def test_authentication_gate_accepts_401_from_protected_route(self):
        from colab.qwen36_27b_api import require_authentication

        with patch(
            "colab.qwen36_27b_api.urlopen",
            side_effect=HTTPError("http://127.0.0.1:8000/v1/props", 401, "Unauthorized", {}, None),
        ):
            require_authentication()

    def test_ngrok_command_targets_loopback_port(self):
        from colab.qwen36_27b_api import build_ngrok_arguments

        self.assertEqual(
            build_ngrok_arguments("/usr/local/bin/ngrok"),
            ["/usr/local/bin/ngrok", "http", "127.0.0.1:8000"],
        )

    def test_quick_tunnel_command_targets_loopback_port(self):
        from colab.qwen36_27b_api import build_quick_tunnel_arguments

        self.assertEqual(
            build_quick_tunnel_arguments("/usr/local/bin/cloudflared"),
            [
                "/usr/local/bin/cloudflared",
                "tunnel",
                "--no-autoupdate",
                "--url",
                "http://127.0.0.1:8000",
            ],
        )

    def test_named_tunnel_command_keeps_token_as_one_argument(self):
        from colab.qwen36_27b_api import build_named_tunnel_arguments

        arguments = build_named_tunnel_arguments("/usr/local/bin/cloudflared", "token with spaces")

        self.assertEqual(arguments[arguments.index("--token") + 1], "token with spaces")

    def test_extract_quick_tunnel_url_returns_v1_address(self):
        from colab.qwen36_27b_api import extract_quick_tunnel_url

        self.assertEqual(
            extract_quick_tunnel_url("INF | https://demo.trycloudflare.com |"),
            "https://demo.trycloudflare.com/v1",
        )

    def test_tunnel_provider_selection_accepts_three_documented_choices(self):
        from colab.qwen36_27b_api import select_tunnel_provider

        self.assertEqual(select_tunnel_provider("1"), "cloudflare")
        self.assertEqual(select_tunnel_provider("2"), "ngrok")
        self.assertEqual(select_tunnel_provider("3"), "quick")

    def test_extract_ngrok_url_prefers_https_tunnel(self):
        from colab.qwen36_27b_api import extract_ngrok_public_base_url

        self.assertEqual(
            extract_ngrok_public_base_url(
                {
                    "tunnels": [
                        {"public_url": "http://example.ngrok.app"},
                        {"public_url": "https://example.ngrok.app"},
                    ]
                }
            ),
            "https://example.ngrok.app/v1",
        )

    def test_extract_ngrok_url_rejects_missing_https_tunnel(self):
        from colab.qwen36_27b_api import extract_ngrok_public_base_url

        with self.assertRaisesRegex(RuntimeError, "HTTPS"):
            extract_ngrok_public_base_url({"tunnels": []})


if __name__ == "__main__":
    unittest.main()

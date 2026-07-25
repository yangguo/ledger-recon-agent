import json
import tempfile
import unittest
from pathlib import Path


class LlmProfileTests(unittest.TestCase):
    def test_colab_profile_exports_openai_compatible_variables(self):
        from scripts.llm_profiles import build_exports, load_profile

        with tempfile.TemporaryDirectory() as directory:
            profiles_path = Path(directory) / "profiles.json"
            profiles_path.write_text(
                json.dumps(
                    {
                        "profiles": {
                            "colab": {
                                "base_url": "https://llm.example.com/v1",
                                "model": "qwen3.6-27b-q4",
                                "api_key_env": "COLAB_LLM_API_KEY",
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )

            profile = load_profile(profiles_path, "colab")

        self.assertEqual(
            build_exports(profile),
            {
                "LLM_BASE_URL": "https://llm.example.com/v1",
                "LLM_MODEL": "qwen3.6-27b-q4",
                "LLM_API_KEY": "${COLAB_LLM_API_KEY}",
            },
        )


if __name__ == "__main__":
    unittest.main()

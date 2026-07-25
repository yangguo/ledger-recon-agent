import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


class AgentModelOverrideTests(unittest.TestCase):
    def test_environment_model_override_is_passed_to_chat_openai(self):
        import agents.agent as agent_module

        with tempfile.TemporaryDirectory() as directory:
            config_directory = Path(directory) / "config"
            config_directory.mkdir()
            (config_directory / "agent_llm_config.json").write_text(
                json.dumps({"config": {"model": "original-model"}, "sp": "test"}),
                encoding="utf-8",
            )
            with patch.dict(
                os.environ,
                {
                    "COZE_WORKSPACE_PATH": directory,
                    "COZE_INTEGRATION_MODEL": "qwen3.6-27b-q4_k_m",
                    "COZE_WORKLOAD_IDENTITY_API_KEY": "test-key",
                    "COZE_INTEGRATION_MODEL_BASE_URL": "https://llm.example.com/v1",
                },
                clear=False,
            ), patch.object(agent_module, "ChatOpenAI") as chat_openai, patch.object(agent_module, "create_agent"):
                agent_module.build_agent()

        self.assertEqual(chat_openai.call_args.kwargs["model"], "qwen3.6-27b-q4_k_m")


if __name__ == "__main__":
    unittest.main()

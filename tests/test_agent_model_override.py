import os
import unittest
from unittest.mock import patch


class AgentModelSettingsTests(unittest.TestCase):
    def test_generic_environment_settings_are_passed_to_chat_openai(self):
        import agents.agent as agent_module

        with patch.dict(
            os.environ,
            {
                "LLM_API_KEY": "test-key",
                "LLM_BASE_URL": "https://llm.example.com/v1",
                "LLM_MODEL": "qwen3.6-27b-q4_k_m",
                "LLM_TEMPERATURE": "0.2",
                "LLM_TIMEOUT_SECONDS": "321",
                "LLM_MAX_TOKENS": "456",
            },
            clear=True,
        ), patch.object(agent_module, "ChatOpenAI") as chat_openai, patch.object(agent_module, "create_agent"):
            agent_module.build_agent()

        self.assertEqual(chat_openai.call_args.kwargs["model"], "qwen3.6-27b-q4_k_m")
        self.assertEqual(chat_openai.call_args.kwargs["api_key"], "test-key")
        self.assertEqual(chat_openai.call_args.kwargs["base_url"], "https://llm.example.com/v1")
        self.assertEqual(chat_openai.call_args.kwargs["temperature"], 0.2)
        self.assertEqual(chat_openai.call_args.kwargs["timeout"], 321)
        self.assertEqual(chat_openai.call_args.kwargs["max_tokens"], 456)


if __name__ == "__main__":
    unittest.main()

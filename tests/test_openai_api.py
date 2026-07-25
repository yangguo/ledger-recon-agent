import json
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from fastapi.testclient import TestClient


class FakeAgent:
    async def astream(self, _payload, *, config, stream_mode):
        self.config = config
        self.stream_mode = stream_mode
        yield SimpleNamespace(content="连接成功"), {}


class OpenAICompatibleApiTests(unittest.TestCase):
    def test_streaming_chat_returns_openai_sse_without_platform_runtime(self):
        from main import app

        with patch("main.get_agent", return_value=FakeAgent()), patch(
            "main.llm_settings", return_value=SimpleNamespace(model="qwen-test")
        ):
            response = TestClient(app).post(
                "/v1/chat/completions",
                json={
                    "model": "ignored-by-server",
                    "messages": [{"role": "user", "content": "测试"}],
                    "stream": True,
                    "session_id": "session-123",
                },
            )

        self.assertEqual(response.status_code, 200)
        events = [line.removeprefix("data: ") for line in response.text.splitlines() if line.startswith("data: ")]
        chunks = [json.loads(event) for event in events if event != "[DONE]"]
        self.assertIn(
            "连接成功",
            [chunk["choices"][0]["delta"].get("content") for chunk in chunks],
        )
        self.assertEqual(events[-1], "[DONE]")


if __name__ == "__main__":
    unittest.main()

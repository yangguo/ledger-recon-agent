import importlib
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


class ConfigTests(unittest.TestCase):
    def test_loads_dotenv_from_configured_workspace_not_current_directory(self):
        import config

        with tempfile.TemporaryDirectory() as workspace, tempfile.TemporaryDirectory() as other_directory:
            Path(workspace, ".env").write_text(
                "LLM_API_KEY=workspace-key\nLLM_BASE_URL=https://api.example/v1\nLLM_MODEL=workspace-model\n",
                encoding="utf-8",
            )
            with patch.dict(os.environ, {"APP_WORKSPACE_PATH": workspace}, clear=True), patch("os.getcwd", return_value=other_directory):
                importlib.reload(config)
                settings = config.llm_settings()

        self.assertEqual(settings.api_key, "workspace-key")
        self.assertEqual(settings.model, "workspace-model")


if __name__ == "__main__":
    unittest.main()

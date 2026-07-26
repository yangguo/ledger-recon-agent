import unittest
from pathlib import Path


class KaggleCliRunnerTests(unittest.TestCase):
    def test_build_commands_pushes_p100_kernel_and_checks_status(self):
        from scripts.kaggle_qwen_api_cli import build_commands

        self.assertEqual(
            build_commands(Path("/tmp/kaggle"), "vyang/qwen-api", "NvidiaTeslaP100"),
            [["kaggle", "kernels", "push", "-p", "/tmp/kaggle", "--accelerator", "NvidiaTeslaP100"], ["kaggle", "kernels", "status", "vyang/qwen-api"]],
        )

    def test_metadata_is_private_gpu_internet_and_secret_free(self):
        from scripts.kaggle_qwen_api_cli import build_metadata

        metadata = build_metadata("vyang/qwen-api")

        self.assertTrue(metadata["is_private"])
        self.assertTrue(metadata["enable_gpu"])
        self.assertTrue(metadata["enable_internet"])
        self.assertEqual(metadata["code_file"], "api.ipynb")
        self.assertNotIn("NGROK_AUTHTOKEN", str(metadata))

    def test_prepared_notebook_embeds_launcher_source(self):
        from scripts.kaggle_qwen_api_cli import prepare
        import json
        import tempfile

        with tempfile.TemporaryDirectory() as directory:
            prepare(Path(directory), "yf2000/qwen36-27b-t4x2-api")
            notebook = json.loads((Path(directory) / "api.ipynb").read_text())
        self.assertIn("exec(", "".join(notebook["cells"][0]["source"]))


if __name__ == "__main__":
    unittest.main()

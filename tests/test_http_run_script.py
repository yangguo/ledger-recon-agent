import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


class HttpRunScriptTests(unittest.TestCase):
    def test_exports_repository_root_as_workspace_path(self):
        source_repository = Path(__file__).resolve().parents[1]

        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_path = Path(temporary_directory)
            repository = temporary_path / "project"
            script = repository / "scripts" / "http_run.sh"
            script.parent.mkdir(parents=True)
            shutil.copy2(source_repository / "scripts" / "http_run.sh", script)
            fake_bin = temporary_path / "bin"
            fake_bin.mkdir()
            result_path = temporary_path / "result.txt"
            fake_python = fake_bin / "python"
            fake_python.write_text(
                "#!/bin/sh\nprintf '%s\\n' \"$APP_WORKSPACE_PATH\" > \"$RESULT_PATH\"\n",
                encoding="utf-8",
            )
            fake_python.chmod(0o755)

            environment = os.environ | {
                "PATH": f"{fake_bin}:{os.environ['PATH']}",
                "RESULT_PATH": str(result_path),
            }
            environment.pop("APP_WORKSPACE_PATH", None)
            subprocess.run(
                ["bash", str(script), "-p", "8001"],
                cwd=temporary_path,
                env=environment,
                check=True,
            )

            self.assertEqual(result_path.read_text(encoding="utf-8").strip(), str(repository))


if __name__ == "__main__":
    unittest.main()

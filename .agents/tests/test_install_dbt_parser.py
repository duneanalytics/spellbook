"""Exercise the install command against real truncated and corrupt HTTP bodies."""

import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile
import threading
import unittest
import venv
import zipfile

import packaging
from packaging.tags import sys_tags


class ParserInstallTest(unittest.TestCase):
    def test_verified_install_and_fail_closed_downloads(self):
        for case in ("retry", "truncated", "bad-wheel", "bad-sdist"):
            with self.subTest(case=case), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                (root / ".agents").mkdir()
                helper = root / ".agents/install-dbt-parser.py"
                shutil.copyfile(Path(__file__).resolve().parents[1] / helper.name, helper)
                environment = root / "venv"
                venv.EnvBuilder(with_pip=False).create(environment)
                python = environment / "bin/python"
                site = Path(subprocess.check_output(
                    [str(python), "-c", "import sysconfig; print(sysconfig.get_path('purelib'))"],
                    text=True,
                ).strip())
                shutil.copytree(Path(packaging.__file__).parent, site / "packaging")

                platform = next(sys_tags()).platform
                filename = f"dbt_core_experimental_parser-2.0.1-py3-none-{platform}.whl"
                buffer = io.BytesIO()
                dist = "dbt_core_experimental_parser-2.0.1"
                with zipfile.ZipFile(buffer, "w") as archive:
                    archive.writestr(f"{dist}.dist-info/METADATA", "Metadata-Version: 2.1\nName: dbt-core-experimental-parser\nVersion: 2.0.1\n")
                    archive.writestr(f"{dist}.dist-info/WHEEL", f"Wheel-Version: 1.0\nRoot-Is-Purelib: false\nTag: py3-none-{platform}\n")
                    archive.writestr(f"{dist}.dist-info/RECORD", "")
                    archive.writestr("verified_parser_fixture.py", "VALUE = 'verified fixture'\n")
                wheel = buffer.getvalue()
                payloads = {}
                requests = []

                class Handler(BaseHTTPRequestHandler):
                    def do_GET(self):
                        requests.append(self.path)
                        data = payloads[self.path]
                        self.send_response(200)
                        self.send_header("Content-Length", str(len(data)))
                        self.end_headers()
                        if self.path == "/" + filename:
                            if case == "truncated" or (case == "retry" and requests.count(self.path) == 1):
                                data = data[:len(data) // 2]
                        self.wfile.write(data)

                    def log_message(self, *args):
                        pass

                server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
                url = f"http://127.0.0.1:{server.server_port}"
                manifest = json.dumps({
                    "name": "dbt-core-experimental-parser", "version": "2.0.1",
                    "base_url": url, "wheels": {platform: {
                        "filename": filename, "sha256": hashlib.sha256(wheel).hexdigest(),
                    }},
                }).encode()
                buffer = io.BytesIO()
                with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
                    member = tarfile.TarInfo("parser/_dbt_sa_build/assets.json")
                    member.size = len(manifest)
                    archive.addfile(member, io.BytesIO(manifest))
                sdist = buffer.getvalue()
                payloads.update({"/parser.tar.gz": sdist, "/" + filename: wheel})
                if case == "bad-wheel":
                    # Still installable: uv cannot substitute for the hash gate.
                    buffer = io.BytesIO(wheel)
                    with zipfile.ZipFile(buffer, "a") as archive:
                        archive.writestr("untrusted_fixture.txt", "tampered")
                    payloads["/" + filename] = buffer.getvalue()
                elif case == "bad-sdist":
                    # Still a valid manifest: only the locked hash rejects it.
                    buffer = io.BytesIO()
                    with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
                        member.size = len(manifest) + 1
                        archive.addfile(member, io.BytesIO(manifest + b"\n"))
                    payloads["/parser.tar.gz"] = buffer.getvalue()
                (root / "uv.lock").write_text(
                    '[[package]]\nname = "dbt-core-experimental-parser"\nversion = "2.0.1"\n'
                    f'sdist = {{ url = "{url}/parser.tar.gz", hash = "sha256:{hashlib.sha256(sdist).hexdigest()}" }}\n'
                )
                thread = threading.Thread(target=server.serve_forever, daemon=True)
                thread.start()
                try:
                    result = subprocess.run([str(python), str(helper)], capture_output=True, text=True)
                finally:
                    server.shutdown()
                    server.server_close()
                    thread.join()
                probe = subprocess.run(
                    [str(python), "-c", "import verified_parser_fixture as p; print(p.VALUE)"],
                    capture_output=True, text=True,
                )
                if case == "retry":
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertEqual(probe.stdout.strip(), "verified fixture")
                else:
                    self.assertNotEqual(result.returncode, 0)
                    self.assertNotEqual(probe.returncode, 0, "Unverified wheel was installed")


if __name__ == "__main__":
    unittest.main()

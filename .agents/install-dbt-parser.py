"""Install the locked parser asset without its non-retrying urllib download."""

import hashlib
import importlib.metadata
import json
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import tomllib

from packaging.tags import sys_tags


def download(url, destination, expected_hash):
    subprocess.run(
        [
            "curl", "--fail", "--location", "--silent", "--show-error",
            "--connect-timeout", "10", "--max-time", "90",
            "--retry", "2", "--retry-all-errors", "--retry-delay", "1",
            "--output", str(destination), url,
        ],
        check=True,
    )
    actual = hashlib.sha256(destination.read_bytes()).hexdigest()
    if actual != expected_hash:
        raise RuntimeError(f"SHA256 mismatch for {destination.name}: {actual}")


def main():
    root = Path(__file__).resolve().parent.parent
    lock = tomllib.loads((root / "uv.lock").read_text())
    name = "dbt-core-experimental-parser"
    packages = [package for package in lock["package"] if package["name"] == name]
    if not packages:
        return
    package, = packages
    version = package["version"]
    try:
        if importlib.metadata.version(name) == version:
            print(f"Locked {name}=={version} already installed")
            return
    except importlib.metadata.PackageNotFoundError:
        pass

    with tempfile.TemporaryDirectory(prefix="spellbook-parser-") as directory:
        directory = Path(directory)
        sdist = directory / "parser.tar.gz"
        download(package["sdist"]["url"], sdist, package["sdist"]["hash"].removeprefix("sha256:"))
        with tarfile.open(sdist) as archive:
            manifest_path, = [
                member for member in archive.getnames()
                if member.endswith("/_dbt_sa_build/assets.json")
            ]
            manifest = json.load(archive.extractfile(manifest_path))
        if manifest["name"] != name or manifest["version"] != version:
            raise RuntimeError("Parser manifest does not match uv.lock")
        entry = next(
            manifest["wheels"][tag.platform]
            for tag in sys_tags() if tag.platform in manifest["wheels"]
        )
        wheel = directory / entry["filename"]
        download(f"{manifest['base_url'].rstrip('/')}/{wheel.name}", wheel, entry["sha256"])
        # Install by name from a local wheel directory, not a direct-URL source
        # override. The caller must finish with a full `uv sync --locked`.
        subprocess.run(
            [
                "uv", "pip", "install", "--python", sys.executable,
                "--no-deps", "--no-index", "--find-links", str(directory),
                f"{name}=={version}",
            ],
            check=True,
        )


if __name__ == "__main__":
    main()

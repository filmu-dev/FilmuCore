"""Project-local protobuf code generation for the FilmuVFS catalog contract."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

_PROTO_RELATIVE_PATH = Path("filmuvfs/catalog/v1/catalog.proto")
_GENERATED_PACKAGE_ROOT = Path("filmuvfs")


def _workspace_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _ensure_generated_package_root(workspace_root: Path) -> None:
    for relative in (
        _GENERATED_PACKAGE_ROOT,
        _GENERATED_PACKAGE_ROOT / "catalog",
        _GENERATED_PACKAGE_ROOT / "catalog" / "v1",
    ):
        package_dir = workspace_root / relative
        package_dir.mkdir(parents=True, exist_ok=True)
        (package_dir / "__init__.py").touch(exist_ok=True)


def generate_catalog_proto() -> None:
    """Generate Python protobuf and gRPC bindings for the FilmuVFS catalog contract."""

    workspace_root = _workspace_root()
    _ensure_generated_package_root(workspace_root)
    subprocess.run(
        [
            sys.executable,
            "-m",
            "grpc_tools.protoc",
            f"-I{workspace_root / 'proto'}",
            f"--python_out={workspace_root}",
            f"--pyi_out={workspace_root}",
            f"--grpc_python_out={workspace_root}",
            _PROTO_RELATIVE_PATH.as_posix(),
        ],
        check=True,
        cwd=workspace_root,
    )


def main() -> int:
    """CLI entrypoint used by [`filmu-py-generate-protos`](../pyproject.toml)."""

    generate_catalog_proto()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

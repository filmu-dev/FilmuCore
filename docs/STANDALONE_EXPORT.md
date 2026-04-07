# Standalone Export Notes

## Purpose

Document what this exported `filmu-python` folder is, what was copied into it, and what was intentionally left out.

## Included

- `filmu_py/`
- `tests/`
- `docs/`
- `.env.example`
- `Dockerfile.local`
- `pyproject.toml`
- `uv.lock`
- `package.json`
- `pyrightconfig.json`
- `README.md`

## Excluded local artifacts

- `.venv/`
- `.mypy_cache/`
- `.pytest_cache/`
- `.ruff_cache/`

## Not needed from the original TypeScript monorepo root

The standalone Python backend does **not** require the TypeScript-monorepo root assets such as:

- `.git`, `.github`, `.husky`, `.vscode`
- `.prettierignore`, `.prettierrc.json`, `apollo.config.json`
- workspace Docker / Turbo / pnpm files
- `apps/`, `packages/`, `turbo/`, `node_modules/`

Those belong to the full `Triven_backend - ts` monorepo, not this backend-only export.

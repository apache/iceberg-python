# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import annotations

import os
import shutil
import sys
from collections.abc import Iterator
from enum import StrEnum
from pathlib import Path

import nox
from identify import identify
from pathspec import GitIgnoreSpec

nox.options.default_venv_backend = "uv"

# Sessions without `default=False` are the CI gate, so `just check` runs exactly those.
nox.options.stop_on_first_error = True

PYPROJECT = nox.project.load_toml("pyproject.toml")
PYTHON_VERSIONS = nox.project.python_versions(PYPROJECT, max_version="3.14")

SOURCE = ("pyiceberg", "tests", "dev", "mkdocs", "noxfile.py", "setup.py")
NOTEBOOKS = ("notebooks", "tests/notebooks")
SKIP_DIRS = (".git", ".nox", ".venv", "build", "dist", "vendor")
COMPOSE = ("docker", "compose", "-f", "dev/docker-compose-integration.yml")

IGNORE = GitIgnoreSpec.from_lines(Path(".gitignore").read_text(encoding="utf-8").splitlines())


class Env(StrEnum):
    """Environment variables that change how the sessions run."""

    CI = "CI"
    COVERAGE = "COVERAGE"
    COVERAGE_FAIL_UNDER = "COVERAGE_FAIL_UNDER"
    GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT = "GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT"
    KEEP_COMPOSE = "KEEP_COMPOSE"
    UV_PROJECT_ENVIRONMENT = "UV_PROJECT_ENVIRONMENT"


# The environment is fixed for the whole run, so the switches are read once at import
CI = Env.CI in os.environ
COVERAGE = os.environ.get(Env.COVERAGE) == "1"
COVERAGE_FAIL_UNDER = os.environ.get(Env.COVERAGE_FAIL_UNDER, "85")
KEEP_COMPOSE = os.environ.get(Env.KEEP_COMPOSE) == "1"
COVERAGE_ARGS = ["-m", "coverage", "run", "--parallel-mode", "--source=pyiceberg"] if COVERAGE else []


def _ignored(path: Path) -> bool:
    """Return whether .gitignore excludes the path."""
    relative = path.as_posix()
    return IGNORE.match_file(relative + "/" if path.is_dir() else relative)


def _tracked(*suffixes: str) -> Iterator[Path]:
    """Yield project files with the given suffixes, skipping ignored and vendored paths.

    .gitignore decides which build artifacts are left out, so a tool that starts writing a
    new cache directory needs no change here. Files that are not ignored are included
    whether or not they are committed yet, so a new file is checked before you stage it.
    """
    for root, dirnames, filenames in os.walk("."):
        base = Path(root)
        dirnames[:] = sorted(name for name in dirnames if name not in SKIP_DIRS and not _ignored(base / name))
        for name in sorted(filenames):
            path = base / name
            if (not suffixes or path.suffix in suffixes) and not _ignored(path):
                yield path


def _tracked_text() -> list[str]:
    """Return tracked text files, matching what the pre-commit `types: [text]` filter gave."""
    texts = []
    for path in _tracked():
        name = str(path)
        if path.is_file() and "text" in identify.tags_from_path(name):
            texts.append(name)
    return texts


def _unsupported_extras() -> list[str]:
    """Return flags for extras this platform cannot build.

    `hive-kerberos` pulls the `kerberos` package, which publishes no Windows wheels, so the
    runner would have to build it against MIT Kerberos headers it does not have. The previous
    workflow excluded the same extra.
    """
    return ["--no-extra", "hive-kerberos"] if sys.platform == "win32" else []


def _check_only(session: nox.Session) -> bool:
    """Return whether this run reports problems instead of fixing them."""
    return CI or "--check" in session.posargs


def _python_arg(session: nox.Session) -> list[str]:
    """Return the interpreter that nox was asked for, when it was asked for one.

    `nox -s install --force-python 3.12` sets `session.python` without creating a virtualenv,
    which is how a session that shells out to `uv` learns which interpreter to sync.
    """
    return ["--python", session.python] if isinstance(session.python, str) else []


def _posargs(session: nox.Session) -> list[str]:
    """Return the arguments meant for the tool, without the flags this file consumes."""
    return [argument for argument in session.posargs if argument != "--check"]


def _pytest(session: nox.Session, *args: str, env: dict[str, str] | None = None) -> None:
    """Run pytest in the project environment, collecting coverage when it is enabled.

    The extras are synced here because pytest runs in the project environment, not in the
    session environment, so these sessions do not depend on a prior `just install`.
    """
    session.run(
        "uv",
        "run",
        "--all-extras",
        *_unsupported_extras(),
        *_python_arg(session),
        "python",
        *COVERAGE_ARGS,
        "-m",
        "pytest",
        *args,
        *_posargs(session),
        external=True,
        env=env,
    )


def _start_services(session: nox.Session) -> None:
    """Recreate the integration services from a clean state."""
    session.run(*COMPOSE, "kill", external=True)
    session.run(*COMPOSE, "rm", "-f", external=True)
    session.run(*COMPOSE, "up", "-d", "--build", "--wait", external=True)


def _stop_services(session: nox.Session) -> None:
    """Tear the services down unless KEEP_COMPOSE=1 asks to keep them."""
    if KEEP_COMPOSE:
        session.log("Keeping containers running for debugging (KEEP_COMPOSE=1)")
        return
    session.run(*COMPOSE, "down", "-v", "--remove-orphans", "--timeout", "0", external=True)


# ----------------------------------------------------------------- setup
@nox.session(tags=["setup"], default=False, venv_backend="none")
def install(session: nox.Session) -> None:
    """Sync all extras and rebuild the Cython extensions when they are missing."""
    session.run("uv", "sync", "--all-extras", *_python_arg(session), *_unsupported_extras(), *session.posargs)
    compiled = list(Path("pyiceberg").rglob("*.so")) + list(Path("pyiceberg").rglob("*.pyd"))
    if not compiled:
        session.log("Cython extensions not found, reinstalling pyiceberg...")
        session.run(
            "uv",
            "sync",
            "--all-extras",
            "--reinstall-package",
            "pyiceberg",
            *_python_arg(session),
            *_unsupported_extras(),
            *session.posargs,
        )


# ----------------------------------------------------------------- quality
@nox.session(tags=["check"], allow_parallel=True)
def format(session: nox.Session) -> None:
    """Format Python and notebook sources with ruff."""
    session.install("--group", "lint")
    session.run("ruff", "format", *(["--check"] if _check_only(session) else []), *SOURCE, *NOTEBOOKS)


@nox.session(tags=["check"], allow_parallel=True)
def lint(session: nox.Session) -> None:
    """Lint Python and notebook sources with ruff, fixing what it can.

    E402 is ignored in notebooks: their imports legitimately follow the markdown and setup cells.
    """
    session.install("--group", "lint")
    fix = [] if _check_only(session) else ["--fix"]
    session.run("ruff", "check", "--no-fix" if not fix else "--fix", "--exit-non-zero-on-fix", *SOURCE)
    session.run("nbqa", "ruff", *fix, "--exit-non-zero-on-fix", "--ignore=E402", *NOTEBOOKS)


@nox.session(tags=["check"], allow_parallel=True)
def hygiene(session: nox.Session) -> None:
    """Check whitespace, final newlines, YAML and Python syntax with pre-commit-hooks.

    The two fixers rewrite files and fail the session when they change something, which is
    the same behaviour the hooks had. `check-yaml --unsafe` parses instead of loading, so the
    `!!python/name:` tag in mkdocs/mkdocs.yml is accepted.
    """
    session.install("--group", "lint")
    text = _tracked_text()
    session.run("trailing-whitespace-fixer", *text, silent=True, log=False)
    session.run("end-of-file-fixer", *text, silent=True, log=False)
    session.run("check-yaml", "--unsafe", *[str(path) for path in _tracked(".yaml", ".yml")], log=False)
    session.run("check-ast", *[str(path) for path in _tracked(".py", ".pyi")], log=False)


@nox.session(name="typecheck-mypy", tags=["check"], allow_parallel=True)
def typecheck_mypy(session: nox.Session) -> None:
    """Type-check every tracked Python file with mypy.

    Only mypy and the type stubs are installed; the project's runtime dependencies stay
    out of the environment, which keeps the gate in line with the previous pre-commit hook.
    Missing third-party imports are ignored by configuration, so the tests and dev scripts
    can be checked without their dependencies present.
    """
    session.install("--group", "types")
    files = [str(path) for path in _tracked(".py", ".pyi")]
    session.run("mypy", "--config-file=pyproject.toml", "--scripts-are-modules", *files, *_posargs(session))


@nox.session(name="typecheck-ty", tags=["experimental"], allow_parallel=True, default=False)
def typecheck_ty(session: nox.Session) -> None:
    """Type-check with ty. Experimental: not a gate yet."""
    session.install("--group", "ty", "--group", "noxfile")
    session.run("ty", "check", *_posargs(session))


@nox.session(tags=["check"], allow_parallel=True)
def codespell(session: nox.Session) -> None:
    """Check spelling in tracked text files, so build artifacts are skipped."""
    session.install("--group", "lint")
    session.run("codespell", "--", *_tracked_text(), silent=True, log=False)


@nox.session(tags=["check"], allow_parallel=True)
def markdownlint(session: nox.Session) -> None:
    """Lint Markdown with rumdl, which reads the existing .markdownlint.yaml.

    MD057 is disabled: it checks that relative links resolve, but the `reference/` pages are
    generated by mkdocstrings at build time and do not exist in the source tree.
    """
    session.install("--group", "lint")
    session.run(
        "rumdl",
        "check",
        *([] if _check_only(session) else ["--fix"]),
        "--disable",
        "MD057",
        *[str(path) for path in _tracked(".md")],
    )


# ----------------------------------------------------------------- testing
@nox.session(python=PYTHON_VERSIONS, tags=["test"], allow_parallel=True, default=False)
def tests(session: nox.Session) -> None:
    """Run the unit test suite on every supported Python version.

    The environment is synced from uv.lock so each version runs the same pins. The project is
    installed into the session environment, which builds the Cython extensions for that
    interpreter (the compiled modules are ABI-tagged, so each version needs its own).
    """
    # pytest runs with the repository on sys.path, so the in-place Cython build must match
    # this interpreter. Rebuild only when its own extension file is missing from the tree.
    suffix = (
        session.run("python", "-c", "import sysconfig; print(sysconfig.get_config_var('EXT_SUFFIX'))", silent=True) or ""
    ).strip()
    compiled = list(Path("pyiceberg").rglob(f"decoder_fast{suffix}"))
    sync = ["uv", "sync", "--locked", "--all-extras", *_unsupported_extras()]
    if not compiled:
        sync += ["--reinstall-package", "pyiceberg"]
    session.run_install(*sync, env={Env.UV_PROJECT_ENVIRONMENT: str(session.env_dir)})
    extra = [] if session.posargs else ["-v", "-x"]
    session.run(
        "python", *COVERAGE_ARGS, "-m", "pytest", "tests/", "-m", "unit", "--ignore=tests/integration", *extra, *_posargs(session)
    )


@nox.session(tags=["test"], default=False)
def coverage(session: nox.Session) -> None:
    """Combine coverage data and enforce the coverage threshold."""
    session.install("--group", "coverage")
    fail_under = COVERAGE_FAIL_UNDER
    session.run("coverage", "combine")
    session.run("coverage", "report", "-m", f"--fail-under={fail_under}")
    session.run("coverage", "html")
    session.run("coverage", "xml")


@nox.session(tags=["test"], default=False, venv_backend="none")
def integration(session: nox.Session) -> None:
    """Run the integration suite against local Docker services."""
    try:
        _start_services(session)
        session.run("uv", "run", "python", "dev/provision.py", external=True)
        _pytest(session, "tests/", "-m", "integration", "-v", "-x")
    finally:
        _stop_services(session)


@nox.session(name="integration-rebuild", tags=["test"], default=False, venv_backend="none")
def integration_rebuild(session: nox.Session) -> None:
    """Rebuild the integration services from scratch, without a cache."""
    session.run(*COMPOSE, "kill", external=True)
    session.run(*COMPOSE, "rm", "-f", external=True)
    session.run(*COMPOSE, "build", "--no-cache", external=True)


@nox.session(tags=["test"], default=False, venv_backend="none")
def s3(session: nox.Session) -> None:
    """Run the S3 suite against a local MinIO."""
    session.run("sh", "./dev/run-s3.sh", external=True)
    _pytest(session, "tests/", "-m", "s3", "-v", "-x")


@nox.session(tags=["test"], default=False, venv_backend="none")
def adls(session: nox.Session) -> None:
    """Run the ADLS suite against a local Azurite."""
    session.run("sh", "./dev/run-azurite.sh", external=True)
    _pytest(session, "tests/", "-m", "adls", "-v", "-x")


@nox.session(tags=["test"], default=False, venv_backend="none")
def gcs(session: nox.Session) -> None:
    """Run the GCS suite against a local fake-gcs-server."""
    session.run("sh", "./dev/run-gcs-server.sh", external=True)
    # gcsfs 2026.6.0 sends HNS/Zonal bucket detection to endpoint_url, which fake-gcs-server
    # does not support; leaving it on makes the GCS sanity tests take close to an hour.
    _pytest(session, "tests/", "-m", "gcs", "-v", "-x", env={Env.GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT: "false"})


@nox.session(name="test-notebook", tags=["test"], default=False, venv_backend="none")
def test_notebook(session: nox.Session) -> None:
    """Run the notebook tests with papermill."""
    _pytest(
        session,
        "tests/notebooks/test_pyiceberg_example.py",
        "tests/notebooks/test_spark_integration_example.py",
        "-m",
        "notebook",
        "-v",
        "-x",
    )


# ----------------------------------------------------------------- docs
@nox.session(name="docs-build", default=False, venv_backend="none")
def docs_build(session: nox.Session) -> None:
    """Build the static documentation site, failing on warnings."""
    session.run(
        "uv",
        "run",
        *_python_arg(session),
        "--no-default-groups",
        "--group",
        "docs",
        "mkdocs",
        "build",
        "-f",
        "mkdocs/mkdocs.yml",
        "--strict",
        external=True,
    )


@nox.session(name="docs-serve", default=False, venv_backend="none")
def docs_serve(session: nox.Session) -> None:
    """Serve the documentation with hot reload."""
    session.run(
        "uv",
        "run",
        *_python_arg(session),
        "--no-default-groups",
        "--group",
        "docs",
        "mkdocs",
        "serve",
        "-f",
        "mkdocs/mkdocs.yml",
        "--livereload",
        external=True,
    )


# ----------------------------------------------------------------- experimentation
@nox.session(default=False, venv_backend="none")
def notebook(session: nox.Session) -> None:
    """Start JupyterLab for experimentation."""
    session.run(
        "uv",
        "run",
        *_python_arg(session),
        "--all-extras",
        "--group",
        "notebook",
        "jupyter",
        "lab",
        "--notebook-dir=notebooks",
        external=True,
    )


@nox.session(name="notebook-infra", default=False, venv_backend="none")
def notebook_infra(session: nox.Session) -> None:
    """Start JupyterLab with the integration services running.

    The services stop when JupyterLab exits, unless KEEP_COMPOSE=1 asks to keep them.
    """
    try:
        _start_services(session)
        session.run(
            "uv",
            "run",
            *_python_arg(session),
            "--all-extras",
            "--group",
            "notebook",
            "jupyter",
            "lab",
            "--notebook-dir=notebooks",
            external=True,
        )
    finally:
        _stop_services(session)


# ----------------------------------------------------------------- maintenance
@nox.session(default=False, venv_backend="none")
def clean(session: nox.Session) -> None:
    """Remove build artifacts, caches and session environments."""
    root = Path.cwd()
    for directory in ("build", "dist", ".venv", ".nox", "htmlcov"):
        shutil.rmtree(root / directory, ignore_errors=True)
    for path in [*root.glob("*.egg-info"), *root.glob("**/__pycache__"), *root.glob("**/.ipynb_checkpoints")]:
        shutil.rmtree(path, ignore_errors=True)
    for pattern in ("**/*.so", "**/*.pyd", "**/*.pyc", "**/*.pyo", ".coverage", ".coverage.*", "coverage.xml"):
        for path in root.glob(pattern):
            path.unlink(missing_ok=True)

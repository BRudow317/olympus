#!/usr/bin/env python3.11
"""/stage/rundeck-scripts/charon/charon.py

usage:
    python charon.py [flags] --exec <child command with args>

python ./charon.py `
    --exec ./src/app.py `
    --source-system salesforce `
    --source-environment TRAIL `
    --source-namespace TRAIL `
    --target-system oracle `
    --target-environment DWH `
    --target-namespace DWH `
    --action upsert `
    --tables Contact
"""
from __future__ import annotations

import sys
import subprocess
import threading
import os
import argparse
import re
import logging
import hashlib
import shutil
from pathlib import Path
from datetime import datetime
from typing import IO, TextIO

# Program identity is a hardcoded constant, not config: it is not sourced from
# stage/rundeck-scripts/.env nor from the ambient environment.
PROGRAM_NAME = "charon"

# === charon-bootstrap: TUNABLES (additive) =================================
# A repo opts in to charon's offline-venv management by putting a [tool.charon]
# table in its pyproject.toml. Without that table, every function below is a
# silent no-op and charon behaves exactly as it did before this block existed,
# so generic jobs that pull this same file are unaffected.
#
# The OS is the deterministic oracle for "can I reach PyPI":
#   win32  -> online  -> may download/build the wheelhouse itself
#   else   -> offline -> may only consume vendored wheels, else fail loudly
#
# The OL8 deploy target is fixed; the Windows target follows whatever
# interpreter launched charon.
_MIN_PYTHON = (3, 11)
_PYPROJECT = "pyproject.toml"
_LOCKFILE = "requirements.lock"
_WHEELHOUSE_DIR = "vendor/wheels"
_DEPS_STAMP = ".charon-deps-hash"
_BOOTSTRAP_EXTRAS = ("pip", "setuptools", "wheel")
_LINUX_TAG = "linux_py311"
_LINUX_PYVER = "3.11"
_LINUX_PLATFORMS = (
    "manylinux_2_28_x86_64",   # required by pyarrow / cryptography; OL8 = glibc 2.28
    "manylinux2014_x86_64",
    "manylinux_2_17_x86_64",
)
# ===========================================================================

_VAR = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}|\$([A-Za-z_][A-Za-z0-9_]*)|\{([A-Za-z_][A-Za-z0-9_]*)\}")
_LOG_FORMAT = '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
_PYTHON_BASENAMES = {"python", "python3", "python3.11"}

def prepare_child(
    args, 
    config_vars: dict[str, str] | None = None,
) -> tuple[list[str], dict[str, str], str]:
    
    config_vars = config_vars or {}
    is_python = False
    venv = getattr(args, "venv", "")
    script = next((Path(t).resolve() for t in args.exec if Path(t).is_file()), None)
    
    cwd = str(script.parent.parent) if script else os.getcwd()
    pkg_root = cwd
    
    if script and script.suffix == ".py":
        root = script.parent
        while (root / "__init__.py").exists() and root.parent != root:
            root = root.parent
        pkg_root = str(root)
        
    # child venv instantiation
    python = sys.executable
    env = {**os.environ, **config_vars}
    env["PYTHONUNBUFFERED"] = "1"
    
    if venv:
        bin_dir = Path(venv) / ("Scripts" if sys.platform == "win32" else "bin")
        venv_python = bin_dir / ("python.exe" if sys.platform == "win32" else "python")
        
        if not venv_python.exists():
            raise FileNotFoundError(f"venv python not found: {venv_python}")
            
        python = str(venv_python)
        env["VIRTUAL_ENV"] = str(Path(venv).resolve())
        env.pop("PYTHONHOME", None)
        env["PATH"] = f"{bin_dir}{os.pathsep}{env.get('PATH', '')}"
        
    # Add to PYTHONPATH local lib directory instead of venv, if not in venv and ./lib exists
    lib_root = Path(pkg_root) / "lib"
    existing = [p for p in env.get("PYTHONPATH", "").split(str(os.pathsep)) if p]
    candidates = [str(lib_root.resolve()), str(pkg_root), str(cwd)]
    new_paths = [p for p in dict.fromkeys(candidates) if os.path.isdir(p) and p not in existing]
    env["PYTHONPATH"] = os.pathsep.join(new_paths + existing)
    
    # prepend pkg_root and cwd to PYTHONPATH
    existing = [p for p in env.get("PYTHONPATH", "").split(str(os.pathsep)) if p]
    new_paths = [p for p in dict.fromkeys([pkg_root, cwd]) if p not in existing]
    env["PYTHONPATH"] = os.pathsep.join(new_paths + existing)
    
    # swap out the system python for the venv python
    cmd = [str(Path(t).resolve()) if Path(t).is_file() else t for t in args.exec]
    
    if os.path.basename(cmd[0]).lower() in _PYTHON_BASENAMES:
        is_python = True
        cmd[0] = python
    elif Path(cmd[0]).suffix == ".py" and os.path.isfile(cmd[0]):
        is_python = True
        cmd = [python] + cmd
        
    log_level = "INFO"
    if getattr(args, "verbose", False):
        log_level = "DEBUG"
        
    child_logging_bootstrap = (
        "import sys,os,logging,runpy;"
        "logging.basicConfig("
        f"level='{log_level}',"
        f"format={_LOG_FORMAT!r});"
        "sys.argv=sys.argv[1:];"
        "runpy.run_path(sys.argv[0],run_name='__main__')"
    )
    
    if is_python:
        cmd = [cmd[0], "-c", child_logging_bootstrap] + cmd[1:]
        
    return cmd, env, cwd

# === charon-bootstrap: IMPLEMENTATION (additive) ===========================
# Pyproject-driven offline venv management. Read top to bottom:
#   _load_project  - is this repo charon-managed? what are its deps?
#   _deps_hash     - fingerprint of the dependency set, for drift detection
#   build_wheelhouse / _download_into / _write_lock - the Windows-only vendoring
#   ensure_wheelhouse - the OS fork: Windows self-heals, Linux consumes-or-stops
#   ensure_venv    - build/heal the venv from the vendored wheels (both OSes)

def _current_pyver() -> str:
    return f"{sys.version_info.major}.{sys.version_info.minor}"

def _platform_tag() -> str:
    osname = "win" if sys.platform == "win32" else "linux"
    return f"{osname}_py{sys.version_info.major}{sys.version_info.minor}"

def _venv_python(venv_path: Path) -> Path:
    if sys.platform == "win32":
        return venv_path / "Scripts" / "python.exe"
    return venv_path / "bin" / "python"

def _run_checked(cmd: list[str], logger: logging.Logger,
                 env: dict[str, str] | None = None) -> None:
    logger.debug("bootstrap run: %s", " ".join(str(c) for c in cmd))
    proc = subprocess.run(
        [str(c) for c in cmd],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env,
    )
    if proc.returncode != 0:
        if proc.stdout:
            logger.error(proc.stdout.rstrip())
        raise RuntimeError(
            f"command failed (exit {proc.returncode}): {' '.join(str(c) for c in cmd)}"
        )

def _load_project(repo_root: Path) -> tuple[bool, list[str]]:
    """Return (is_charon_managed, dependencies) from pyproject.toml.
    Managed == a [tool.charon] table exists. Deps == [project].dependencies."""
    pp = repo_root / _PYPROJECT
    if not pp.is_file():
        return False, []
    import tomllib  # stdlib on 3.11+ (server) and 3.13 (dev)
    try:
        data = tomllib.loads(pp.read_text(encoding="utf-8"))
    except tomllib.TOMLDecodeError as e:
        raise RuntimeError(f"{_PYPROJECT} is not valid TOML: {e}") from e
    managed = isinstance(data.get("tool", {}).get("charon"), dict)
    deps = data.get("project", {}).get("dependencies", []) or []
    return managed, [str(d).strip() for d in deps if str(d).strip()]

def _deps_hash(deps: list[str]) -> str:
    return hashlib.sha256("\n".join(sorted(deps)).encode("utf-8")).hexdigest()

def _wheel_name_version(filename: str) -> tuple[str, str]:
    stem = filename[:-4] if filename.lower().endswith(".whl") else filename
    parts = stem.split("-")
    return parts[0].replace("_", "-"), parts[1]

def _write_lock(app_wheels: list[str], lock_path: Path) -> None:
    pins: dict[str, str] = {}
    for whl in app_wheels:
        name, version = _wheel_name_version(whl)
        pins[name] = version
    if not pins:
        raise RuntimeError("no application wheels resolved to pin")
    lines = [
        "# Auto-generated by charon from pyproject.toml. Do not edit by hand.",
        "# Pins are shared across linux + windows; pip picks the matching wheel.",
    ]
    lines += [f"{n}=={pins[n]}" for n in sorted(pins, key=str.lower)]
    lock_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

def _download_into(target: Path, pyver: str, platforms: tuple[str, ...],
                   deps: list[str], logger: logging.Logger) -> list[str]:
    target.mkdir(parents=True, exist_ok=True)
    cmd = [sys.executable, "-m", "pip", "download", *deps,
           "--python-version", pyver, "--implementation", "cp",
           "--only-binary=:all:",            # hard stop: refuse sdists, no source builds
           "-d", str(target)]
    for plat in platforms:
        cmd += ["--platform", plat]
    _run_checked(cmd, logger)
    # App closure snapshot BEFORE adding bootstrap extras, so the lock pins only real deps.
    app_wheels = sorted(p.name for p in target.glob("*.whl"))
    _run_checked([sys.executable, "-m", "pip", "download", *_BOOTSTRAP_EXTRAS,
                  "--only-binary=:all:", "-d", str(target)], logger)
    return app_wheels

def build_wheelhouse(repo_root: Path, deps: list[str], logger: logging.Logger) -> None:
    """Windows-only. Wipe and rebuild both platform wheelhouses from PyPI,
    regenerate requirements.lock, and stamp the dependency hash."""
    if sys.platform != "win32":
        raise RuntimeError(
            "refusing to download wheels off Windows: this host is treated as "
            "offline. Build on a Windows box with PyPI access and redeploy."
        )
    if not deps:
        raise RuntimeError(f"no [project].dependencies in {_PYPROJECT} to build")

    base = repo_root / _WHEELHOUSE_DIR
    if base.exists():
        shutil.rmtree(base)        # clear + reset so the folder matches deps exactly
    base.mkdir(parents=True, exist_ok=True)

    win_tag = _platform_tag()      # follows the current interpreter, e.g. win_py313
    logger.info("vendoring wheels from PyPI: %s + %s", _LINUX_TAG, win_tag)
    linux_app = _download_into(base / _LINUX_TAG, _LINUX_PYVER, _LINUX_PLATFORMS, deps, logger)
    _download_into(base / win_tag, _current_pyver(), ("win_amd64",), deps, logger)

    _write_lock(linux_app, repo_root / _LOCKFILE)
    (base / _DEPS_STAMP).write_text(_deps_hash(deps), encoding="utf-8")
    logger.info("wheelhouse ready (%d deps pinned)", len(linux_app))

def ensure_wheelhouse(repo_root: Path, deps: list[str], logger: logging.Logger) -> None:
    """The OS fork. Windows self-heals a missing/drifted wheelhouse by
    rebuilding from PyPI. Linux consumes what shipped, or stops with a precise
    reason - it cannot fetch wheels."""
    base = repo_root / _WHEELHOUSE_DIR
    stamp = base / _DEPS_STAMP
    want = _deps_hash(deps)
    have = stamp.read_text(encoding="utf-8").strip() if stamp.is_file() else None

    linux_dir = base / _LINUX_TAG
    linux_ok = linux_dir.is_dir() and any(linux_dir.glob("*.whl"))
    # On Windows we also need the wheelhouse for the interpreter we run under.
    if sys.platform == "win32":
        run_dir = base / _platform_tag()
        run_ok = run_dir.is_dir() and any(run_dir.glob("*.whl"))
    else:
        run_ok = True

    if have == want and linux_ok and run_ok and (repo_root / _LOCKFILE).is_file():
        logger.debug("wheelhouse matches pyproject; no vendoring needed")
        return

    if sys.platform == "win32":
        logger.info("wheelhouse missing or out of date for current pyproject; rebuilding")
        build_wheelhouse(repo_root, deps, logger)
        return

    # Offline host: cannot fetch. Distinguish "never built" from "deps drifted".
    if have is None or not linux_ok:
        raise RuntimeError(
            f"no usable wheelhouse in this deployment: {linux_dir}\n"
            f"  The wheels were not built/committed before the zip was made.\n"
            f"  On Windows: run the job once (or `python configs/build_wheelhouse.py`),\n"
            f"  commit pyproject.toml + requirements.lock + {_WHEELHOUSE_DIR}/, and redeploy."
        )
    raise RuntimeError(
        f"{_PYPROJECT} dependencies do not match the vendored wheels.\n"
        f"  Someone changed dependencies without rebuilding on Windows.\n"
        f"  This host is offline and cannot fetch wheels. Rebuild on Windows and redeploy."
    )

def ensure_venv(venv: str, repo_root: Path, logger: logging.Logger) -> None:
    """Build or self-heal the venv from the vendored wheels. Idempotent when
    the venv already matches the lockfile; fail-fast if a needed wheel is absent."""
    manifest = repo_root / _LOCKFILE
    if not manifest.is_file():
        raise RuntimeError(f"{_LOCKFILE} missing at {manifest}; wheelhouse build incomplete")

    venv_path = Path(venv).resolve()
    tag = _platform_tag()
    wheelhouse = repo_root / _WHEELHOUSE_DIR / tag
    if not wheelhouse.is_dir():
        base = repo_root / _WHEELHOUSE_DIR
        present = sorted(p.name for p in base.iterdir() if p.is_dir()) if base.is_dir() else []
        raise RuntimeError(
            f"wheelhouse not found for this platform: {wheelhouse}\n"
            f"  running Python {_current_pyver()} on {sys.platform} needs '{tag}'.\n"
            f"  folders present: {present or 'none'}"
        )

    token = (
        hashlib.sha256(manifest.read_bytes()).hexdigest()
        + ":" + tag + ":"
        + f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    )
    sentinel = venv_path / ".charon_venv.lock"
    vpy = _venv_python(venv_path)

    if vpy.exists() and sentinel.is_file() and sentinel.read_text().strip() == token:
        probe = subprocess.run([str(vpy), "-c", "import sys"],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if probe.returncode == 0:
            logger.debug("venv up to date: %s", venv_path)
            return

    if venv_path.exists():
        if not (venv_path / "pyvenv.cfg").exists() and not vpy.exists():
            raise RuntimeError(f"refusing to rebuild: {venv_path} exists but is not a virtualenv")
        logger.info("rebuilding offline venv: %s", venv_path)
        shutil.rmtree(venv_path)
    else:
        logger.info("building offline venv: %s", venv_path)

    # Two-tier, OS-defensive venv build.
    # Tier 1: a normal venv WITH pip via ensurepip. This is the happy path on
    #         Windows and on any OL8 whose ensurepip is intact.
    made = subprocess.run(
        [sys.executable, "-m", "venv", str(venv_path)],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    have_pip = made.returncode == 0 and subprocess.run(
        [str(vpy), "-m", "pip", "--version"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    ).returncode == 0

    if have_pip:
        # Use the venv's own pip to install the pinned wheels, offline.
        _run_checked([str(vpy), "-m", "pip", "install", "--no-index",
                      "--find-links", str(wheelhouse), "-r", str(manifest)], logger)
    else:
        # Tier 2: ensurepip is stripped (some locked-down builds). Recreate the
        # venv without pip and install using pip imported from the bundled wheel
        # via PYTHONPATH. We invoke `-m pip` (not the wheel's console entry) and
        # we do NOT install pip into the venv, so the Windows "to modify pip,
        # run -m pip" self-modify guard is never triggered. The app does not
        # need pip at run time anyway.
        if venv_path.exists():
            shutil.rmtree(venv_path)
        _run_checked([sys.executable, "-m", "venv", "--without-pip", str(venv_path)], logger)
        pip_wheels = sorted(wheelhouse.glob("pip-*.whl"))
        if not pip_wheels:
            raise RuntimeError(
                f"venv has no pip and no bundled pip wheel in {wheelhouse}; "
                f"rebuild the wheelhouse on Windows"
            )
        boot_env = {**os.environ, "PYTHONPATH": str(pip_wheels[-1])}
        extras = [e for e in _BOOTSTRAP_EXTRAS if e != "pip"]   # setuptools, wheel
        _run_checked([str(vpy), "-m", "pip", "install", "--no-index",
                      "--find-links", str(wheelhouse), *extras, "-r", str(manifest)],
                     logger, env=boot_env)

    sentinel.write_text(token)
    logger.info("offline venv ready: %s", venv_path)
# === charon-bootstrap: END =================================================

def setup_logging(
    log_dir: str = "sys.stdout", 
    verbose: bool = True, 
    program_name: str = PROGRAM_NAME
) -> tuple[logging.Logger, Path | None]:
    
    level = logging.DEBUG if verbose else logging.INFO
    formatter = logging.Formatter(_LOG_FORMAT)
    
    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()
    
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    root.addHandler(console)
    
    logfile: Path | None = None
    if log_dir and log_dir != 'sys.stdout':
        path = Path(log_dir)
        path.mkdir(parents=True, exist_ok=True)
        logfile = path / f"{datetime.now():%Y_%m_%d_%H_%M_%S}_{program_name}.log"
        fh = logging.FileHandler(logfile)
        fh.setFormatter(formatter)
        root.addHandler(fh)
        
    return logging.getLogger(program_name), logfile

def parse_config_file(config_path: str | Path = "", env: str = "") -> dict[str, str]:
    if os.name == 'nt':
        root_path = str(Path.home())
    else:
        root_path = str(Path('/'))
        
    os.environ['ROOT_PATH'] = root_path
    default_loc = '/stage/rundeck-scripts/.env'

    explicit = bool(config_path)
    if not config_path:
        config_path = root_path.rstrip('/') + default_loc

    path = Path(config_path)
    if not path.is_file():
        if explicit:
            raise FileNotFoundError(f"Config file not found: {path}")
        return {}
        
    raw = {}
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or line.startswith("!") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            raw[key.strip()] = val.strip().strip('"').strip("'")
            
    lookup = {**os.environ, **raw, "env": env, "ENV": env}
    
    def interpolate(val: str) -> str:
        previous = None
        loops = 0
        while val != previous and loops < 10:
            previous = val
            def repl(m: re.Match) -> str:
                name = m.group(1) or m.group(2) or m.group(3)
                return lookup.get(name, m.group(0))
            val = _VAR.sub(repl, val)
            loops += 1
        return val
        
    resolved = {k: interpolate(v) for k, v in raw.items()}
    
    for k, v in resolved.items():
        os.environ[k] = v
        
    if not resolved:
        raise RuntimeError('Config file contained no key=value pairs to interpolate.')

    return resolved

def parse_args(argv) -> argparse.Namespace:
    _env_help_msg = "Environment (dev01, mmdev, sit01, etc...) NOTE: This is not for .env files, use --config for those."
    _config_help_msg = "Path to environment config file with key=value pairs. Values can reference other keys with $KEY or ${KEY} syntax, and can also reference environment variables. See README for details."
    _venv_help_msg = "Path to venv for the child process (default: <repo>/.venv when charon-managed, else inherit caller's environment)"
    _verbose_help_msg = "Enable debug logging (default: errors and info only)"
    _log_help_msg = "The folder where the log should be written (default: sys.stdout)"
    _exec_help_msg = f"Child command to run. Must follow all master flags. Usage: {PROGRAM_NAME} [flags] --exec python script.py [child args...]"
    
    parser = argparse.ArgumentParser(
        prog=PROGRAM_NAME, 
        description=f"{PROGRAM_NAME}.py - universal pipeline orchestrator", 
        allow_abbrev=False
    )
    parser.add_argument("--env", dest="env", required=False, type=str, help=_env_help_msg, default="")
    parser.add_argument("--config", "--config_file", "--config-file", dest="config", required=False, type=str, default="", help=_config_help_msg)
    parser.add_argument("--venv", "--venv_dir", "--venv-dir", dest="venv", required=False, type=str, default="", help=_venv_help_msg)
    parser.add_argument("-v", "--verbose", action="store_true", default=False, help=_verbose_help_msg)
    parser.add_argument("-l", "--log", type=str, dest="log_dir", default="./.logs", required=False, help=_log_help_msg)
    parser.add_argument("--exec", nargs=argparse.REMAINDER, default=[], dest="exec", help=_exec_help_msg)
    
    args = parser.parse_args(argv)
    
    if not args.exec:
        parser.error("Child command required after --exec")
        
    return args

def main():
    # === charon-bootstrap: version guard (additive) ===
    if sys.version_info < _MIN_PYTHON:
        sys.stderr.write(
            f"{PROGRAM_NAME}: requires Python >= {_MIN_PYTHON[0]}.{_MIN_PYTHON[1]}, "
            f"but is running under {sys.version.split()[0]} ({sys.executable}).\n"
            f"On the server, invoke with python3.11 (not python3).\n"
        )
        sys.exit(2)
    # === end ===

    try:
        args = parse_args(sys.argv[1:])
        logger, logfile = setup_logging(args.log_dir, args.verbose, PROGRAM_NAME)
        
        logger.debug(f"\nStarting {PROGRAM_NAME} with args: {args}\n\n\n")
        
        config_vars = parse_config_file(args.config, env=args.env) # if args.config else {}

        # === charon-bootstrap: preflight (additive) ===
        # Managed repo (pyproject has [tool.charon]) -> vendor/heal/consume per OS.
        # Unmanaged repo -> do nothing; prepare_child handles --venv as it always has.
        repo_root = Path(__file__).resolve().parent
        managed, deps = _load_project(repo_root)
        if managed:
            if not getattr(args, "venv", ""):
                args.venv = str(repo_root / ".venv")
                logger.debug("charon-managed repo; venv defaults to %s", args.venv)
            ensure_wheelhouse(repo_root, deps, logger)
            ensure_venv(args.venv, repo_root, logger)
        # === end ===

        cmd, child_env, child_cwd = prepare_child(args, config_vars)
        
        logger.debug(f"Child working directory: {child_cwd}")
        
        process = subprocess.Popen(
            cmd, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE, 
            env=child_env, 
            cwd=child_cwd
        )
        
        assert process.stdout is not None
        assert process.stderr is not None
        
        log_lock = threading.Lock()
        lf = open(logfile, "a", encoding="utf-8") if logfile else None
        
        def stream_pipe(pipe: IO[bytes], out_stream: TextIO) -> None:
            for line in iter(pipe.readline, b""):
                text = line.decode("utf-8", errors="replace")
                out_stream.write(text)
                out_stream.flush()
                if lf:
                    with log_lock:
                        lf.write(text)
                        lf.flush()
            pipe.close()
            
        t_out = threading.Thread(target=stream_pipe, args=(process.stdout, sys.stdout))
        t_err = threading.Thread(target=stream_pipe, args=(process.stderr, sys.stderr))
        
        t_out.start()
        t_err.start()
        t_out.join()
        t_err.join()
        
        if lf:
            lf.close()
            
        process.wait()
        sys.exit(process.returncode)

    except Exception as e:
        raise e

if __name__ == '__main__':
    main()
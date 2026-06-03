#!/usr/bin/env python3.11
"""charon.py"""
from __future__ import annotations

import sys
import subprocess
import threading
import os
import argparse
import re
import logging
from pathlib import Path
from datetime import datetime
from typing import IO, TextIO

# Program identity is a hardcoded constant, not config: it is not sourced from
# stage/rundeck-scripts/.env nor from the ambient environment.
PROGRAM_NAME = "charon"

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
    _venv_help_msg = "Path to venv for the child process (default: inherit caller's environment)"
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
    try:
        args = parse_args(sys.argv[1:])
        logger, logfile = setup_logging(args.log_dir, args.verbose, PROGRAM_NAME)
        
        logger.debug(f"\nStarting {PROGRAM_NAME} with args: {args}\n\n\n")
        
        config_vars = parse_config_file(args.config, env=args.env) # if args.config else {}
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
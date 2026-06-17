```python
#!/usr/bin/env python3
"""
master.py - Universal pipeline orchestrator
Python 3.6+, stdlib only, no external dependencies

Usage:
    python3 master.py --env dev --config /stage/scripts/config.dat --exec python3 myscript.py [-- extra args]

Arguments:
    --env       Environment to run (dev, sit, uat, etc.)
    --config    Path to config.dat (default: /stage/scripts/config.dat)
    --exec      Program and script to run (everything after --exec up to -- is the command)
    --          Separator: everything after this is passed through to the child as args

Example:
    python3 master.py --env dev --exec python3 app.py -- --input myfile.csv --verbose
    python3 master.py --env sit --config /alt/config.dat --exec java -jar processor.jar
    python3 master.py --env dev --exec bash legacy_script.ksh
"""

import sys
import os
import subprocess
import argparse
import shlex

# Config parser
# Handles the legacy bash-style config.dat format:
#   user_dev_host='/path/to/db'
#   user_dev_username='myuser'

def parse_config(config_path, env):
    """
    Parse a bash-style key=value config file.
    Returns a dict of all variables, with indirect env references resolved.

    For example given env=dev:
        user_dev_host='/path/to/db'   -> included as-is
        user_app_host=user_dev_host   -> resolved to '/path/to/db'
    """
    if not os.path.isfile(config_path):
        fatal("Config file not found: {}".format(config_path))

    raw = {}

    with open(config_path, "r") as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()

            # skip blanks, comments, and shebangs
            if not line or line.startswith("#") or line.startswith("!"):
                continue

            # skip lines that don't look like assignments
            if "=" not in line:
                continue

            key, _, val = line.partition("=")
            key = key.strip()
            val = val.strip().strip("'\"")  # strip quotes

            raw[key] = val

    # second pass: resolve indirect references
    # e.g. user_app_host=user_dev_host -> look up raw['user_dev_host']
    resolved = {}
    for key, val in raw.items():
        if val in raw:
            resolved[key] = raw[val]
        else:
            resolved[key] = val

    return resolved


def build_child_env(config_vars):
    """
    Merge config variables into a copy of the current OS environment.
    Child processes inherit this enriched environment.
    """
    child_env = os.environ.copy()
    child_env.update(config_vars)
    return child_env


# Streaming subprocess runner
# Streams stdout and stderr live to console as the child produces output.
# Exits with the child's exit code on failure.

def run(cmd, child_env):
    """
    Run a command with live streaming stdout/stderr.
    Returns the exit code.
    """
    log("Running: {}".format(" ".join(cmd)))
    log("-" * 60)

    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=child_env
        )

        # stream stdout and stderr live
        # Python 3.6 doesn't have subprocess.run with live streaming
        # so we read line by line from both pipes
        import threading

        def stream_pipe(pipe, out_stream):
            for line in iter(pipe.readline, b""):
                out_stream.write(line.decode("utf-8", errors="replace"))
                out_stream.flush()
            pipe.close()

        stdout_thread = threading.Thread(
            target=stream_pipe, args=(process.stdout, sys.stdout)
        )
        stderr_thread = threading.Thread(
            target=stream_pipe, args=(process.stderr, sys.stderr)
        )

        stdout_thread.start()
        stderr_thread.start()

        stdout_thread.join()
        stderr_thread.join()

        process.wait()
        return process.returncode

    except FileNotFoundError:
        fatal("Command not found: {}".format(cmd[0]))
    except PermissionError:
        fatal("Permission denied running: {}".format(cmd[0]))
    except Exception as e:
        fatal("Unexpected error running child process: {}".format(str(e)))


# Argument parsing

def parse_args(argv):
    """
    Parse master.py's own args, cleanly separated from child args by --

    Everything before -- belongs to master.py
    Everything after -- is passed through to the child verbatim

    --exec is extracted manually so it doesn't greedily consume
    --env or --config regardless of argument order.
    """
    # split on -- to separate master args from child passthrough args
    if "--" in argv:
        split_at = argv.index("--")
        master_argv = argv[:split_at]
        passthrough_args = argv[split_at + 1:]
    else:
        master_argv = argv
        passthrough_args = []

    # manually extract --exec and its args
    # stop collecting when we hit another known master.py flag
    MASTER_FLAGS = {"--env", "--config"}
    exec_cmd = []
    if "--exec" in master_argv:
        exec_idx = master_argv.index("--exec")
        rest = master_argv[exec_idx + 1:]
        for i, token in enumerate(rest):
            if token in MASTER_FLAGS:
                exec_cmd = rest[:i]
                master_argv = master_argv[:exec_idx] + rest[i:]
                break
        else:
            exec_cmd = rest
            master_argv = master_argv[:exec_idx]

    parser = argparse.ArgumentParser(
        description="master.py - universal pipeline orchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    parser.add_argument(
        "--env",
        required=True,
        help="Environment to run against (dev, sit, uat, prod, etc.)"
    )

    parser.add_argument(
        "--config",
        default="/stage/scripts/config.dat",
        help="Path to config.dat (default: /stage/scripts/config.dat)"
    )

    args = parser.parse_args(master_argv)

    if not exec_cmd:
        parser.error("--exec is required and needs at least one argument (the program to run)")

    args.exec = exec_cmd
    return args, passthrough_args


# Logging helpers

def log(msg):
    print("[master] {}".format(msg), flush=True)

def fatal(msg):
    print("[master] FATAL: {}".format(msg), file=sys.stderr, flush=True)
    sys.exit(1)


# Entry point

def main():
    args, passthrough_args = parse_args(sys.argv[1:])

    log("Environment : {}".format(args.env))
    log("Config      : {}".format(args.config))
    log("Exec        : {}".format(" ".join(args.exec)))

    if passthrough_args:
        log("Passthrough : {}".format(" ".join(passthrough_args)))

    # parse config and build enriched environment
    config_vars = parse_config(args.config, args.env)
    child_env = build_child_env(config_vars)

    log("Loaded {} config variables".format(len(config_vars)))

    # build the full command: the --exec command + any passthrough args
    cmd = args.exec + passthrough_args

    # run it and exit with whatever the child exits with
    exit_code = run(cmd, child_env)

    log("-" * 60)
    if exit_code == 0:
        log("Process completed successfully (exit 0)")
    else:
        log("Process exited with code {}".format(exit_code))

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
```
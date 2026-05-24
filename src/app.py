#!/usr/bin/env python3
"""main.py"""
from __future__ import annotations
import logging
logger: logging.Logger = logging.getLogger(__name__)
import os, argparse
from typing import Any, Literal
from src.seeding import seeding_test

PROGRAM_NAME: str = os.getenv(key="PROGRAM_NAME", default="Python ETL")
PROJECT_ROOT: str = '.'
if os.path.basename(os.path.dirname(__file__)) == 'src':
    PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))

def main(**kwargs: Any) -> int:
    return seeding_test(**kwargs)

def cmd_line() -> Literal[0] | Literal[2]:
    parser = argparse.ArgumentParser(prog=PROGRAM_NAME, add_help=True)
    parser.add_argument('--source-system', required=True, type=str,)
    parser.add_argument('--source-environment', required=True, type=str,)
    parser.add_argument('--source-namespace', required=False, type=str,)
    parser.add_argument('--target-system', required=True, type=str,)
    parser.add_argument('--target-environment', required=True, type=str,)
    parser.add_argument('--target-namespace', required=False, type=str,)
    parser.add_argument('--tables', required=False, type=str, default=['*'], nargs='+')
    args: argparse.Namespace = parser.parse_args()

    result = 1

    try:
        result: int = main(**vars(args))
        if result == 0:
            return result
        raise Exception(f'{result}')
    except KeyboardInterrupt:
        return 2
    except Exception as e:
        raise e
    return result

if __name__ == '__main__':
    r"""
python "./main.py" -v -l ./.logs `
    --exec ./src/app.py `
    --source-system salesforce `
    --source-environment TRAIL `
    --source-namespace TRAIL `
    --target-system oracle `
    --target-environment DWH `
    --target-namespace DWH `
    --tables Contact Account
    """
    raise SystemExit(cmd_line())
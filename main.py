#!/usr/bin/env python3
from __future__ import annotations
import logging
logger: logging.Logger = logging.getLogger(__name__)
import sys,os, argparse
from typing import Any
from src.app import app

PROGRAM_NAME: str = os.getenv(key="PROGRAM_NAME", default="argparse_template")
PROJECT_ROOT: str = '.' 
if os.path.basename(os.path.dirname(__file__)) == 'src':
    PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))

def main(**kwargs: Any):
    app(**kwargs)

def cmd_line():
    parser = argparse.ArgumentParser(prog=PROGRAM_NAME, add_help=True)
    parser.add_argument('--system', required=True, type=str,)
    parser.add_argument('--environment', required=True, type=str,)
    parser.add_argument('--tables', required=False, type=str, default=['*'], nargs='+')
    parser.add_argument('--limit', required=False, type=int, default=200)
    args: argparse.Namespace = parser.parse_args()
    
    result = 1

    try:
        result = main(**vars(args))
        if result == 0:
            return result
        raise Exception(f'{result}')
    except KeyboardInterrupt:
        return 2
    except Exception as e:
        raise e
    return result

if __name__ == '__main__':
    """clear; python "./boot.py" -v  -l ./.logs --env QBL  --exec ./main.py --system oracle --environment QBL --tables QBL_USER"""
    raise SystemExit(cmd_line())

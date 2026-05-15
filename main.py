#!/usr/bin/env python3
from __future__ import annotations
import logging
logger: logging.Logger = logging.getLogger(__name__)
import sys,os
from typing import Any
from src.app import app

PROGRAM_NAME: str = os.getenv(key="PROGRAM_NAME", default="argparse_template")
PROJECT_ROOT: str = '.' 
if os.path.basename(os.path.dirname(__file__)) == 'src':
    PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))

def main(**kwargs: Any):
    app()

def cmd_line():
    parser = argparse.ArgumentParser(prog=PROGRAM_NAME, add_help=True)
    parser.add_argument('--source', required=True, type=str, help='Required: Path to file')
    parser.add_argument('--verbose', '-v', dest='verbose', action='store_true', default=False)
    parser.add_argument('--test', '-t', dest='test_run', action='store_true', default=False)
    parser.add_argument('--log-dir', default='sys.stdout', type=str, help='The folder where the log should be written')
    parser.add_argument('--main-dir', type=str, help='The parent folder for the log, error, and processed directories')
    parser.add_argument('--table', required=False, type=str, help='Oracle Table overriding csv name default')
    parser.add_argument('--schema', required=False, type=str, help='Oracle Schema overriding user default')
    parser.add_argument('--batch-size', type=int, required=False, help='Integer for batch sizes')
    parser.add_argument('--error-dir', type=str, required=False, help='Path to errored files')
    parser.add_argument('--processed-dir', type=str, required=False, help='Path to processed files')
    args = parser.parse_args()
    
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
    import argparse
    raise SystemExit(cmd_line())

#!/usr/bin/env python3

import argparse
import logging
import os
import sys
import unittest

import xmlrunner

from sqlitecaching.config import UTCFormatter
from sqlitecaching.enums import LogLevel
from sqlitecaching.test import TestLevel
from sqlitecaching.test import config as testconfig


def handle_arguments():
    argparser = argparse.ArgumentParser(
        description="Harness to run testing covering sqlitecaching functionality.",
    )
    argparser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=1,
        required=False,
    )
    argparser.add_argument(
        "-o",
        "--output-dir",
        default="./test-reports/unittest/",
        type=str,
        required=False,
    )
    argparser.add_argument(
        "--text",
        action="store_true",
    )

    argparser.add_argument(
        "-l",
        "--log-level",
        default="warning",
        type=str,
        choices=[level.casefold() for level in LogLevel.value_strs()],
    )
    argparser.add_argument(
        "-t",
        "--test-level",
        default="pre-commit",
        type=str,
        choices=TestLevel.value_strs(),
    )
    argparser.add_argument(
        "-T",
        "--test-log-level",
        default="notset",
        type=str,
        choices=[level.casefold() for level in LogLevel.value_strs()],
    )
    argparser.add_argument(
        "-O",
        "--log-output-dir",
        default=None,
        type=str,
        required=False,
    )

    args = argparser.parse_args()

    if not os.path.isdir(args.output_dir):
        os.makedirs(args.output_dir)
    testconfig.set_output_dir(args.output_dir)

    log_level = LogLevel.convert(args.log_level).value
    test_log_level = LogLevel.convert(args.test_log_level)

    root_logger = logging.getLogger()

    root_log_path = f"{args.output_dir}/test_handler.log"
    root_handler = logging.FileHandler(root_log_path)
    root_handler.setLevel(log_level)

    root_formatter = UTCFormatter()
    root_handler.setFormatter(root_formatter)

    root_logger.addHandler(root_handler)

    if not args.log_output_dir:
        args.log_output_dir = args.output_dir
    testconfig.set_logger_level(test_log_level)
    testconfig.set_log_output((f"{args.log_output_dir}/test.log", test_log_level))
    testconfig.set_debug_output(
        (f"{args.log_output_dir}/test.debug.log", LogLevel.DEBUG),
    )

    if args.text:
        args.testrunner = unittest.TextTestRunner()
    else:
        args.testrunner = xmlrunner.XMLTestRunner(output=args.output_dir)

    testconfig.set_test_level(args.test_level)

    return args


if __name__ == "__main__":
    args = handle_arguments()
    unittest.main(
        module=None,
        argv=[sys.argv[0]],
        testRunner=args.testrunner,
        verbosity=args.verbose,
    )

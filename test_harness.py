#!/usr/bin/env python3

import argparse
import logging
import os
import sys
import unittest

import xmlrunner

import tests
from sqlitecaching.enums import LogLevel


def handle_arguments():
    argparser = argparse.ArgumentParser(
        description="Harness to run testing covering sqlitecaching functionality."
    )
    argparser.add_argument("-v", "--verbose", action="count", default=1, required=False)
    argparser.add_argument(
        "-o",
        "--output-dir",
        default="./test-reports/unittest/",
        type=str,
        required=False,
    )
    argparser.add_argument(
        "--text", action="store_true",
    )

    argparser.add_argument(
        "-l",
        "--log-level",
        default="warning",
        type=str,
        choices=[level.casefold() for level in LogLevel.values()],
    )
    argparser.add_argument(
        "-t",
        "--test-level",
        default="pre-commit",
        type=str,
        choices=tests.TestLevel.values(),
    )
    argparser.add_argument(
        "-T",
        "--test-log-level",
        default="warning",
        type=str,
        choices=[level.casefold() for level in LogLevel.values()],
    )
    argparser.add_argument(
        "-O", "--log-output-dir", default=None, type=str, required=False,
    )

    args = argparser.parse_args()

    if not os.path.isdir(args.output_dir):
        os.makedirs(args.output_dir)
    tests.config.set_output_dir(args.output_dir)
    root_logger = logging.getLogger("")
    root_log_path = f"{args.output_dir}/test_handler.log"
    root_handler = logging.FileHandler(root_log_path)
    root_handler.setLevel(LogLevel.convert(args.log_level).value[1])
    root_logger.addHandler(root_handler)

    if not args.log_output_dir:
        args.log_output_dir = args.output_dir
    tests.config.set_log_dir(args.log_output_dir)

    if args.text:
        args.testrunner = unittest.TextTestRunner()
    else:
        args.testrunner = xmlrunner.XMLTestRunner(output=args.output_dir)

    tests.config.set_test_level(args.test_level)
    tests.config.set_log_level(args.test_log_level)

    return args


if __name__ == "__main__":
    args = handle_arguments()
    unittest.main(
        module=None,
        argv=[sys.argv[0]],
        testRunner=args.testrunner,
        verbosity=args.verbose,
    )

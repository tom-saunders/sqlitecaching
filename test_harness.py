#!/usr/bin/env python3

import argparse
import os
import sys
import unittest

import xmlrunner

import tests


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
        "-l",
        "--level",
        default="pre-commit",
        type=str,
        choices=tests.TestLevel.values(),
    )
    argparser.add_argument(
        "-t", "--text", action="store_true",
    )
    argparser.add_argument(
        "-L",
        "--log-level",
        default="notset",
        type=str,
        choices=tests.TestLogLevel.values(),
    )

    args = argparser.parse_args()

    if not os.path.isdir(args.output_dir):
        os.makedirs(args.output_dir)
    tests.Config.set_output_dir(args.output_dir)

    if args.text:
        args.testrunner = unittest.TextTestRunner()
    else:
        args.testrunner = xmlrunner.XMLTestRunner(output=args.output_dir)

    tests.Config.set_test_level(args.level)
    tests.Config.set_log_level(args.log_level)

    return args


if __name__ == "__main__":
    args = handle_arguments()
    unittest.main(
        module=None,
        argv=[sys.argv[0]],
        testRunner=args.testrunner,
        verbosity=args.verbose,
    )

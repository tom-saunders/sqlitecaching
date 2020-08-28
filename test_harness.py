#!/usr/bin/env python3

import argparse
import os
import sys
import unittest

import xmlrunner

import sqlitecaching


def handle_arguments():
    argparser = argparse.ArgumentParser(
        description="Harness to run testing covering sqlitecaching functionality."
    )
    argparser.add_argument("-v", "--verbose", action="count", default=1, required=False)
    argparser.add_argument(
        "-o", "--output", default="test-reports/", type=str, required=False
    )
    argparser.add_argument(
        "-l",
        "--level",
        default="pre-commit",
        type=str,
        choices=sqlitecaching.TestLevel.values(),
    )

    args = argparser.parse_args()
    if not os.path.isdir(args.output):
        os.makedirs(args.output)
    sqlitecaching.set_test_level(args.level)
    return args


if __name__ == "__main__":
    args = handle_arguments()
    unittest.main(
        module=None,
        argv=[sys.argv[0]],
        testRunner=xmlrunner.XMLTestRunner(output=args.output),
        verbosity=args.verbose,
    )

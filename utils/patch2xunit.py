#!/usr/bin/env python3

import argparse
import logging
import re
import sys

from sqlitecaching.config import UTCFormatter

# import xml.etree.ElementTree as ET


log = logging.getLogger(__name__)


def handle_arguments():
    argparser = argparse.ArgumentParser(
        description="Utility to convert patch files into a rough xunit format."
    )
    argparser.add_argument(
        "-l",
        "--log-file",
        default="./test-reports/black/patch2xunit.log",
        type=str,
        required=False,
    )
    argparser.add_argument(
        "-L", "--log-level", action="count", default=1, required=False
    )
    argparser.add_argument(
        "-o", "--output", default=None, type=str, required=False,
    )
    argparser.add_argument(
        "-i", "--input", default=None, type=str, required=False,
    )

    args = argparser.parse_args()

    if args.log_level == 1:
        log_level = logging.WARNING
    if args.log_level == 2:
        log_level = logging.INFO
    if args.log_level == 3:
        log_level = logging.DEBUG

    root_logger = logging.getLogger()

    root_log_path = args.log_file
    root_handler = logging.FileHandler(root_log_path)
    root_handler.setLevel(log_level)

    root_formatter = UTCFormatter()
    root_handler.setFormatter(root_formatter)

    root_logger.addHandler(root_handler)

    return args


# fmt: off
__FILE_PATCH_FIRST_LINE_PATTERN = re.compile(
    r"^        # start of line""\n"
    r"---      # before marker for diff""\n"
    r"[ ]      # single literal space""\n"
    r"(        # capturing group (filename)""\n"
    r"  [^\t]+ # ""\n"
    r")        # close group""\n"
    r"[\t]     # single literal tab""\n"
    r"(        # capturing group (timestamp)""\n"
    r"  .+     # anything""\n"
    r")        # close group""\n"
    r"$        # end of line""\n",
    re.VERBOSE)
__FILE_PATCH_SECOND_LINE_PATTERN = re.compile(
    r"^        # start of line""\n"
    r"[+][+][+]# after marker for diff""\n"
    r"[ ]      # single literal space""\n"
    r"(        # capturing group (filename)""\n"
    r"  [^\t]+ # ""\n"
    r")        # close group""\n"
    r"[\t]     # single literal tab""\n"
    r"(        # capturing group (timestamp)""\n"
    r"  .+     # anything""\n"
    r")        # close group""\n"
    r"$        # end of line""\n",
    re.VERBOSE)
__FILE_HUNK_FIRST_LINE_PATTERN = re.compile(
    r"^        # start of line""\n"
    r"@@       # hunk marker for diff""\n"
    r"[ ][-]   # single literal space, dash""\n"
    r"(        # capturing group (before_start)""\n"
    r"  [0-9]+ # before_start line number""\n"
    r")        # close group""\n"
    r"[,]      # single literal comma""\n"
    r"(        # capturing group (before_length)""\n"
    r"  [0-9]+ # before_length line count""\n"
    r")        # close group""\n"
    r"[ ][+]   # single literal space, plus""\n"
    r"(        # capturing group (after_start)""\n"
    r"  [0-9]+ # after_start line number""\n"
    r")        # close group""\n"
    r"[,]      # single literal comma""\n"
    r"(        # capturing group (after_length)""\n"
    r"  [0-9]+ # after_length line count""\n"
    r")        # close group""\n"
    r"$        # end of line""\n",
    re.VERBOSE)
# fmt: on


def process_hunk(*, in_file):
    return {}


def process_input_from_file(*, in_file):
    root = {}
    line = in_file.readline()
    while line:
        # line is expected to be the first line of a patch:
        # ^--- file/name.py<TAB>YYYY-MM-DD HH:MM_SS.uuuuuu +OOOO
        match = __FILE_PATCH_FIRST_LINE_PATTERN.match(line)
        if not match:
            log.info("input line does not match patch first line format, skip")
            log.debug("input line was [%s]", line)
            continue
        file_name = match.group(1)
        second_line = in_file.readline()
        second_match = __FILE_PATCH_FIRST_LINE_PATTERN.match(second_line)
        if not second_match:
            raise Exception("oh no")
        root[file_name] = process_hunk(in_file)
        line = in_file.readline()
    return root


def process_input(*, input_path):
    if input_path:
        log.info("reading from [%s]", input_path)
        with open(input_path, "r") as in_file:
            return process_input_from_file(in_file=in_file)
    else:
        log.info("reading from stdin as input_path is [%s]", input_path)
        return process_input_from_file(in_file=sys.stdin)


def write_output(*, output_path, content):
    pass


if __name__ == "__main__":
    args = handle_arguments()
    processed_input = process_input(input_path=args.input)
    write_output(output_path=args.output, content=processed_input)

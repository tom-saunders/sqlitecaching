#!/usr/bin/env python3

import argparse
import logging
import re
import sys
import xml.etree.ElementTree as ET

from sqlitecaching.config import UTCFormatter

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)


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
        "-L", "--log-level", action="count", default=0, required=False
    )
    argparser.add_argument(
        "-o", "--output", default=None, type=str, required=False,
    )
    argparser.add_argument(
        "-i", "--input", default=None, type=str, required=False,
    )

    args = argparser.parse_args()

    if args.log_level == 0:
        log_level = logging.WARNING
    if args.log_level == 1:
        log_level = logging.INFO
    if args.log_level >= 2:
        log_level = logging.DEBUG

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

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
    r"^         # start of line""\n"
    r"[+][+][+] # after marker for diff""\n"
    r"[ ]       # single literal space""\n"
    r"(         # capturing group (filename)""\n"
    r"  [^\t]+  # ""\n"
    r")         # close group""\n"
    r"[\t]      # single literal tab""\n"
    r"(         # capturing group (timestamp)""\n"
    r"  .+      # anything""\n"
    r")         # close group""\n"
    r"$         # end of line""\n",
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
    r"[ ]      # single literal space""\n"
    r"@@       # hunk marker for diff""\n"
    r"$        # end of line""\n",
    re.VERBOSE)
__FILE_HUNK_UNCHANGED_LINE_PATTERN = re.compile(
    r"^        # start of line""\n"
    r"[ ]      # single literal space""\n"
    r".*       # anything""\n"
    r"$        # end of line""\n",
    re.VERBOSE)
__FILE_HUNK_REMOVED_LINE_PATTERN = re.compile(
    r"^        # start of line""\n"
    r"[-]      # single literal dash""\n"
    r".*       # anything""\n"
    r"$        # end of line""\n",
    re.VERBOSE)
__FILE_HUNK_ADDED_LINE_PATTERN = re.compile(
    r"^        # start of line""\n"
    r"[+]      # single literal plus""\n"
    r".*       # anything""\n"
    r"$        # end of line""\n",
    re.VERBOSE)
# fmt: on


def valid_counts(*, before_count, after_count):
    if before_count < 0:
        log.error("negative before_count: %s", before_count)
        raise Exception("oh no")
    elif after_count < 0:
        log.error("negative after_count: %s", after_count)
        raise Exception("oh no")
    elif before_count or after_count:
        return True
    else:
        return False


def process_hunk(*, in_file, hunk_header, before_count, after_count):
    hunk_lines = []
    hunk_lines.append(hunk_header)
    while valid_counts(before_count=before_count, after_count=after_count):
        line = in_file.readline()
        hunk_lines.append(line)
        if __FILE_HUNK_UNCHANGED_LINE_PATTERN.match(line):
            before_count -= 1
            after_count -= 1
        elif __FILE_HUNK_REMOVED_LINE_PATTERN.match(line):
            before_count -= 1
        elif __FILE_HUNK_ADDED_LINE_PATTERN.match(line):
            after_count -= 1
    return "".join(hunk_lines)


def process_hunks(*, in_file):
    hunks = []
    line = in_file.readline()
    # line is expected to be first line of a hunk
    match = __FILE_HUNK_FIRST_LINE_PATTERN.match(line)
    log.debug("line: [%s]", line)
    log.debug("match hunk: [%s]", match)
    while match:
        hunk = process_hunk(
            in_file=in_file,
            hunk_header=line,
            before_count=int(match.group(2)),
            after_count=int(match.group(4)),
        )
        hunks.append(hunk,)
        line = in_file.readline()
        match = __FILE_HUNK_FIRST_LINE_PATTERN.match(line)

    return (hunks, line)


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
            line = in_file.readline()
            continue
        file_name = match.group(1)
        second_line = in_file.readline()
        second_match = __FILE_PATCH_SECOND_LINE_PATTERN.match(second_line)
        if not second_match:
            raise Exception("oh no")
        (root[file_name], line) = process_hunks(in_file=in_file)
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
    tree_builder = ET.TreeBuilder()
    root = tree_builder.start("testsuites", {})
    root_failures = 0
    for (path, hunks) in content.items():
        path_el = tree_builder.start("testsuite", {"name": path})
        path_failures = 0
        for hunk in hunks:
            tree_builder.start(
                "testcase", {"name": f"{path}.path_failures", "classname": path},
            )
            fail_el = tree_builder.start("failure", {})
            fail_el.text = hunk
            root_failures += 1
            path_failures += 1
            tree_builder.end("failure")
            tree_builder.end("testcase")
        path_el.set("tests", str(path_failures))
        path_el.set("failures", str(path_failures))
        tree_builder.end("testsuite")
    root.set("tests", str(root_failures))
    root.set("failures", str(root_failures))
    tree_builder.end("testsuites")
    xml_tree = ET.ElementTree(tree_builder.close())

    if output_path:
        with open(output_path, "w+") as out:
            xml_tree.write(out, encoding="unicode")
    else:
        xml_tree.write(sys.stdout, encoding="unicode")


if __name__ == "__main__":
    args = handle_arguments()
    processed_input = process_input(input_path=args.input)
    write_output(output_path=args.output, content=processed_input)

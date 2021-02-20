#!/usr/bin/python3

#  Copyright 2021 Kilian Lackhove

import argparse
import asyncio
import logging
import re
import subprocess
from typing import List

import sys

logger = logging.getLogger("localdup")


def setup_logging(verbosity):
    global formatter
    sh = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(str("{asctime} {levelname} {message}"), style="{")
    sh.setFormatter(formatter)
    logger.addHandler(sh)

    logger.setLevel(
        {
            0: logging.CRITICAL,
            1: logging.ERROR,
            2: logging.WARNING,
            3: logging.INFO,
            4: logging.DEBUG,
        }.get(verbosity)
    )


async def read_stream(stream, log_func):
    output = ""
    while True:
        line = await stream.readline()
        if not line:
            break
        line = line.decode("utf-8").rstrip()
        output += line
        log_func(line)
    return output


async def call_rsync_async(command: List[str]):

    logger.debug(f"running command '{' '.join(command)}'")
    process = await asyncio.create_subprocess_exec(
        *command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    try:
        out, err = await asyncio.gather(
            read_stream(process.stdout, lambda x: logger.info("rsync STDOUT: " + x)),
            read_stream(process.stderr, lambda x: logger.info("rsync STDERR: " + x)),
        )
    except Exception:
        process.kill()
        raise
    finally:
        exitcode = await process.wait()

    if exitcode != 0:
        raise subprocess.CalledProcessError(exitcode, " ".join(command), out, err)

    return out, err


def main():
    parser = argparse.ArgumentParser(
        description="perform rsync backups, prune and checks"
    )
    parser.add_argument("--verbosity", type=int, choices=range(5), default=3)
    args = parser.parse_args()
    setup_logging(args.verbosity)

    if get_backup_size() >= get_current_size() * 1.2:
        logger.error("localdup failed due to memory limit")
        subprocess.run(["kdialog", "--sorry", "localdup failed due to memory limit"])
        sys.exit(-1)

    logger.info(f"starting backup")
    success = True
    for command in (
        [
            "rsync",
            "--stats",
            "--delete",
            "--delete-excluded",
            "-au",
            "--exclude-from=/home/kilian/.rsync-exclude",
            "--include=.*",
            "--exclude=/*",
            "/home/lackhove/",
            "/home/lackhove/ownCloud/backup_rechenknecht/home/",
        ],
        [
            "rsync",
            "--stats",
            "--delete",
            "--delete-excluded",
            "-au",
            "/etc/",
            "/home/lackhove/ownCloud/backup_rechenknecht/etc/",
        ],
    ):
        try:
            asyncio.run(call_rsync_async(command))
        except subprocess.CalledProcessError:
            pass
    logger.info(f"done (?)")


def get_current_size():
    proc = subprocess.run(
        [
            "du",
            "-s",
            "--block-size=1",
            "/home/lackhove/ownCloud/backup_rechenknecht/home/",
        ],
        text=True,
        capture_output=True,
    )
    dir_size = int(proc.stdout.split()[0])
    return dir_size


def get_backup_size():
    proc = subprocess.run(
        [
            "rsync",
            "-n",
            "--stats",
            "--delete",
            "--delete-excluded",
            "-au",
            "--exclude-from=/home/kilian/.rsync-exclude",
            "--include=.*",
            "--exclude=/*",
            "/home/lackhove/",
            "/home/lackhove/ownCloud/backup_rechenknecht/home/",
        ],
        text=True,
        capture_output=True,
    )
    backup_size = None
    for line in proc.stdout.splitlines():
        m = re.match(r"Total file size: (.*?) bytes", line)
        if m is None:
            continue
        backup_size = int(m.group(1).replace(",", ""))
        break
    return backup_size


if __name__ == "__main__":
    main()

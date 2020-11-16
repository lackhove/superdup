#!/usr/bin/python3

#  Copyright 2020 Kilian Lackhove

import argparse
import asyncio
import json
import logging
import re
import smtplib
import socket
import ssl
import subprocess
import sys
from configparser import ConfigParser
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from functools import wraps
from pathlib import Path
from time import sleep
from typing import List

import attr

logger = logging.getLogger("superdup")
logger.addHandler(logging.StreamHandler(sys.stdout))


@attr.s
class Config:
    duplicacy_command: str = attr.ib(default="/usr/local/bin/duplicacy", converter=str)
    stamp_path: Path = attr.ib(
        default=Path("/superdup/stamps.json"),
        converter=lambda x: Path(x).expanduser().resolve(),
    )
    log_path: Path = attr.ib(
        default=Path("/superdup/logs"),
        converter=lambda x: Path(x).expanduser().resolve(),
    )
    source_path_dirs: Path = attr.ib(
        default=Path("/source_dirs/"),
        converter=lambda x: Path(x).expanduser().resolve(),
    )
    dry_run: bool = attr.ib(default=False, converter=bool)
    num_logfiles: int = attr.ib(default=5, converter=int)

    email_to: str = attr.ib(default="bar@foo.com", converter=str)
    email_from: str = attr.ib(default="noreply@foo.com", converter=str)
    email_server: str = attr.ib(default="smtp.foobar.com", converter=str)
    email_port: int = attr.ib(default=465, converter=int)
    email_username: str = attr.ib(default="noreply@foo.com", converter=str)
    email_password: str = attr.ib(default="verysecret", converter=str)

    duplicacy_env = attr.ib(factory=dict)

    @classmethod
    def from_ini_file(cls, path: Path):
        parser = ConfigParser(interpolation=None)
        # enable case sensitive keys
        parser.optionxform = lambda option: option
        parser.read(path)
        kwargs = dict(parser["superdup"])
        kwargs["duplicacy_env"] = dict(parser["dulicacy-env"])

        return cls(**kwargs)


config = Config()


def log_to_file(func):
    @wraps(func)
    def wrapper_log_to_file(source_dir):
        logfile_path = (
            config.log_path
            / source_dir.name
            / f"{func.__name__}_{datetime.now().isoformat()}.log"
        )
        logfile_path.parent.mkdir(parents=True, exist_ok=True)

        old_logfiles = sorted(
            logfile_path.parent.glob(f"{func.__name__}_*.log"), reverse=True
        )
        for del_file in old_logfiles[config.num_logfiles :]:
            logger.debug(f"purging old logfile {del_file}")
            del_file.unlink()

        file_handler = logging.FileHandler(logfile_path)
        logger.addHandler(file_handler)

        value = func(source_dir)

        logger.removeHandler(file_handler)

        return value

    return wrapper_log_to_file


class NetworkError(Exception):
    pass


def wait_online():
    for i in range(10):
        try:
            socket.gethostbyname("www.google.de")
            return True
        except socket.error:
            logger.critical(f"not online, retrying in {2**i} seconds")
            sleep(2 ** i)
    else:
        return False


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


async def call_duplicacy_async(command: List[str], cwd: Path, dry_run=None):
    dry_run = config.dry_run if dry_run is None else dry_run

    command.insert(0, config.duplicacy_command)
    if logger.isEnabledFor(logging.DEBUG):
        command.insert(1, "-debug")
    if dry_run:
        logger.debug(f"would run command '{' '.join(command)}' (dry-run)")
    else:
        logger.debug(f"runnig command '{' '.join(command)}'")
    if not dry_run:
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=config.duplicacy_env,
            cwd=cwd,
        )
        try:
            out, err = await asyncio.gather(
                read_stream(
                    process.stdout, lambda x: logger.info("duplicacy STDOUT: " + x)
                ),
                read_stream(
                    process.stderr, lambda x: logger.info("duplicacy STDERR: " + x)
                ),
            )
        except Exception:
            process.kill()
            raise
        finally:
            exitcode = await process.wait()

        if exitcode != 0:
            raise subprocess.CalledProcessError(exitcode, " ".join(command), out, err)

        return out, err
    else:
        return "", ""


@log_to_file
def backup(source_dir: Path) -> bool:
    logger.info(f"starting backup for {source_dir}")
    try:
        asyncio.run(call_duplicacy_async(["backup", "-stats"], source_dir))
        return True
    except subprocess.CalledProcessError:
        logger.error("backup failed for {}".format(source_dir))
        return False


@log_to_file
def prune(source_dir):
    logger.info(f"starting prune for {source_dir}")
    try:
        asyncio.run(
            call_duplicacy_async(
                [
                    "prune",
                    "-keep",
                    "0:360",
                    "-keep",
                    "30:30",
                    "-keep",
                    "7:7",
                    "-keep",
                    "1:1",
                ],
                source_dir,
            )
        )
        return True
    except subprocess.CalledProcessError:
        logger.error(f"prune failed for {source_dir}")
        return False


def load_stamps():
    try:
        with open(config.stamp_path, "r") as f:
            stamps = json.load(f)
    except IOError:
        stamps = {}
    return stamps


def save_stamp(source_dir):
    stamps = load_stamps()
    stamps[source_dir.as_posix()] = datetime.now().isoformat()
    config.stamp_path.parent.mkdir(parents=True, exist_ok=True)
    with open(config.stamp_path, "w") as f:
        json.dump(stamps, f)
    logger.debug(f"saved stamp for {source_dir.as_posix()}: {stamps}")


@log_to_file
def verify(source_dir):
    logger.info(f"starting verification for {source_dir}")

    try:
        out, _ = asyncio.run(call_duplicacy_async(["list"], source_dir, dry_run=False))
        matches = re.findall(r"Snapshot (\S+) revision (\d+)", out)
        if len(matches) == 0:
            logger.warning(f"No snapshots found for {source_dir}")
            return False

        snapshot_id, latest_rev = matches[-1]
        logger.info(f"verifying snapshot {snapshot_id} revision {latest_rev}")
        asyncio.run(
            call_duplicacy_async(
                ["check", "-chunks", "-r", latest_rev, "-id", snapshot_id], source_dir
            )
        )
        save_stamp(source_dir)
        return True
    except subprocess.CalledProcessError:
        logger.error(f"verification failed for {source_dir}")
        return False


@log_to_file
def check(source_dir):
    logger.info(f"starting check for {source_dir}")

    try:
        asyncio.run(call_duplicacy_async(["check"], source_dir))
        return True
    except subprocess.CalledProcessError:
        logger.error(f"ERROR: check failed for {source_dir}")
        return False


def is_verification_scheduled(source_dir: Path):
    stamps = load_stamps()
    if source_dir.as_posix() in stamps:
        last_verify = datetime.fromisoformat(stamps[source_dir.as_posix()])
        time_diff = datetime.now() - last_verify
        logger.info(f"last verification was {time_diff} ago")
        if time_diff > timedelta(days=90):
            return True
    else:
        logger.info("no previous verification found")
        return True

    return False


def latest_logs(summary):
    for source_dir, results in summary.items():
        for step_name in results:
            logfiles = sorted(
                Path(config.log_path, source_dir.name).glob(f"{step_name}_*.log"),
                reverse=True,
            )
            if len(logfiles) > 0:
                yield logfiles[0]


def email_notify(summary):
    message = MIMEMultipart()
    message["From"] = config.email_from
    message["To"] = config.email_to
    message["Subject"] = "superdup: " + ("SUCCESS" if successful(summary) else "ERROR")
    message.attach(MIMEText(summary_to_str(summary), "plain"))

    for logfile_path in latest_logs(summary):
        with open(logfile_path, "r") as fd:
            attachment = MIMEText(fd.read())
            attachment.add_header(
                "Content-Disposition", "attachment", filename=logfile_path.parent.name + "_" + logfile_path.name
            )
            message.attach(attachment)

    with smtplib.SMTP_SSL(
        config.email_server, config.email_port, context=ssl.create_default_context(),
    ) as server:
        server.login(config.email_username, config.email_password)
        server.sendmail(config.email_from, config.email_to, message.as_string())

    logger.info(f"sent email notification to {config.email_to}")


def successful(summary):
    for source_dir, results in summary.items():
        for step_name, state in results.items():
            if state is False:
                return False
    return True


def main():
    parser = argparse.ArgumentParser(
        description="perform duplicacy backups, prune and checks"
    )
    parser.add_argument("--verbosity", type=int, choices=range(5), default=3)
    parser.add_argument(
        "--config",
        type=lambda x: Path(x).expanduser().resolve(),
        default=Path("/superdup/config.ini"),
    )
    parser.add_argument("--force-verification", action="store_true")
    parser.add_argument("--dry-run", action="store_true")

    args = parser.parse_args()

    logger.setLevel(
        {
            0: logging.CRITICAL,
            1: logging.ERROR,
            2: logging.WARNING,
            3: logging.INFO,
            4: logging.DEBUG,
        }.get(args.verbosity)
    )

    global config
    config = Config.from_ini_file(args.config)
    config.dry_run = config.dry_run or args.dry_run

    if not wait_online():
        logger.critical("not online, exiting")
        sys.exit(-1)

    summary = {}
    for sd in config.source_path_dirs.iterdir():
        if not sd.is_dir():
            logger.info(f"Skipping {sd}")
            continue
        if not Path(sd, ".duplicacy").is_dir():
            logger.info(f"Skipping {sd}, not a duplicacy repo")
            continue

        summary[sd] = {"backup": backup(sd), "prune": prune(sd)}
        if args.force_verification or is_verification_scheduled(sd):
            summary[sd]["verify"] = verify(sd)
        else:
            summary[sd]["check"] = check(sd)

    logger.info(summary_to_str(summary))

    email_notify(summary)

    if not successful(summary):
        sys.exit(1)


def summary_to_str(summary):
    retval = "Summary:"
    for sd, results in summary.items():
        retval += f"\n  {sd.as_posix()}:"
        for step_name, step_result in results.items():
            retval += f"\n    {step_name:6}: {'OK' if step_result else 'FAILED'}"
    retval += "\n"
    retval += "\nSee individual logfiles for more info"

    return retval


if __name__ == "__main__":
    main()

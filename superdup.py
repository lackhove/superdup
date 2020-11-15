#!/usr/bin/python3

#  Copyright 2020 Kilian Lackhove

import argparse
import json
import logging
import re
import subprocess
import sys
from configparser import ConfigParser
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import List

logger = logging.getLogger("superdup")
logger.addHandler(logging.StreamHandler(sys.stdout))


@dataclass
class Config:
    duplicacy_command: str = "/usr/bin/duplicacy"
    stamp_path: Path = Path("~/.duplicacy_stamps_new").expanduser().resolve()
    source_path_dirs: Path = Path("/source_dirs/")
    dry_run: bool = True
    duplicacy_env = {
        "DUPLICACY_PASSWORD": "topsecret",
        "DUPLICACY_B2_ID": "secret",
        "DUPLICACY_B2_KEY": "supersecret",
    }

    __instance = None

    def __new__(cls):
        if cls.__instance is None:
            cls.__instance = super(Config, cls).__new__(cls)
        return cls.__instance

    def from_ini_file(self, path: Path):
        parser = ConfigParser()
        # enable case sensitive keys
        parser.optionxform = lambda option: option
        parser.read(path)
        kwargs = dict(parser["superdup"])
        # TODO: use attrs so we can have converters
        self.duplicacy_command = str(
            kwargs.get("duplicacy_command", self.duplicacy_command)
        )
        self.stamp_path = (
            Path(kwargs.get("stamp_path", self.stamp_path)).expanduser().resolve()
        )
        self.source_path_dirs = (
            Path(kwargs.get("source_path_dirs", self.source_path_dirs))
            .expanduser()
            .resolve()
        )

        envs = dict(parser["dulicacy-env"])
        self.duplicacy_env.update(envs)


class NetworkError(Exception):
    pass


def test_online():
    # TODO: implement this
    return True


def call_duplicacy(command: List[str], cwd: Path, dry_run=None):
    config = Config()
    if dry_run is None:
        dry_run = config.dry_run

    command.insert(0, config.duplicacy_command)
    if logger.isEnabledFor(logging.DEBUG):
        command.insert(1, "-debug")
    if dry_run:
        logger.debug(f"would run command '{' '.join(command)}' (dry-run)")
    else:
        logger.debug(f"runnig command '{' '.join(command)}'")
    if not dry_run:
        p = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=config.duplicacy_env,
            cwd=cwd,
            text=True,
        )
        out = ""
        while p.poll() is None:
            tmp = p.stdout.readline().rstrip()
            logger.debug("duplicacy STDOUT: " + tmp)
            out += tmp + "\n"
        tmp, err = p.communicate()
        out += tmp + "\n"
        if p.returncode != 0:
            raise subprocess.CalledProcessError(
                p.returncode, " ".join(command), out, err
            )
        return out, err
    else:
        return "", ""


def backup(source_dir: Path) -> bool:
    logger.info(f"starting backup for {source_dir}")
    try:
        call_duplicacy(["backup", "-stats"], cwd=source_dir)
    except subprocess.CalledProcessError:
        logger.error("backup failed for {}".format(source_dir), flush=True)
        return False
    return True


def prune(source_dir):
    logger.info(f"starting prune for {source_dir}")
    try:
        call_duplicacy(
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
            cwd=source_dir,
        )
    except subprocess.CalledProcessError:
        logger.error(f"prune failed for {source_dir}")
        return False
    return True


def load_stamps():
    config = Config()

    try:
        with open(config.stamp_path, "r") as f:
            stamps = json.load(f)
    except IOError:
        stamps = {}
    return stamps


def save_stamp(source_dir):
    config = Config()

    stamps = load_stamps()
    stamps[source_dir.as_posix()] = datetime.now().isoformat()
    config.stamp_path.parent.mkdir(parents=True, exist_ok=True)
    with open(config.stamp_path, "w") as f:
        json.dump(stamps, f)


def verify(source_dir):
    logger.info(f"starting verification for {source_dir}")

    out, _ = call_duplicacy(["list"], cwd=source_dir, dry_run=False)
    matches = re.findall(r"Snapshot (\S+) revision (\d+)", out)
    snapshot_id, latest_rev = matches[-1]

    logger.info(f"verifying snapshot {snapshot_id} revision {latest_rev}")
    try:
        call_duplicacy(
            ["check", "-chunks", "-r", latest_rev, "-id", snapshot_id], cwd=source_dir,
        )
    except subprocess.CalledProcessError:
        logger.error(f"verification failed for {source_dir}")
        return False

    save_stamp(source_dir)

    return True


def check(source_dir):
    logger.info(f"starting check for {source_dir}")
    config = Config()

    try:
        call_duplicacy([config.duplicacy_command, "check"], cwd=source_dir)
    except subprocess.CalledProcessError:
        logger.error(f"ERROR: check failed for {source_dir}")
        return False
    return True


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


def main():
    parser = argparse.ArgumentParser(
        description="perform duplicacy backups, prune and checks"
    )
    parser.add_argument("--verbosity", type=int, choices=range(5))
    parser.add_argument(
        "--config",
        type=lambda x: Path(x).expanduser().resolve(),
        default=Path("config.ini").resolve(),
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

    config = Config()
    config.from_ini_file(args.config)
    config.dry_run = args.dry_run

    if not test_online():
        logger.critical("not online exiting", flush=True)
        sys.exit(-1)

    summary = {}
    for sd in (Path("/etc/").expanduser().resolve(),):  # SOURCE_DIRS_PATH.iterdir():
        if not sd.is_dir():
            logger.info(f"Skipping {sd}")
            continue
        if not Path(sd, ".duplicacy").is_dir():
            logger.info(f"Skipping {sd}, not a duplicacy repo")
            continue

        summary[sd] = {"backup": backup(sd), "prune": prune(sd)}
        if not args.force_verification or is_verification_scheduled(sd):
            summary[sd]["verify"] = verify(sd)
        else:
            summary[sd]["check"] = check(sd)

    logger.info("")
    logger.info("Summary:")
    for sd, results in summary.items():
        logger.info(f"  {sd.as_posix()}:")
        for step_name, step_result in results.items():
            logger.info(f"    {step_name:6}: {'OK'if step_result else 'FAILED'}")


if __name__ == "__main__":
    main()

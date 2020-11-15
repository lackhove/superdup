#!/usr/bin/python3

#  Copyright 2020 Kilian Lackhove

import argparse
import json
import logging
import re
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Mapping

DUPLICACY_COMMAND = "/usr/bin/duplicacy"
STAMP_PATH = Path("~/.duplicacy_stamps_new").expanduser().resolve()
SOURCE_DIRS_PATH = Path("/source_dirs/")
DRY_RUN = True

DUPLICACY_ENV = {
    "DUPLICACY_PASSWORD": "topsecret",
    "DUPLICACY_B2_ID": "secret",
    "DUPLICACY_B2_KEY": "supersecret",
}


logger = logging.getLogger("superdup")
logger.addHandler(logging.StreamHandler(sys.stdout))


class NetworkError(Exception):
    pass


def test_online():
    return True


def call_duplicacy(command: List[str], cwd: Path, dry_run=DRY_RUN):
    command.insert(0, DUPLICACY_COMMAND)
    if logger.isEnabledFor(logging.DEBUG):
        command.insert(1, "-debug")
    logger.debug(f"runnig command {' '.join(command)}")
    if not dry_run:
        p = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=DUPLICACY_ENV,
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
    try:
        with open(STAMP_PATH, "r") as f:
            stamps = json.load(f)
    except IOError:
        stamps = {}
    return stamps


def save_stamp(source_dir):
    stamps = load_stamps()
    stamps[source_dir.as_posix()] = datetime.now().isoformat()
    STAMP_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(STAMP_PATH, "w") as f:
        json.dump(stamps, f)


def verify(source_dir):
    logger.info(f"starting verification for {source_dir}")

    # p = subprocess.Popen(
    #     [DUPLICACY_COMMAND, "list"],
    #     stdout=subprocess.PIPE,
    #     stderr=subprocess.PIPE,
    #     env=DUPLICACY_ENV,
    #     cwd=source_dir,
    # )
    # out, err = p.communicate()
    # if p.returncode != 0:
    #     logger.error(f"list failed for {source_dir}")
    #     return False

    out, _ = call_duplicacy(["list",], cwd=source_dir, dry_run=False)
    print(out)
    matches = re.findall(r"Snapshot (\S+) revision (\d+)", out)
    snapshot_id, latest_rev = matches[-1]

    logger.info(f"verifying snapshot {snapshot_id} revision {latest_rev}")
    try:
        call_duplicacy(
            ["check", "-chunks", "-r", latest_rev, "-id", snapshot_id,], cwd=source_dir,
        )
    except subprocess.CalledProcessError:
        logger.error(f"verification failed for {source_dir}")
        return False

    save_stamp(source_dir)

    return True


def check(source_dir):
    logger.info(f"starting check for {source_dir}")
    try:
        call_duplicacy([DUPLICACY_COMMAND, "check"], cwd=source_dir)
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
    parser.add_argument("--force-verification", action="store_true")

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

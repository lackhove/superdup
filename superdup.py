#!/usr/bin/python3
# -*- coding: utf-8 -*-

#  Copyright 2018 Kilian Lackhove
#
#  nektar-tools is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  nektar-tools is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with nektar-tools.  If not, see <http://www.gnu.org/licenses/>.


import os
import sys
import re
import argparse
import subprocess
import json
import datetime
import dateutil.parser

DUPLICACY_COMMAND = "/usr/local/bin/duplicacy"
if not os.path.isfile(DUPLICACY_COMMAND):
    DUPLICACY_COMMAND = "/usr/bin/duplicacy"
STAMP_PATH = "/root/.duplicacy_stamps"

CONFIG = {
    "DUPLICACY_PASSWORD": "topsecret",
    "DUPLICACY_B2_ID": "secret",
    "DUPLICACY_B2_KEY": "supersecret",
}

args = None


def main():
    parser = argparse.ArgumentParser(description="perform duplicacy backups, prune and checks")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--verify-force", action="store_true")
    parser.add_argument("source_dir", type=str, nargs="+", help="source dir")

    global args
    args = parser.parse_args()

    if not testOnline():
        print("--- ERROR: not online exiting", flush=True)
        sys.exit(-1)

    retval = True

    for sd in args.source_dir:
        print("--- procesing {}".format(sd), flush=True)

        if not os.path.isdir(sd):
            print(
                "--- ERROR:  source dir {} does not exist, skipping".format(sd),
                flush=True,
            )
            retval = False
            continue

        retval = backup(sd) and retval
        retval = prune(sd) and retval
        retval = verify(sd) and retval

    if not retval:
        sys.exit(-1)


def testOnline():
    return True


def wrapDuplicacy(command, cwd):
    if args.debug:
        command.insert(1, "-debug")
        print("runnig command {}".format(command))
    p = subprocess.Popen(
        command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=CONFIG, cwd=cwd
    )
    while p.poll() is None:
        print(p.stdout.readline().decode("utf-8").rstrip(), flush=True)
    out, err = p.communicate()
    if p.returncode:
        raise subprocess.CalledProcessError(p.returncode, err)


def backup(source_dir):

    print("--- starting backup for {}".format(source_dir), flush=True)
    try:
        wrapDuplicacy([DUPLICACY_COMMAND, "backup", "-stats"], cwd=source_dir)
    except subprocess.CalledProcessError:
        print("--- ERROR: backup failed for {}".format(source_dir), flush=True)
        return False
    print("--- SUCCESS", flush=True)
    return True


def prune(source_dir):

    print("--- starting prune for {}".format(source_dir), flush=True)
    try:
        wrapDuplicacy(
            [
                DUPLICACY_COMMAND,
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
        print("--- ERROR: prune failed for {}".format(source_dir), flush=True)
        return False
    print("--- SUCCESS", flush=True)
    return True


def verify(source_dir):

    rightNow = datetime.datetime.now()

    if not os.path.exists(STAMP_PATH):
        stamps = {}
    else:
        with open(STAMP_PATH, "r") as f:
            stamps = json.load(f)

    verify = False
    if source_dir in stamps:
        lastVerify = dateutil.parser.parse(stamps[source_dir])
        timediff = rightNow - lastVerify
        print("--- last verification was {} ago".format(timediff), flush=True)
        if timediff > datetime.timedelta(days=90):
            verify = True
    else:
        print("--- no previous verification found", flush=True)
        verify = True

    if args.verify_force:
        print("--- Enforcing verification")
        verify = True

    if verify:
        print("--- starting verification for {}".format(source_dir), flush=True)
        p = subprocess.Popen(
            [DUPLICACY_COMMAND, "list"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=CONFIG,
            cwd=source_dir,
        )
        out, err = p.communicate()
        if p.returncode:
            print("--- ERROR: list failed for {}".format(source_dir), flush=True)
            return False
        matches = re.findall(r"Snapshot (\S+) revision (\d+)", out.decode("utf-8"))
        snapshot_id, latestRev = matches[-1]

        # duplicacy check -chunks -r 3
        print(
            "--- verifying snapshot {} revision {}".format(snapshot_id, latestRev),
            flush=True,
        )
        command = [
            DUPLICACY_COMMAND,
            "check",
            "-chunks",
            "-r",
            latestRev,
            "-id",
            snapshot_id,
        ]
        try:
            wrapDuplicacy(command, cwd=source_dir)
        except subprocess.CalledProcessError:
            print(
                "--- ERROR: verification failed for {}".format(source_dir), flush=True
            )
            return False

        stamps[source_dir] = rightNow.isoformat()
        with open(STAMP_PATH, "w") as f:
            json.dump(stamps, f)
        print("--- SUCCESS", flush=True)

        return True

    else:

        print("--- starting check for {}".format(source_dir), flush=True)
        try:
            wrapDuplicacy([DUPLICACY_COMMAND, "check"], cwd=source_dir)
        except subprocess.CalledProcessError:
            print("--- ERROR: check failed for {}".format(source_dir), flush=True)
            return False
        print("--- SUCCESS", flush=True)
        return True


if __name__ == "__main__":
    main()

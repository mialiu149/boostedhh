"""
Checks that there is an output for each job submitted.

Author: Raghav Kansal
"""

from __future__ import annotations

import argparse
import os
from os import listdir
from pathlib import Path

import numpy as np

from boostedhh import utils
from boostedhh.submit_utils import print_red, replace_batch_size

parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument(
    "--processor",
    help="which processor",
    type=str,
    required=True,
)

parser.add_argument(
    "--analysis", required=True, choices=["bbbb", "bbtautau"], help="which analysis", type=str
)

parser.add_argument(
    "--site", default="lpc", choices=["lpc", "ucsd"], help="t2 site we are checking", type=str
)
parser.add_argument("--tag", default="", help="tag for jobs", type=str)
parser.add_argument("--year", help="year", type=str, required=True)
parser.add_argument("--change-batch-size", help="Change batch size for failed jobs - primarily in case the jobs are failing because of memory issues", type=int, default=None)
parser.add_argument("--user", default="rkansal", help="user", type=str)
utils.add_bool_arg(parser, "submit-missing", default=False, help="submit missing files")
utils.add_bool_arg(parser, "print-shell", default=False, help="print .sh files as well")
utils.add_bool_arg(
    parser,
    "check-running",
    default=False,
    help="check against running jobs as well (running_jobs.txt will be updated automatically)",
)

args = parser.parse_args()


cmspath = {
    "lpc": "/eos/uscms/",
    "ucsd": "/ceph/cms/",
}[args.site]

xrddir = (
    f"{cmspath}/store/user/{args.user}/{args.analysis}/{args.processor}/{args.tag}/{args.year}/"
)

samples = listdir(xrddir)
jdls = [jdl for jdl in listdir(f"condor/{args.processor}/{args.tag}/") if jdl.endswith(".jdl")]

jdl_dict = {}
for sample in samples:
    x = [
        int(jdl[:-4].split("_")[-1])
        for jdl in jdls
        if jdl.split("_")[0] == args.year and "_".join(jdl.split("_")[1:-1]) == sample
    ]
    if len(x) > 0:
        jdl_dict[sample] = np.sort(x)[-1] + 1

"""
jdl_dict = {
    sample: np.sort(
        [
            int(jdl[:-4].split("_")[-1])
            for jdl in jdls
            if jdl.split("_")[0] == args.year and "_".join(jdl.split("_")[1:-1]) == sample
        ]
    )[-1]
    + 1
    for sample in samples
}
"""


running_jobs = []
if args.check_running:
    os.system("condor_q | awk '{print $9}' > running_jobs.txt")
    with Path("running_jobs.txt").open() as f:
        lines = f.readlines()

    running_jobs = [s[:-4] for s in lines if s.endswith(".sh\n")]


missing_files = []
err_files = []


for sample in samples:
    print(f"Checking {sample}")

    if args.processor != "trigger":
        # add all files if entire parquet directory is missing
        if not Path(f"{xrddir}/{sample}/parquet").exists():
            print_red(f"No parquet directory for {sample}!")
            if sample not in jdl_dict:
                continue

            for i in range(jdl_dict[sample]):
                if f"{args.year}_{sample}_{i}" in running_jobs:
                    print(f"Job #{i} for sample {sample} is running.")
                    continue

                jdl_file = f"condor/{args.processor}/{args.tag}/{args.year}_{sample}_{i}.jdl"
                err_file = f"condor/{args.processor}/{args.tag}/logs/{args.year}_{sample}_{i}.err"
                print(jdl_file)
                missing_files.append(jdl_file)
                err_files.append(err_file)
                if args.submit_missing:
                    os.system(f"condor_submit {jdl_file}")

            continue

        num_batches = [f for f in listdir(f"{xrddir}/{sample}/jobchecks") if "num_batches" in f]

        expected_parquets = {}
        for f in num_batches:
            with Path(f"{xrddir}/{sample}/jobchecks/{f}").open() as file:
                bnum = file.readlines()

            fnum = int(f.split("_")[2].split(".")[0])  # remove .txt
            expected_parquets[fnum] = int(bnum[0])

        outs_parquet = {}
        for out in listdir(f"{xrddir}/{sample}/parquet"):
            fnum = int(out.split("_")[1])
            if fnum not in outs_parquet:
                outs_parquet[fnum] = []

            bnum = int(out.split("_")[3].split(".")[0])  # remove .parquet
            outs_parquet[fnum].append(bnum)

        pouts_parquet = [f"{fnum}-{list(bnum)[-1]}" for fnum, bnum in outs_parquet.items()]
        print(f"Out parquets: {pouts_parquet}")

    if not Path(f"{xrddir}/{sample}/pickles").exists():
        print_red(f"No pickles directory for {sample}!")
        continue

    outs_pickles = [
        int(out.split(".")[0].split("_")[-1]) for out in listdir(f"{xrddir}/{sample}/pickles")
    ]

    if args.processor == "trigger":
        print(f"Out pickles: {outs_pickles}")

    for i in range(jdl_dict[sample]):
        check_pickles = i in outs_pickles
        check_parquet = True
        if args.processor != "trigger":
            if i not in outs_parquet:
                check_parquet = False
            else:
                missing_batches = [
                    j for j in range(expected_parquets[i]) if j not in outs_parquet[i]
                ]
                check_parquet = len(missing_batches) == 0

        if not check_pickles or not check_parquet:
            if f"{args.year}_{sample}_{i}" in running_jobs:
                print(f"Job #{i} for sample {sample} is running.")
                continue

            if not check_pickles:
                print_red(f"Missing output pickle #{i} for sample {sample}")

            if not check_parquet:
                if i not in outs_parquet:
                    print_red(f"Missing all output parquets for job #{i} for sample {sample}")
                else:
                    print_red(f"Missing batches {missing_batches} for job #{i} for sample {sample}")

            jdl_file = f"condor/{args.processor}/{args.tag}/{args.year}_{sample}_{i}.jdl"
            err_file = f"condor/{args.processor}/{args.tag}/logs/{args.year}_{sample}_{i}.err"
            missing_files.append(jdl_file)
            err_files.append(err_file)
            if args.submit_missing:
                os.system(f"condor_submit {jdl_file}")


print(f"{len(missing_files)} files to re-run:")
for f in missing_files:
    print(f)

if args.print_shell:
    print(f"\n{len(missing_files)} bash files:")
    for f in missing_files:
        print(f.replace(".jdl", ".sh"))

if args.change_batch_size is not None:
    print(f"\nChanging the batch size to {args.change_batch_size} in the following files:")

    for f in missing_files:
        shfile = Path(f.replace(".jdl", ".sh"))
        print(shfile)
        replace_batch_size(shfile, args.change_batch_size)
    
print("\nError files:")
for f in err_files:
    print(f)
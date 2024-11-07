"""
Create a JSON list of files of privately produced NanoAOD files.

Author: Raghav Kansal
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from XRootD import client

from boostedhh import hh_vars, utils


def _dirlist(fs, path) -> list:
    status, listing = fs.dirlist(str(path))
    if not status.ok:
        raise FileNotFoundError(f"Failed to list directory: {status}")

    return [f.name for f in listing]


def xrootd_index_private_nano(
    base_dir: str,
    redirector: str = "root://cmseos.fnal.gov/",
    users: list[str] = None,
    years: list[str] = None,
    samples: list[str] = None,
    subsamples: list[str] = None,
    files: dict[str] = None,
) -> list:
    """Recursively search for privately produced NanoAOD files via XRootD.

    Can specify specific users, years, samples, and subsamples to search for;
    otherwise, it will search for all by default.

    Files are organized as:

    MC:
    ......redirector.......|...............base_dir....................|..user.|year|sample|....................................subsample.......................................|
    root://cmseos.fnal.gov//store/user/lpcdihiggsboost/NanoAOD_v12_ParT/rkansal/2022/HHbbtt/GluGlutoHHto2B2Tau_kl-1p00_kt-1p00_c2-0p00_LHEweights_TuneCP5_13p6TeV_powheg-pythia8/
    .............................f1...........................|.....f2......|.f3.|......
    GluGlutoHHto2B2Tau_kl-1p00_kt-1p00_c2-0p00_TuneCP5_13p6TeV/241028_235514/0000/*.root

    Data:
    TODO
    """
    fs = client.FileSystem(redirector)
    base_dir = Path(base_dir)

    users = _dirlist(fs, base_dir) if users is None else users
    years = hh_vars.years if years is None else years

    if files is None:
        files = {}

    for user in users:
        print(f"\t{user}")
        for year in years:
            print(f"\t\t{year}")
            if year not in files:
                files[year] = {}

            ypath = base_dir / user / year
            tsamples = _dirlist(fs, ypath) if samples is None else samples
            for sample in tsamples:
                if sample not in files[year]:
                    files[year][sample] = {}

                print(f"\t\t\t{sample}")
                spath = ypath / sample
                tsubsamples = _dirlist(fs, spath) if subsamples is None else subsamples
                for subsample in tsubsamples:
                    if subsample in files[year][sample]:
                        # Change to warning?
                        raise ValueError(f"Duplicate subsample found! {subsample}")

                    print(f"\t\t\t\t{subsample}")
                    sspath = spath / subsample
                    for f1 in _dirlist(fs, sspath):
                        f1path = sspath / f1
                        for f2 in _dirlist(fs, f1path):
                            f2path = f1path / f2
                            for f3 in _dirlist(fs, f2path):
                                f3path = f2path / f3
                                tfiles = [
                                    f"{redirector}{f3path!s}/{f}"
                                    for f in _dirlist(fs, f3path)
                                    if f.endswith(".root")
                                ]

                    files[year][sample][subsample] = tfiles
                    print(f"\t\t\t\t\t{len(tfiles)} files")

    return files


def main():
    # Set up argument parser
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--out-name",
        type=str,
        default="index",
        help="Output JSON name (year and .json will automatically be appended)",
    )

    utils.add_bool_arg(
        parser, "append", "Append to existing JSON file versus overwriting it", default=True
    )

    parser.add_argument(
        "--redirector",
        type=str,
        default="root://cmseos.fnal.gov/",
        help="Base XRootD redirector",
    )

    parser.add_argument(
        "--base-dir",
        type=str,
        default="/store/user/lpcdihiggsboost/NanoAOD_v12_ParT",
        help="Base directory for XRootD search",
    )

    parser.add_argument(
        "--users",
        nargs="+",
        type=str,
        help="Which users' directories. By default searches all.",
        default=None,
    )

    parser.add_argument(
        "--years",
        nargs="+",
        type=str,
        help="Which years to index. By default searches all.",
        default=hh_vars.years,
    )

    parser.add_argument(
        "--samples",
        nargs="+",
        type=str,
        help="Which samples to index. By default searches all.",
        default=None,
    )

    parser.add_argument(
        "--subsamples",
        nargs="+",
        type=str,
        help="Which subsamples to index. By default searches all.",
        default=None,
    )

    args = parser.parse_args()

    if args.append:
        # check if output file exists for each year; if so, load and save to files dict.
        files = {}
        for year in args.years:
            try:
                with Path(f"{args.out_name}_{year}.json").open() as f:
                    files[year] = json.load(f)
            except FileNotFoundError:
                continue
    else:
        files = None

    files = xrootd_index_private_nano(
        args.base_dir,
        args.redirector,
        args.users,
        args.years,
        args.samples,
        args.subsamples,
        files,
    )

    # save files per year
    for year in files:
        with Path(f"{args.out_name}_{year}.json").open("w") as f:
            json.dump(files[year], f, indent=4)


if __name__ == "__main__":
    main()

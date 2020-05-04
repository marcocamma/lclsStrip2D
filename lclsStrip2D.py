#!/usr/bin/env python
""" Copy LCLS hdf5 file skipping large data chunks [like CsPad & Camera data (by default)]"""

from __future__ import print_function
import glob
import time
import os
import h5py as h5
import argparse
import sys
import re

__author__ = "Marco Cammarata"
__copyright__ = "Copyright 2015, Marco Cammarata"
__license__ = "GPL"
__version__ = "1.0.1"
__maintainer__ = "Marco Cammarata"
__email__ = "marcocamma@gmail.com"
__status__ = "Development"


def getCMD(cmd, strip=True):
    shell = os.popen(cmd)
    ret = shell.readlines()
    shell.close()
    if strip:
        ret = [x.strip() for x in ret]
    return ret


class DatasetList(object):
    def __init__(self, skipList=["CsPad", "Camera"]):
        if type(skipList) == str:
            skipList = [skipList]
        self.data = []
        self.skip = re.compile("|".join(skipList))
        if skipList is not None:
            self.addDataset = self.addDatasetWithSkip
        else:
            self.addDataset = self.addDatasetWithOutSkip

    def addDatasetWithSkip(self, name, h5handle):
        # print(name,self.skip.search(name))
        if isinstance(h5handle, h5.Dataset) and (self.skip.search(name) is None):
            self.data.append(h5handle)

    def addDatasetWithOutSkip(self, name, h5handle):
        if isinstance(h5handle, h5.Dataset):
            self.data.append(h5handle)

    def clear(self):
        self.data = []


def stripFile(h5name, outputFolder="./", skipList="CsPad", force=False):
    h5OutName = (
        outputFolder
        + "/"
        + os.path.splitext(os.path.basename(h5name))[0]
        + ".stripped.h5"
    )
    if os.path.exists(h5OutName) and not force:
        return
    H_IN = h5.File(h5name, "r")
    H_OUT = h5.File(h5OutName, "w")
    data = DatasetList(skipList=skipList)
    t0 = time.time()
    print("Analyzing file ...", end="")
    sys.stdout.flush()
    H_IN.visititems(data.addDataset)
    print("done (%.1f sec)" % (time.time() - t0))
    for d in data.data:
        try:
            H_OUT.copy(d, d.name)
        except RuntimeError:
            print("failed to copy %s" % d.name)
    H_IN.close()
    H_OUT.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Strip CsPad and Opal data from LCLS hdf5 files",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--input",
        required=True,
        help="input file or folder; if folder all *.h5 will be processed",
    )
    parser.add_argument(
        "--out_folder", default="./", help="where to put new files, default, CWD"
    )
    parser.add_argument(
        "--skip_list",
        default="CsPad,Camera",
        help="comma separated list of strings to look for to skip dataset/group (default: %(default)s)",
    )
    parser.add_argument(
        "--force", action="store_true", help="do it even if files exists already"
    )
    args = parser.parse_args()
    if os.path.isdir(args.input):
        files = glob.glob("%s/*.h5" % args.input)
        files = sorted(files)
        files = [f for f in files if "stripped.h5" not in os.path.basename(f)]
    else:
        files = (args.input,)
    for f in files:
        try:
            stripFile(
                f,
                outputFolder=args.out_folder,
                skipList=args.skip_list.split(","),
                force=args.force,
            )
        except Exception as e:
            print("failed to process %s" % f,"error was",str(e))

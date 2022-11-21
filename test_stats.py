#!/usr/bin/env python2
from __future__ import print_function

import argparse
import gfal2

from random import sample
from os.path import basename, dirname, realpath
from time import time


class Dump:
    def __init__(self, path):
        self.path = path
        self.nlines = None
        
    def _count_lines(self):
        with open(self.path) as fd:
            for idx, line in enumerate(fd):
                pass
        self.nlines = idx 

    @property
    def ready(self):
        return self.nlines is not None

    def prepare(self):
        self._count_lines()

    def random_lines(self, line_count):
        if not self.ready:
            self.prepare()

        if line_count > self.nlines:
            raise ValueError("Requested more {0} lines, while there is only {1} in {2}".format(line_count, self.nlines, self.path))

        selected = sample(range(self.nlines), line_count)

        res = []
        with open(self.path) as fd:
            for idx, line in enumerate(fd):
                if idx in selected:
                   res.append(line)
        return res


def run_stats(dump, count, output_dir='.'):
    output_urls = "{0}/urls_{1}.csv".format(realpath(output_dir), basename(dump.path))
    output_chunks = "{0}/chunks_{1}.csv".format(realpath(output_dir), basename(dump.path))
    URLs = dump.random_lines(count)
    ctx = gfal2.creat_context()
    url_times = []
    chunk_res = 0
    print("Starting stats for {0} at {1}".format(dump.path, time()))
    chunk_start = time()
    for idx, url in enumerate(URLs):
        url = url.strip()
        url_start = time() 
        try:
            stat_res = ctx.stat(url)
        except gfal2.GError as e:
            print("Exception while stat {0}: {1}".format(url, e))
            res = 1
        else:
            res = 0 
        finally:
            url_end = time()
            url_times.append( (url_start, url_end-url_start, res) )
            chunk_res = max(chunk_res, res)
        if (idx+1) % 50 == 0:
            print("{0} urls processed".format(idx+1))
    chunk_time = url_end - chunk_start
    print("Chunk processed in {0}".format(chunk_time))

    for fname, res in ( (output_urls, url_times), (output_chunks, [(chunk_start, chunk_time, chunk_res)]) ):
        with open(fname, 'a') as fd:
            for tup in res:
                print("{0},{1},{2}".format(tup[0],tup[1],tup[2]), file=fd)


def run_dirac_checks(dump, prefix, count, se, output_dir='.'):
    output_file = "{0}/chunks_{1}.csv".format(realpath(output_dir), basename(dump.path))
    URLs = [u.replace(prefix, '', 1) for u in  dump.random_lines(count)]
    replicas = {}
    for url in URLs:
        replicas[url] = [se]

    from LHCbDIRAC.DataManagementSystem.Client.DataIntegrityClient import DataIntegrityClient
    client = DataIntegrityClient()

    print("Starting stats for {0} at {1}".format(dump.path, time()))
    chunk_start = time()
    res = client.checkPhysicalFiles(replicas=replicas, catalogMetadata={})
    chunk_end = time()

    with open(output_file, 'a') as fd:
        print( "{0},{1},{2}".format(chunk_start, chunk_end - chunk_start, 0 if res['OK'] else 1) )


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--count", help="Number of files to stat. Must not be less then number of lines in a dump", type=int)
    parser.add_argument("-d", "--dumps", help="Comma-separated list of files with URLs for stats. No commas in filenames, please")
    parser.add_argument("-o", "--output_dir", help="Dir where results should be stored")
    parser.add_argument("-D", "--DIRAC", help="Run DIRAC-specific checks", action='store_true')
    parser.add_argument("-S", "--SE", help="Dirac SE. Required only for DIRAC-specific tests", default=None)
    parser.add_argument("-p", "--prefix", help="Prefix to remove from PFN to get LFN. Required only for DIRAC-Specific tests", default=None)
    args = parser.parse_args()
    if args.DIRAC and (args.SE is None or args.prefix is None):
        raise ValueError("For DIRAC-specific tests SE and prefix must be given.")
    elif not args.DIRAC and (args.SE is not None or args.prefix is not None):
        raise ValueError("For non DIRAC-specific tests SE and prefix must not be given.")
    return args


if __name__ == '__main__':
    args = parse_args()
    Dumps = []
    for fname in args.dumps.split(','):
        Dumps.append(Dump(fname))

    for dump in Dumps:
        if args.DIRAC:
            run_dirac_checks(dump, args.prefix, args.count, args.SE)
        else:
            run_stats(dump, args.count, output_dir=args.output_dir)

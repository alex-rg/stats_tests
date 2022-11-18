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
    data = { 'url': [], 'chunks': [] }
    chunk_start = time()
    chunk_res = 0
    for url in URLs:
        url_start = time() 
        try:
            stat_res = ctx.stat(url)
        except gfal2.GError:
            res = 1
        else:
            res = 0 
        finally:
            url_end = time()
            data['url'].append( (url_start, url_end-url_start, res) )
            chunk_res = max(chunk_res, res)
    data['chunks'].append( (chunk_start, url_end - chunk_start, chunk_res) )

    for fname, res in ( (output_urls, data['url']), (output_chunks, data['chunks']) ):
        with open(fname, 'a') as fd:
            for tup in res:
                print("{0},{1},{2}".format(tup[0],tup[1],tup[2]), file=fd)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--count", help="Number of files to stat. Must not be less then number of lines in a dump", type=int)
    parser.add_argument("-d", "--dumps", help="Comma-separated list of files with URLs for stats. No commas in filenames, please")
    parser.add_argument("-o", "--output_dir", help="Dir where results should be stored")
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    Dumps = []
    for fname in args.dumps.split(','):
        Dumps.append(Dump(fname))

    for dump in Dumps:
        run_stats(dump, args.count, output_dir=args.output_dir)

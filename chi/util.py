from joblib import Parallel, delayed
import pandas
import time
import argparse

class BatchParRun:
    def iterate(self, start=0, stop=None):
        raise NotImplementedError()
    
    def execute_one(self, arg):
        raise NotImplementedError()

    def iteration_count(self):
        raise NotImplementedError()
    
    def single(self, result):
        return [result]
    
    def multiple(self, result):
        return result

    def run_parallel(self, n_jobs=-1, start=0, stop=None):
        results = Parallel(n_jobs=n_jobs, verbose=10)(delayed(self.execute_one)(arg) for arg in self.iterate(start, stop))
        results = pandas.DataFrame.from_records([r for res in results for r in res])
        return results

    def run_from_args(self, args):
        start = args.batch_start
        stop = self.iteration_count()
        if args.batch_count > -1:
            stop = min(start + args.batch_count, stop)

        self.run_parallel(n_jobs=args.jobs, start=start, stop=stop)

    @classmethod
    def update_parser(cls, parser):
        parser.add_argument("--batch_start", default=0, type=int)
        parser.add_argument("--batch_count", default=-1, type=int)
        parser.add_argument("--jobs", default=-1, type=int)


class DFBatchParRun(BatchParRun):
    def __init__(self, df, group_key=None):
        self.df = df
        self.group_key = group_key
        if self.group_key:
            self.grouped = self.df.groupby(self.group_key)
            self.group_names = list(self.grouped.groups.keys())

    def iteration_count(self):
        if self.group_key is None:
            return self.df.shape[0]
        else:
            return len(self.group_names)

    def iterate(self, start=0, stop=None):
        if self.group_key is None:
            rowiter = self.df.iloc[slice(start, stop)].iterrows()
            yield from rowiter
        else:
            group_names = self.group_names[slice(start, stop)]
            for gname in group_names:
                tab = self.grouped.get_group(gname)
                yield gname, tab


    


class Tic:
    def __init__(self):
        self.tic()

    def get_time(self):
        return time.perf_counter_ns()

    def process_diff(self, diff):
        return diff / 1e9

    def tic(self):
        self._last = self.get_time()
    
    def toc(self):
        diff = self.get_time() - self._last
        return self.process_diff(diff)


def make_parser(f=None):
    parser = argparse.ArgumentParser()
    if f:
        f(parser)
    subparsers = parser.add_subparsers()
    return parser, subparsers


class _EntryPoint:
    def __init__(self, f):
        self.f = f
        self._parser = None
        self.name = f.__name__

        f.parser = self.parser


    def prepare_parser(self, parser, subparsers):
        parser = subparsers.add_parser(self.name)
        if self._parser:
            self._parser(parser)

        parser.set_defaults(cmd=self.f)

    def parser(self, f):
        self._parser = f
        return f

class EntryPoints:
    def __init__(self):
        self.entrypoints = []
        self.parser_functions = []

    def common_parser(self, parser):
        for pf in self.parser_functions:
            pf(parser)

    def point(self, f):
        ep =  _EntryPoint(f)
        self.entrypoints.append(ep)
        return f

    def add_common_parser(self, f):
        self.parser_functions.append(f)
        return f
    

    def parse_args(self):
        parser, subparsers = make_parser(self.common_parser)
        for ep in self.entrypoints:
            ep.prepare_parser(parser, subparsers)

        args = parser.parse_args()
        return args

    def main(self):
        args = self.parse_args()
        tic = Tic()
        args.cmd(args)
        tdiff = tic.toc()
        print(f"Ran in {tdiff:0.05f} seconds")

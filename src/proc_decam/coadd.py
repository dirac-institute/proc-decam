"""
Aggregate over available input warps to make a coadd

This should select input warps with the where query and collections
Associate those into {coadd_name}/warps
Make a new chained collection {coadd_name} and execute.py on the input
"""
import parsl
from parsl import bash_app
from parsl.executors import HighThroughputExecutor
from functools import partial
from parsl import run_command, EpycProvider, KloneAstroProvider, KloneA40Provider
from subprocess import Popen, PIPE
import selectors
import sys
import atexit

processes = []

def cleanup():
    for p in processes:
        p.kill()

atexit.register(cleanup)

def popen(*args, **kwargs):
    global processes
    p = Popen(*args, **kwargs)
    # p = Popen("echo", **kwargs)
    print("popen: " + " ".join(*args), file=sys.stderr)
    processes.append(p)
    return p

def _print(p):
    sel = selectors.DefaultSelector()
    sel.register(p.stdout, selectors.EVENT_READ)
    sel.register(p.stderr, selectors.EVENT_READ)

    while True:
        for key, _ in sel.select():
            data = key.fileobj.read1().decode()
            if not data:
                return p
            if key.fileobj is p.stdout:
                print(data, end="")
            else:
                print(data, end="", file=sys.stderr)

def run_and_pipe(*args, **kwargs):
    if 'stdout' not in kwargs:
        kwargs['stdout'] = PIPE
    if 'stderr' not in kwargs:
        kwargs['stderr'] = PIPE
    p = popen(*args, **kwargs)
    return _print(p)

pipeline_lookup = {
    "mean": "mean-template.yaml",
    "median": "median-template.yaml",
    "meanclip": "meanclip-template.yaml",
    "min": "min-template.yaml",
    "": "template.yaml",
    None: "template.yaml",
}

def main():
    # loop over subsets?
    import argparse
    import os

    parser = argparse.ArgumentParser()
    parser.add_argument("repo")
    parser.add_argument("coadd_name")
    parser.add_argument("--template-type", default="")
    parser.add_argument("--coadd-subset", default="")
    parser.add_argument("--where")
    parser.add_argument("--collections", nargs="+", default=[])
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--slurm", action="store_true")
    parser.add_argument("--pipeline-slurm", action="store_true")
    parser.add_argument("--provider", default="EpycProvider")
    parser.add_argument("--workers", "-J", type=int, default=4)
    
    args = parser.parse_args()

    htex_label = "htex"
    executor_kwargs = dict()
    
    if args.slurm:
        provider = KloneA40Provider(max_blocks=args.workers)
    else:
        provider = EpycProvider(max_blocks=1)
        executor_kwargs = dict(
            max_workers_per_node=args.workers
        )
    
    executor_kwargs['provider'] = provider
    config = parsl.Config(
        executors=[
            HighThroughputExecutor(
                label=htex_label,
                **executor_kwargs,
            )
        ],
        run_dir=os.path.join("runinfo", "coadd"),
    )
    parsl.load(config)

    futures = [] # chage to dictionary
    inputs = []
    cmd = [
        "proc-decam",
        "collection",
        args.repo,
        "coadd",
        args.coadd_name,
    ]
    if args.template_type:
        cmd += ["--template-type", args.template_type]
    if args.coadd_subset:
        cmd += ["--coadd-subset", args.coadd_subset]
    # cmd = " ".join(map(str, cmd))
    # print(cmd)
    # p = run_and_pipe(cmd)
    # p.wait()
    # if p.returncode != 0:
    #     raise RuntimeError("collection failed")
    cmd = " ".join(map(str, cmd))
    func = partial(run_command)
    setattr(func, "__name__", f"collection")
    future = bash_app(func)(cmd, inputs=inputs)
    inputs = [future]
    futures.append(future)
        
    cmd = [
        "proc-decam",
        "execute",
        args.repo,
        os.path.normpath(f"{args.coadd_name}/{args.coadd_subset}/coadd/{args.template_type}"),
        "--pipeline", f"{os.environ.get('PROC_DECAM_DIR')}/pipelines/{pipeline_lookup[args.template_type]}#assembleCoadd",
    ]
    if args.where:
        cmd += [f"--where \"{args.where}\""]
    # cmd = " ".join(map(str, cmd))
    # print(cmd)
    # p = run_and_pipe(cmd)
    # p.wait()
    # if p.returncode != 0:
    #     raise RuntimeError("execute failed")

    cmd = " ".join(map(str, cmd))
    func = partial(run_command)
    setattr(func, "__name__", f"execute_coadd")
    future = bash_app(func)(cmd, inputs=inputs)
    inputs = [future]
    futures.append(future)

    cmd = [
        "proc-decam",
        "collection",
        args.repo,
        "coadd",
        args.coadd_name,
    ]
    if args.template_type:
        cmd += ["--template-type", args.template_type]
    if args.coadd_subset:
        cmd += ["--coadd-subset", args.coadd_subset]
    # cmd = " ".join(map(str, cmd))
    # print(cmd)
    # p = run_and_pipe(cmd)
    # p.wait()
    # if p.returncode != 0:
    #     raise RuntimeError("collection failed")
    cmd = " ".join(map(str, cmd))
    func = partial(run_command)
    setattr(func, "__name__", f"collection")
    future = bash_app(func)(cmd, inputs=inputs)
    inputs = [future]
    futures.append(future)

    for future in futures:
        if future:
            future.exception()
    
    parsl.dfk().cleanup()


if __name__ == "__main__":
    main()

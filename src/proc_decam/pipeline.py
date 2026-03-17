"""
Runs a pipeline via bin/execute.py uniformly over a set of nights

The choice is:
- run locally in processes, e.g. Popen
- run via slurm submits

inputs: 
- nights: regex match against collections in the butler
- {bias, flat, science}
- steps: execute these steps in order
- where: data query

for night in nights:
    for step in steps:
        python ./bin/execute.py ./repo {night}/bias --pipeline ./pipelines/bias.yaml#{step} --where {where}

Perhaps this should be a parsl workflow that targets local resources or slurm? Because there is an implicit dependency graph
and a set of command line commands to run
"""
import logging
import os

logging.basicConfig()
logger = logging.getLogger(__name__)

pipelines = dict(
    bias="bias.yaml",
    flat="flat.yaml",
    science="science.yaml",
    drp="DRP.yaml",
    coadd={
        "mean": "mean-template.yaml",
        "median": "median-template.yaml",
        "meanclip": "meanclip-template.yaml",
        "min": "min-template.yaml",
        "": "template.yaml",
        None: "template.yaml",
    },
    diff_drp="DRP.yaml",
)

import parsl
from parsl import bash_app
from parsl.executors import HighThroughputExecutor
from .parsl import EpycProvider, KloneA40Provider, run_command
from functools import partial


def build_futures(repo, proc_type, subset, template_type="", coadd_subset="", steps=None, where=None, inputs=None):
    """
    Build Parsl futures for executing a pipeline over matching Butler collections.

    Adds tasks to the currently-loaded Parsl configuration without creating a new
    Parsl context. This allows callers (e.g. ``night`` or ``coadd``) to add these
    tasks to their own single Parsl pipeline rather than nesting a separate one.

    Parameters
    ----------
    repo : str
        Path to the Butler repository.
    proc_type : str
        Processing type key (e.g. ``'bias'``, ``'flat'``, ``'drp'``, ``'coadd'``).
    subset : str
        Night or coadd-subset identifier used to match Butler collections.
    template_type : str, optional
        Template type (e.g. ``'meanclip'``).  Defaults to ``''``.
    coadd_subset : str, optional
        Coadd subset identifier.  Defaults to ``''``.
    steps : list of str, optional
        Pipeline steps to execute (e.g. ``['step1', 'step2']``).
    where : str, optional
        Butler data query string passed to ``proc-decam execute``.
    inputs : list, optional
        Upstream Parsl futures that all tasks created here will depend on.

    Returns
    -------
    list
        Parsl futures representing the pipeline execution tasks, in dependency
        order.  Pass the last element as an upstream ``inputs`` dependency for
        any subsequent tasks.
    """
    import lsst.daf.butler as dafButler
    from lsst.daf.butler.registry import CollectionType
    import re

    if steps is None:
        steps = []
    if inputs is None:
        inputs = []

    butler = dafButler.Butler(repo)
    collections = butler.registry.queryCollections(
        re.compile(
            os.path.normpath(f"{subset}/{coadd_subset}/{template_type}/{proc_type}")
        ),
        collectionTypes=CollectionType.CHAINED,
    )

    pipeline_file = pipelines[proc_type]
    if proc_type == "coadd":
        pipeline_file = pipeline_file[template_type]

    futures = []
    for collection in collections:
        collection_subset = collection.split("/")[0]
        local_inputs = list(inputs)
        for step in steps:
            cmd = [
                "proc-decam",
                "collection",
                repo,
                proc_type,
                collection_subset,
            ]
            if template_type:
                cmd += ["--template-type", template_type]
            if coadd_subset:
                cmd += ["--coadd-subset", coadd_subset]
            cmd = " ".join(map(str, cmd))
            func = partial(run_command)
            setattr(func, "__name__", f"collection_{collection_subset}_{proc_type}_{step}")
            future = bash_app(func)(cmd, inputs=local_inputs)
            local_inputs = [future]
            futures.append(future)

            cmd = [
                "proc-decam",
                "execute",
                repo,
                collection,
                "--pipeline", f"{os.getcwd()}/pipelines/{pipeline_file}#{step}",
            ]
            if where:
                cmd += [f"--where \"{where}\""]
            cmd = " ".join(map(str, cmd))
            func = partial(run_command)
            setattr(func, "__name__", f"execute_{collection_subset}_{proc_type}_{step}")
            future = bash_app(func)(cmd, inputs=local_inputs)
            local_inputs = [future]
            futures.append(future)

            # update collection chain after each step so the final state is
            # correct even if a later step fails
            cmd = [
                "proc-decam",
                "collection",
                repo,
                proc_type,
                collection_subset,
            ]
            if template_type:
                cmd += ["--template-type", template_type]
            if coadd_subset:
                cmd += ["--coadd-subset", coadd_subset]
            cmd = " ".join(map(str, cmd))
            func = partial(run_command)
            setattr(func, "__name__", f"collection_{collection_subset}_{proc_type}_{step}")
            future = bash_app(func)(cmd, inputs=local_inputs)
            local_inputs = [future]
            futures.append(future)

    return futures


def main():
    import argparse
    import lsst.daf.butler as dafButler
    from lsst.daf.butler.registry import CollectionType
    import re

    if not os.path.exists(os.path.join(os.getcwd(), "pipelines")):
        raise RuntimeError("Cannot find directory 'pipelines' in the current working directory")

    parser = argparse.ArgumentParser(prog="proc-decam pipeline")
    parser.add_argument("repo")
    parser.add_argument("proc_type")
    parser.add_argument("subset") # to support coadd/diff_drp replace nights with subset and add template-type and coadd-subset as an option...
    parser.add_argument("--template-type", default="")
    parser.add_argument("--coadd-subset", default="")
    parser.add_argument("--steps", nargs="+", default=[])
    parser.add_argument("--where")
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--slurm", action="store_true")
    parser.add_argument("--workers", "-J", type=int, default=4)

    args = parser.parse_args()
    
    logging.getLogger().setLevel(args.log_level)

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
        run_dir=os.path.join("runinfo", "pipeline"),
    )
    parsl.load(config)

    futures = build_futures(
        args.repo,
        args.proc_type,
        args.subset,
        template_type=args.template_type,
        coadd_subset=args.coadd_subset,
        steps=args.steps,
        where=args.where,
    )

    for future in futures:
        if future:
            future.exception()

    parsl.dfk().cleanup()

if __name__ == "__main__":
    main()


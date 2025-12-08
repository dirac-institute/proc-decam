
from typing import TYPE_CHECKING, List

import os

from lsst.ctrl.bps.parsl.configuration import get_bps_config_value
from lsst.ctrl.bps.parsl.sites import Local
from parsl.executors import HighThroughputExecutor
from parsl.executors.base import ParslExecutor
from parsl.providers import LocalProvider

__all__ = ("Epyc",)

class Epyc(Local):
    def get_executors(self) -> List[ParslExecutor]:    
        cores = int(os.environ.get("J", get_bps_config_value(self.site, "cores", int, required=True)))
        return [HighThroughputExecutor("local", provider=LocalProvider(), max_workers_per_node=cores)]

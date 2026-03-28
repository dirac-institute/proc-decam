"""
Tests that verify the unified-pipeline refactor.

The goal of the refactor is that ``pipeline.build_futures`` can be called
directly by higher-level modules (``night``, ``coadd``) instead of spawning
``proc-decam pipeline`` as a subprocess that itself starts a second Parsl
context.

These tests use ``unittest.mock`` to replace the LSST Butler and Parsl
bash_app so they can run without the full LSST Science Pipelines installed.
They confirm that:

1. ``pipeline.build_futures`` produces the expected sequence of shell commands
   (collection → execute → collection for every step).
2. ``night`` calls ``pipeline.build_futures`` directly rather than scheduling
   a ``proc-decam pipeline`` subprocess.
3. ``coadd`` calls ``pipeline.build_futures`` directly rather than scheduling
   a ``proc-decam pipeline`` subprocess.
"""

import os
import sys
import types
import unittest
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _install_lsst_stubs(collections):
    """
    Insert minimal LSST stubs into sys.modules so that modules that do
    ``import lsst.daf.butler as dafButler`` at function-call time will get
    an object whose ``Butler.registry.queryCollections`` returns *collections*.
    """
    lsst_mod = types.ModuleType("lsst")
    lsst_daf = types.ModuleType("lsst.daf")
    lsst_butler = types.ModuleType("lsst.daf.butler")
    lsst_registry = types.ModuleType("lsst.daf.butler.registry")
    lsst_daf.butler = lsst_butler
    lsst_mod.daf = lsst_daf

    class _Registry:
        def queryCollections(self, pattern, collectionTypes=None):
            return list(collections)

    class _Butler:
        def __init__(self, repo, writeable=False):
            self.registry = _Registry()

    lsst_butler.Butler = _Butler
    lsst_registry.CollectionType = MagicMock()

    stubs = {
        "lsst": lsst_mod,
        "lsst.daf": lsst_daf,
        "lsst.daf.butler": lsst_butler,
        "lsst.daf.butler.registry": lsst_registry,
    }
    return stubs


class _FakeFuture:
    """Minimal stand-in for a Parsl AppFuture."""

    def __init__(self, cmd, inputs=()):
        self.cmd = cmd
        self.inputs = list(inputs)

    def exception(self):
        return None


def _recording_bash_app(recorded_cmds):
    """Return a bash_app replacement that records submitted commands."""
    def bash_app(func):
        def wrapper(cmd, inputs=(), **kwargs):
            fut = _FakeFuture(cmd, inputs)
            recorded_cmds.append(cmd)
            return fut
        return wrapper
    return bash_app


# ---------------------------------------------------------------------------
# Test: pipeline.build_futures
# ---------------------------------------------------------------------------

class TestBuildFutures(unittest.TestCase):
    """Unit tests for ``pipeline.build_futures``."""

    def setUp(self):
        # Ensure a clean slate for each test so module-level state does not
        # bleed between tests.
        for key in list(sys.modules.keys()):
            if "proc_decam" in key:
                del sys.modules[key]

    def _call_build_futures(self, collections, proc_type, subset, **kwargs):
        """
        Call ``pipeline.build_futures`` with LSST and bash_app mocked out.
        Returns ``(futures, recorded_cmds)``.
        """
        recorded = []
        stubs = _install_lsst_stubs(collections)

        with patch.dict(sys.modules, stubs):
            import proc_decam.pipeline as pipeline_mod
            with (
                patch.object(pipeline_mod, "bash_app", _recording_bash_app(recorded)),
                patch("os.getcwd", return_value="/repo"),
            ):
                futures = pipeline_mod.build_futures(
                    repo="/repo",
                    proc_type=proc_type,
                    subset=subset,
                    **kwargs,
                )
        return futures, recorded

    # ------------------------------------------------------------------
    def test_step_command_pattern(self):
        """
        build_futures should schedule, per step:
          1. proc-decam collection
          2. proc-decam execute  (with the correct pipeline step)
          3. proc-decam collection   (post-step chain update)
        """
        futures, recorded = self._call_build_futures(
            ["20190401/bias"],
            proc_type="bias",
            subset="20190401",
            steps=["step1", "step2"],
        )

        self.assertEqual(len(futures), 6)  # 3 cmds × 2 steps

        for i, step in enumerate(["step1", "step2"]):
            base = i * 3
            self.assertIn("proc-decam collection", recorded[base])
            self.assertIn("proc-decam execute", recorded[base + 1])
            self.assertIn(f"bias.yaml#{step}", recorded[base + 1])
            self.assertIn("proc-decam collection", recorded[base + 2])

    def test_where_clause_appended_to_execute(self):
        """build_futures should append --where to proc-decam execute."""
        _, recorded = self._call_build_futures(
            ["20190401/bias"],
            proc_type="bias",
            subset="20190401",
            steps=["step1"],
            where="instrument='DECam'",
        )

        execute_cmd = next(c for c in recorded if "proc-decam execute" in c)
        self.assertIn("--where", execute_cmd)
        self.assertIn("instrument='DECam'", execute_cmd)

    def test_no_steps_returns_empty(self):
        """build_futures with no steps should return an empty list and no cmds."""
        futures, recorded = self._call_build_futures(
            ["20190401/bias"],
            proc_type="bias",
            subset="20190401",
            steps=[],
        )

        self.assertEqual(futures, [])
        self.assertEqual(recorded, [])

    def test_no_collections_returns_empty(self):
        """build_futures when no collection matches should return empty."""
        futures, recorded = self._call_build_futures(
            [],  # no collections
            proc_type="bias",
            subset="20190401",
            steps=["step1"],
        )

        self.assertEqual(futures, [])
        self.assertEqual(recorded, [])

    def test_template_type_in_collection_commands(self):
        """--template-type should be passed to every proc-decam collection cmd."""
        _, recorded = self._call_build_futures(
            ["2019/meanclip/coadd"],
            proc_type="coadd",
            subset="2019",
            template_type="meanclip",
            steps=["step3b"],
        )

        collection_cmds = [c for c in recorded if "proc-decam collection" in c]
        self.assertTrue(collection_cmds, "No collection cmds recorded")
        for cmd in collection_cmds:
            self.assertIn("--template-type meanclip", cmd)

    def test_coadd_subset_in_collection_commands(self):
        """--coadd-subset should be passed to every proc-decam collection cmd."""
        _, recorded = self._call_build_futures(
            ["20190401/2019/diff_drp"],
            proc_type="diff_drp",
            subset="20190401",
            coadd_subset="2019",
            steps=["step4a"],
        )

        collection_cmds = [c for c in recorded if "proc-decam collection" in c]
        self.assertTrue(collection_cmds, "No collection cmds recorded")
        for cmd in collection_cmds:
            self.assertIn("--coadd-subset 2019", cmd)

    def test_dependency_chain(self):
        """
        Each future's inputs should reference the previous future, forming a
        strict sequential chain within a single collection.
        """
        futures, _ = self._call_build_futures(
            ["20190401/bias"],
            proc_type="bias",
            subset="20190401",
            steps=["step1", "step2"],
        )

        # futures[1] (execute step1) should depend on futures[0] (collection)
        self.assertIn(futures[0], futures[1].inputs)
        # futures[2] (post-step collection) depends on futures[1]
        self.assertIn(futures[1], futures[2].inputs)
        # futures[3] (collection step2) depends on futures[2]
        self.assertIn(futures[2], futures[3].inputs)


# ---------------------------------------------------------------------------
# Shared helper for integration-style tests
# ---------------------------------------------------------------------------

def _make_recording_build_futures():
    """
    Return ``(spy_func, calls_list)`` where ``spy_func`` is a replacement for
    ``pipeline.build_futures`` that records every invocation in ``calls_list``
    and returns an empty future list.
    """
    calls = []

    def fake_build_futures(repo, proc_type, subset, **kwargs):
        calls.append({"repo": repo, "proc_type": proc_type, "subset": subset, **kwargs})
        return []

    return fake_build_futures, calls


# ---------------------------------------------------------------------------
# Test: night uses pipeline.build_futures directly
# ---------------------------------------------------------------------------

class TestNightUsesPipelineBuildFutures(unittest.TestCase):
    """
    Verify that ``night.main`` calls ``pipeline.build_futures`` directly
    rather than scheduling a ``proc-decam pipeline`` subprocess.
    """

    def setUp(self):
        for key in list(sys.modules.keys()):
            if "proc_decam" in key:
                del sys.modules[key]

    def test_bias_calls_build_futures_not_subprocess(self):
        import astropy.table

        exposures = astropy.table.Table({
            "night": [20190401],
            "obs_type": ["zero"],
        })

        fake_build_futures, build_futures_calls = _make_recording_build_futures()
        recorded_cmds = []

        stubs = _install_lsst_stubs([])

        with patch.dict(sys.modules, stubs):
            import proc_decam.pipeline as pipeline_mod
            import proc_decam.night as night_mod

            with (
                patch.object(pipeline_mod, "build_futures", fake_build_futures),
                patch.object(night_mod, "pipeline_module", pipeline_mod),
                patch.object(
                    night_mod, "bash_app", _recording_bash_app(recorded_cmds)
                ),
                patch.object(night_mod, "parsl") as mock_parsl,
                patch("astropy.table.Table.read", return_value=exposures),
                patch("os.getcwd", return_value="/repo"),
                patch.object(
                    sys, "argv",
                    ["proc-decam", "/repo", "/data/exposures.ecsv",
                     "--proc-types", "bias", "--nights", "20190401"],
                ),
            ):
                mock_parsl.Config = MagicMock()
                mock_parsl.load = MagicMock()
                mock_parsl.dfk = MagicMock(return_value=MagicMock())
                night_mod.main()

        # build_futures must have been called for bias
        bias_calls = [c for c in build_futures_calls if c["proc_type"] == "bias"]
        self.assertEqual(len(bias_calls), 1)
        self.assertEqual(bias_calls[0]["subset"], "20190401")
        self.assertIn("step1", bias_calls[0]["steps"])
        self.assertIn("step2", bias_calls[0]["steps"])

        # proc-decam pipeline must NOT appear in any recorded bash command
        for cmd in recorded_cmds:
            self.assertNotIn("proc-decam pipeline", cmd,
                             f"Found nested proc-decam pipeline call: {cmd!r}")


# ---------------------------------------------------------------------------
# Test: coadd uses pipeline.build_futures directly
# ---------------------------------------------------------------------------

class TestCoaddUsesPipelineBuildFutures(unittest.TestCase):
    """
    Verify that ``coadd.main`` calls ``pipeline.build_futures`` directly
    rather than scheduling a ``proc-decam pipeline`` subprocess.
    """

    def setUp(self):
        for key in list(sys.modules.keys()):
            if "proc_decam" in key:
                del sys.modules[key]

    def test_coadd_calls_build_futures_not_subprocess(self):
        fake_build_futures, build_futures_calls = _make_recording_build_futures()
        recorded_cmds = []
        stubs = _install_lsst_stubs([])

        with patch.dict(sys.modules, stubs):
            import proc_decam.pipeline as pipeline_mod
            import proc_decam.coadd as coadd_mod

            with (
                patch.object(pipeline_mod, "build_futures", fake_build_futures),
                patch.object(coadd_mod, "pipeline_module", pipeline_mod),
                patch.object(
                    coadd_mod, "bash_app", _recording_bash_app(recorded_cmds)
                ),
                patch.object(coadd_mod, "parsl") as mock_parsl,
                patch("os.getcwd", return_value="/repo"),
                patch.object(
                    sys, "argv",
                    ["proc-decam", "/repo", "2019*/drp",
                     "--coadd-subset", "2019",
                     "--template-type", "meanclip"],
                ),
            ):
                mock_parsl.Config = MagicMock()
                mock_parsl.load = MagicMock()
                mock_parsl.dfk = MagicMock(return_value=MagicMock())
                coadd_mod.main()

        coadd_calls = [c for c in build_futures_calls if c["proc_type"] == "coadd"]
        self.assertEqual(len(coadd_calls), 1)
        self.assertEqual(coadd_calls[0]["subset"], "2019")
        self.assertIn("step3b", coadd_calls[0]["steps"])
        self.assertIn("step3c", coadd_calls[0]["steps"])
        self.assertIn("step3d", coadd_calls[0]["steps"])

        # proc-decam pipeline must NOT appear in any recorded bash command
        for cmd in recorded_cmds:
            self.assertNotIn("proc-decam pipeline", cmd,
                             f"Found nested proc-decam pipeline call: {cmd!r}")


if __name__ == "__main__":
    unittest.main()


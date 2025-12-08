import lsst.daf.butler as dafButler
from lsst.pipe.base.quantum_graph_builder import QuantumGraphBuilder
from lsst.pipe.base.all_dimensions_quantum_graph_builder import AllDimensionsQuantumGraphBuilder
from lsst.pipe.base.pipeline_graph import PipelineGraph
from lsst.pipe.base.pipeline import Pipeline
from lsst.pipe.base.quantum_graph_skeleton import DatasetKey
import os
import argparse
import logging

logging.basicConfig()
logger = logging.getLogger(__name__)
logging.getLogger().setLevel(25)

class SkipFailuresQuantumGraphBuilder(AllDimensionsQuantumGraphBuilder):
    """
    Optionally skip tasks which have previously failed
    """
    def __init__(self, *args, skip_failures=True, **kwargs):
        self.skip_failures = skip_failures
        super().__init__(*args, **kwargs)
        
    def _skip_quantum_if_metadata_exists(self, task_node, quantum_key, skeleton):
        skip = super()._skip_quantum_if_metadata_exists(task_node, quantum_key, skeleton)
        if self.skip_failures:
            if not skip:
                # if the metadata does not exist, check that the log exists
                log_dataset_key = DatasetKey(
                    task_node.log_output.parent_dataset_type_name, quantum_key.data_id_values
                )
                # print(log_dataset_key)
                if log_dataset_key in self.existing_datasets.outputs_for_skip:
                    print("skipping", log_dataset_key)
                    # This quantum's metadata is already present in the the
                    # skip_existing_in collections; we'll skip it.  But the presence of
                    # the metadata dataset doesn't guarantee that all of the other
                    # outputs we predicted are present; we have to check.
                    for output_dataset_key in list(skeleton.iter_outputs_of(quantum_key)):
                        print("  removing output", output_dataset_key)
                        if (
                            output_ref := self.existing_datasets.outputs_for_skip.get(output_dataset_key)
                        ) is not None:
                            # Populate the skeleton graph's node attributes
                            # with the existing DatasetRef, just like a
                            # predicted output of a non-skipped quantum.
                            print("    replacing as output", output_ref)
                            skeleton[output_dataset_key]["ref"] = output_ref
                        else:
                            # Remove this dataset from the skeleton graph,
                            # because the quantum that would have produced it
                            # is being skipped and it doesn't already exist.
                            print("    removing dataset node", output_dataset_key)
                            skeleton.remove_dataset_nodes([output_dataset_key])
                        # If this dataset was "in the way" (i.e. already in the
                        # output run), it isn't anymore.
                        self.existing_datasets.outputs_in_the_way.pop(output_dataset_key, None)
                    # Removing the quantum node from the graph will happen outside this
                    # function.
                    return True
                # else:
                #     print("not skipping", log_dataset_key)
                return False    
        
        return skip
    
def create_qgraph(butler, pipeline_graph, input_collections, output_run=None, skip_failures=True, clobber=False, skip_existing_in=None, where=None):
    builder = SkipFailuresQuantumGraphBuilder(
        pipeline_graph,
        butler,
        where=where,
        output_run=output_run,
        input_collections=input_collections,
        skip_existing_in=skip_existing_in,
        skip_failures=skip_failures,
        clobber=clobber,
    )
    metadata = dict()
    if output_run:
        metadata["output_run"] = output_run

    qgraph = builder.build(metadata) 
    return qgraph   

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-b", "--butler-config")
    parser.add_argument("-p", "--pipeline")
    parser.add_argument("-i", "--input")
    parser.add_argument("--output-run")
    parser.add_argument("-d", "--data-query")
    parser.add_argument("--save-qgraph")
    parser.add_argument("--skip-existing-in")
    parser.add_argument("--skip-failures", action='store_true')

    args = parser.parse_args()
    pipeline = Pipeline.from_uri(args.pipeline)
    pipeline_graph = pipeline.to_graph()
    butler = dafButler.Butler(args.butler_config)
    qgraph = create_qgraph(
        butler, pipeline_graph, 
        args.input,
        where=args.data_query,
        output_run=args.output_run,
        skip_existing_in=args.skip_existing_in,
        skip_failures=args.skip_failures,
    )
    print("there are", len(qgraph), "tasks")
    if len(qgraph) == 0:
        raise Exception("quantum graph is empty")

if __name__ == "__main__":
    main()
    
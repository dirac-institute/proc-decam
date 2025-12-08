
import argparse
import logging

__all__ = ["query", "cli_query"]

logging.basicConfig()
logger = logging.getLogger(__name__)

def query(process_type, observation_type, outfields, **kwargs):
    query_search = [
        ["instrument", "decam"],
        ["telescope", "ct4m"],
        ["prod_type", "image"],
        ["proc_type", process_type],
        ["obs_type", observation_type],
    ]

    filters = []
    if kwargs.get('caldat'):
        _filter = ["caldat", kwargs.get('caldat'), kwargs.get('caldat')]
        logger.debug("adding filter: %s", _filter)
        filters.append(_filter)

    if observation_type in ["object", "dome flat"]:
        if kwargs.get('band'):
            _filter = ["ifilter", kwargs.get('band'), "startswith"]
            logger.debug("adding filter: %s", _filter)
            filters.append(_filter)

    for key in ["proposal", "OBJECT"]:
        args_value = kwargs.get(key)
        if args_value:
            _filter = [key, args_value, "regex"]
            logger.debug("adding filter: %s", _filter)
            filters.append(_filter)

    query_search.extend(filters)

    if kwargs.get("caldat") is None and kwargs.get("proposal") is None:
        raise Exception("use --caldat or --proposal to constrain queries for images")

    query = {
        "outfields": outfields,
        "search": query_search,
    }

    return query


def cli_query():
    parser = argparse.ArgumentParser(prog='python -m deep.noirlab.query')
    parser.add_argument("--process-type", type=str, default="raw")
    parser.add_argument("--observation-type", type=str, default="object")
    parser.add_argument("--band", type=str, default="VR")
    parser.add_argument("--caldat", type=str, default=None)
    parser.add_argument("--OBJECT", type=str, default=None)
    parser.add_argument("--proposal", type=str, default="2019A-0337")
    parser.add_argument("--log-level", type=str, default="INFO")
    args, _ = parser.parse_known_args()

    logger.setLevel(getattr(logging, args.log_level.upper()))
    
    outfields = [ 
        "archive_filename", "obs_type", "proc_type", 
        "prod_type", "md5sum", "dateobs_min", 
    ]

    if args.caldat is None and args.proposal is None:
        raise Exception("use --caldat or --proposal to constrain queries for images")

    return query(
        args.process_type, args.observation_type, outfields, 
        band=args.band, caldat=args.caldat, OBJECT=args.OBJECT, proposal=args.proposal
    )

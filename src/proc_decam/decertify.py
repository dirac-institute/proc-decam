import argparse
import lsst.daf.butler as dafButler
from lsst.daf.butler.registry import MissingCollectionError
import astropy.time

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("repo")
    parser.add_argument("collection")
    parser.add_argument("dataset")

    args = parser.parse_args()

    butler = dafButler.Butler(args.repo, writeable=True)
    try:
        butler.registry.decertify(args.collection, args.dataset, dafButler.Timespan(astropy.time.Time("1970-01-01T00:00:00", scale='tai'), astropy.time.Time("2100-01-01T00:00:00", scale='tai')))
    except MissingCollectionError:
        pass

if __name__ == "__main__":
    main()

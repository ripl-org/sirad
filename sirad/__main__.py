#!/usr/env/bin python
"""
SIRAD - Secure Infrastructure for Research with Administrative Data
"""

import argparse
import logging
import multiprocessing
import sirad
import sys
import traceback

def main():

    parser = argparse.ArgumentParser(description=sirad.__doc__,
                                    formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("-v", "--version",
                        action="version",
                        version="SIRAD {}".format(sirad.__version__))
    parser.add_argument("-n", type=int, default=1, help="number of threads to use in parallel")
    parser.add_argument("-q", "--quiet",
                        action="store_true",
                        help="suppress all logging messages except for errors")
    parser.add_argument("-d", "--debug",
                        action="store_true",
                        help="show all logging messages, including debugging output")

    subparsers = parser.add_subparsers()

    process = subparsers.add_parser("sources")
    process.set_defaults(cmd="sources")

    process = subparsers.add_parser("validate")
    process.set_defaults(cmd="validate")

    process = subparsers.add_parser("process")
    process.set_defaults(cmd="process")

    research = subparsers.add_parser("research")
    research.set_defaults(cmd="research")
    research.add_argument("--seed", type=int, default=0, help="random seed for reproducible SIRAD ID [default: none]")

    args = parser.parse_args()

    if "cmd" in args:

        import logging
        if args.debug:
            logging.basicConfig(level=logging.DEBUG)
        elif args.quiet:
            logging.basicConfig(level=logging.ERROR)
        else:
            logging.basicConfig(level=logging.INFO)

        from sirad import config

        if args.cmd == "sources":
            config.parse_layouts()
            for dataset in config.DATASETS:
                print(dataset.source)

        elif args.cmd == "validate":
            config.parse_layouts()
            from sirad.validate import Validate
            if args.n > 1:
                pool = multiprocessing.Pool(processes=args.n)
                nwarnings = sum(pool.map(Validate, config.DATASETS, chunksize=1))
            else:
                nwarnings = sum(map(Validate, config.DATASETS))
            if nwarnings > 0:
                sys.exit(1)

        elif args.cmd == "process":
            config.parse_layouts(process_log=True)
            from sirad.process import Process
            if args.n > 1:
                pool = multiprocessing.Pool(processes=args.n)
                pool.map(Process, config.DATASETS, chunksize=1)
            else:
                for dataset in config.DATASETS:
                    try:
                        Process(dataset)
                    except Exception as e:
                        logging.error("Error processing dataset '{}': {} {}\n{}".format(
                            dataset.name,
                            type(e),
                            str(e),
                            "".join(traceback.format_tb(e.__traceback__)))
                        )

        elif args.cmd == "research":
            config.parse_layouts()
            from sirad.research import Research
            Research(args.n, args.seed)

    else:
        parser.print_help()


if __name__ == "__main__":
    sys.exit(main())

# vim: expandtab sw=4 ts=4

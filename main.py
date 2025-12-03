"""Folder Synchronizer."""

import argparse
import logging
import sys
import time


class Synchronizer:
    """Class for the Folder Synchronizer."""

    def __init__(self, source, replica, interval_secs, count):
        """Initialize the Synchronizer attributes for later use.

        Args:
            source: source folder
            replica: replica folder
            interval_secs: interval between two synchronizations in seconds
            count: count of synchronizations
        """
        self.source = source
        self.replica = replica
        self.interval = interval_secs
        self.count = count

    def run(self):
        """Run the synchronizer."""
        logging.info("Folder Syncer Running")

        for i in range(self.count):
            self._sync()
            logging.info(f"Folder sync Completed {i + 1} times")
            time.sleep(self.interval)

    def _sync(self):
        pass  # TODO


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Folder syncer for Veeam technical assessment"
    )

    parser.add_argument("source", help="Path of the source folder")
    parser.add_argument("replica", help="Path of the replica folder")
    parser.add_argument(
        "interval_seconds", help="Time between synchronizations in seconds", type=int
    )
    parser.add_argument("count", help="Number of synchronizations", type=int)
    parser.add_argument("logfile", help="Path to log file")

    args = parser.parse_args()

    logging.getLogger(__name__).addHandler(logging.StreamHandler(sys.stdout))
    logging.basicConfig(
        level=logging.DEBUG,
        filename=args.logfile,
        format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
        filemode="w",
    )

    syncer = Synchronizer(args.source, args.replica, args.interval_seconds, args.count)

    syncer.run()


if __name__ == "__main__":
    main()

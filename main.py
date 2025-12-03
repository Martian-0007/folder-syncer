"""Folder Synchronizer."""

import argparse
import logging
import os
import sys
import time


class Synchronizer:
    """Class for the Folder Synchronizer."""

    def __init__(self, source: str, replica: str, interval_secs: int, count: int):
        """Initialize the Synchronizer attributes for later use.

        Args:
            source: source folder
            replica: replica folder
            interval_secs: interval between two synchronizations in seconds
            count: count of synchronizations
        """
        self.source: str = source
        self.replica: str = replica
        self.interval: int = interval_secs
        self.count: int = count

        self.source_abs = os.path.abspath(self.source)
        self.replica_abs = os.path.abspath(self.replica)

    def run(self):
        """Run the synchronizer."""
        logging.info("Folder Syncer Running")

        logging.info(
            f"Source: {self.source_abs}",
        )
        logging.info(f"Replica: {self.replica_abs}")
        logging.debug(f"Interval: {self.interval} s")
        logging.debug(f"Count: {self.count}")

        for i in range(self.count):
            self._sync()
            logging.info(f"Folder sync Completed {i + 1} times")
            time.sleep(self.interval)

    def _sync(self):
        pass  # TODO

    def _symlink_in_source(self, symlink_path) -> str:
        """Check if a symlink is in the source folder.

        Returns absolute path in case symlink points outside of source folder.

        Args:
            symlink_path: path to symlink

        Returns:
            str: path of symlink to be written
        """
        symlink_path_abs = os.path.abspath(os.path.join(self.source, symlink_path))

        logging.debug(f"Symlink absolute path: {symlink_path_abs}")

        if self.source_abs == os.path.commonpath(
            [self.source_abs, symlink_path_abs]
        ):  # alternatively `self.source_abs in symlink_path_abs[:len(self.source_abs)]`
            return symlink_path
        else:
            logging.debug(
                f"Symlink path leads outside of source folder: {symlink_path_abs}"
            )
            return symlink_path_abs


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

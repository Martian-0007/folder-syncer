"""Folder Synchronizer."""

import argparse
import logging
import os
import shutil
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

        logging.info(f"Source: {self.source}")
        logging.debug(f"Source absolute: {self.source_abs}")

        logging.info(f"Replica: {self.replica}")
        logging.debug(f"Replica absolute: {self.replica_abs}")

        logging.debug(f"Interval: {self.interval} s")
        logging.debug(f"Count: {self.count}")

    def run(self):
        """Run the synchronizer."""
        logging.info("Folder Syncer Running")

        for i in range(self.count):
            self._sync()
            logging.info(f"Folder sync Completed {i + 1} times")
            if i < self.count - 1: # no need to sleep on the last sync
                time.sleep(self.interval)

    def _sync(self):
        """Synchronize source folder to replica folder."""
        if not os.path.exists(self.replica_abs):
            logging.debug(f"{self.replica_abs} does not exist, creating directory...")
            os.mkdir(self.replica_abs)

        if not os.path.isdir(self.replica_abs):
            logging.error(f"{self.replica_abs} is not a directory! Quitting")
            quit()

        logging.debug("Cleaning replica folder")
        self._clean(self.replica_abs)

        self._copyfolder(self.source, self.replica)

    def _symlink_path_handler(self, symlink_path, symlink_path_absolute) -> str:
        """Check if a symlink is pointing inside the source folder.

        Returns absolute path of the original link in case symlink points outside the source folder.

        Args:
            symlink_path: path of symlink
            symlink_path: absolute path of symlink

        Returns:
            str: path to which the symlink should point
        """
        logging.debug(f"Symlink absolute path: {symlink_path_absolute}")

        if self.source_abs == os.path.commonpath(
            [self.source_abs, symlink_path_absolute]
        ):  # alternatively `self.source_abs in symlink_path_abs[:len(self.source_abs)]`
            logging.debug(f"Symlink inside source, using {symlink_path}")
            return symlink_path
        else:
            logging.debug(
                f"Symlink path leads outside of source folder, using absolute path: {symlink_path_absolute}"
            )
            return symlink_path_absolute

    def _clean(self, folder_path: str):
        """Clean the folder."""
        # An alternative idea was to remove the whole replica folder and recreate it.
        # This could, however, rewrite metadata it shouldn't.

        if os.path.exists(folder_path):
            for i in os.listdir(folder_path):
                logging.info(f"Remove: {os.path.abspath(os.path.join(folder_path, i))}")

                if os.path.isdir(os.path.join(folder_path, i)):
                    shutil.rmtree(os.path.join(folder_path, i)) # remove even non-empty
                else:
                    os.remove(os.path.join(folder_path, i))

    def _copyfolder(self, src, dst):
        """Copy source folder to destination.

        Args:
            src: path to source folder
            dst: path to destination folder
        """
        contents = os.scandir(src)

        logging.info(f"Copy: {os.path.abspath(src)} to {os.path.abspath(dst)}")

        if not os.path.exists(dst):
            os.mkdir(dst)

        for i in contents:
            if i.is_dir(follow_symlinks=False):
                self._copyfolder(os.path.join(src, i.name), os.path.join(dst, i.name))

            elif i.is_junction():
                logging.warn(
                    f"Junction in path {os.path.realpath(os.path.join(src, i.name))}"
                )

                logging.info(
                    f"Copy: {os.path.abspath(i.path)} to {os.path.abspath(os.path.join(dst, i.name))}"
                )

                shutil.copy2(i.path, os.path.join(dst, i.name))

            elif i.is_symlink():
                source_link_path = os.readlink(i.path)

                logging.info(
                    f"Copy: {os.path.abspath(i.path)} to {os.path.abspath(os.path.join(dst, i.name))}"
                )

                os.symlink(
                    self._symlink_path_handler(source_link_path, os.path.abspath(os.path.join(src, source_link_path))),
                    os.path.join(dst, i.name),
                )

            elif i.is_file():
                logging.info(
                    f"Copy: {os.path.abspath(i.path)} to {os.path.abspath(os.path.join(dst, i.name))}"
                )

                shutil.copy2(i.path, os.path.join(dst, i.name))

            else:
                logging.error(f"Unknown file type: {os.path.abspath(i.path)}")


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

    logging.getLogger(__name__).addHandler(logging.StreamHandler())
    logging.basicConfig(
        level=logging.DEBUG,
        #filename=args.logfile,
        #filemode="w"
        format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
        stream=sys.stderr
    )

    syncer = Synchronizer(args.source, args.replica, args.interval_seconds, args.count)

    syncer.run()


if __name__ == "__main__":
    main()

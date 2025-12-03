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

        self.logger = logging.getLogger(__name__)

        self.logger.info(f"Source: {self.source}")
        self.logger.debug(f"Source absolute: {self.source_abs}")

        self.logger.info(f"Replica: {self.replica}")
        self.logger.debug(f"Replica absolute: {self.replica_abs}")

        self.logger.debug(f"Interval: {self.interval} s")
        self.logger.debug(f"Count: {self.count}")

    def run(self):
        """Run the synchronizer."""
        self.logger.info("Folder Syncer Running")

        for i in range(self.count):
            self._sync()
            self.logger.info(f"Folder sync Completed {i + 1} times")

            if i < self.count - 1:  # no need to sleep on the last sync
                time.sleep(self.interval)

    def _sync(self):
        """Synchronize source folder to replica folder."""
        if not os.path.exists(self.source_abs) or not os.path.isdir(self.source_abs):
            self.logger.error(
                f"Source {self.source_abs} is not a directory or doesn't exist! Quitting"
            )
            quit()

        if not os.path.exists(self.replica_abs):
            self.logger.debug(
                f"Replica {self.replica_abs} does not exist, creating directory..."
            )
            os.mkdir(self.replica_abs)

        if not os.path.isdir(self.replica_abs):
            self.logger.error(
                f"Replica {self.replica_abs} is not a directory! Quitting"
            )
            quit()

        self.logger.info("Syncing...")

        self.logger.debug("Cleaning replica folder")
        self._clean(self.replica_abs)

        self._copyfolder(self.source, self.replica)

    def _clean(self, folder_path: str):
        """Clean the folder."""
        # An alternative idea was to remove the whole replica folder and recreate it.
        # This could, however, rewrite metadata it shouldn't.

        if os.path.exists(folder_path):
            for i in os.listdir(folder_path):
                self.logger.info(
                    f"Remove: {os.path.abspath(os.path.join(folder_path, i))}"
                )

                if os.path.isdir(os.path.join(folder_path, i)):
                    shutil.rmtree(os.path.join(folder_path, i))  # remove even non-empty
                else:
                    os.remove(os.path.join(folder_path, i))

    def _symlink_path_handler(self, symlink_path, symlink_path_absolute) -> str:
        """Check if a symlink is pointing inside the source folder.

        Returns absolute path of the original link in case symlink points outside the source folder.

        Args:
            symlink_path: path of symlink
            symlink_path_absolute: absolute path of symlink

        Returns:
            str: path to which the symlink should point
        """
        self.logger.debug(f"Symlink absolute path: {symlink_path_absolute}")

        if self.source_abs == os.path.commonpath(
            [self.source_abs, symlink_path_absolute]
        ):  # alternatively `self.source_abs in symlink_path_abs[:len(self.source_abs)]`
            self.logger.debug(f"Symlink inside source, using {symlink_path}")
            return symlink_path
        else:
            self.logger.debug(
                f"Symlink path leads outside of source folder, using absolute path: {symlink_path_absolute}"
            )
            return symlink_path_absolute

    def _copyfolder(self, src, dst):
        """Copy source folder to destination.

        Args:
            src: path to source folder
            dst: path to destination folder
        """
        contents = os.scandir(src)

        self.logger.info(f"Copy: {os.path.abspath(src)} to {os.path.abspath(dst)}")

        if not os.path.exists(dst):
            os.mkdir(dst)

        for i in contents:
            if i.is_dir(follow_symlinks=False):
                self._copyfolder(os.path.join(src, i.name), os.path.join(dst, i.name))

            elif i.is_junction():
                self.logger.warning(
                    f"Junction in path {os.path.realpath(os.path.join(src, i.name))}"
                )

                self.logger.info(
                    f"Copy: {os.path.abspath(i.path)} to {os.path.abspath(os.path.join(dst, i.name))}"
                )

                shutil.copy2(i.path, os.path.join(dst, i.name))

            elif i.is_symlink():
                source_link_path = os.readlink(i.path)

                self.logger.info(
                    f"Copy: {os.path.abspath(i.path)} to {os.path.abspath(os.path.join(dst, i.name))}"
                )

                os.symlink(
                    self._symlink_path_handler(
                        source_link_path,
                        os.path.abspath(os.path.join(src, source_link_path)),
                    ),
                    os.path.join(dst, i.name),
                )

                shutil.copystat(i.path, os.path.join(dst, i.name))

            elif i.is_file():
                self.logger.info(
                    f"Copy: {os.path.abspath(i.path)} to {os.path.abspath(os.path.join(dst, i.name))}"
                )

                shutil.copy2(i.path, os.path.join(dst, i.name))

            else:
                self.logger.error(
                    f"Unknown file type: {os.path.abspath(i.path)}, attempting copy..."
                )

                self.logger.info(
                    f"Copy: {os.path.abspath(i.path)} to {os.path.abspath(os.path.join(dst, i.name))}"
                )

                shutil.copy2(i.path, os.path.join(dst, i.name))


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

    logger = logging.getLogger(__name__)
    fh = logging.FileHandler(args.logfile, "w")
    fh.setLevel(logging.DEBUG)

    ch = logging.StreamHandler(sys.stderr)
    ch.setLevel(logging.DEBUG)

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
        handlers=[ch, fh],
    )

    logger.debug("Logger initialized")

    syncer = Synchronizer(args.source, args.replica, args.interval_seconds, args.count)

    syncer.run()


if __name__ == "__main__":
    main()

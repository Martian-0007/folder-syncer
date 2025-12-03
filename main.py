"""Folder Synchronizer."""

import argparse
import filecmp
import logging
import os
import shutil
import sys
import time


class Synchronizer:
    """Class for the Folder Synchronizer."""

    def __init__(
        self, source: str, replica: str, interval_secs: int = 30, count: int = 1
    ):
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

        # self.logger.debug("Cleaning replica folder")
        # self._clean(self.replica_abs)
        #
        # self._copyfolder(self.source, self.replica)
        #
        # Original implementation - would take morbidly long and take up IO on large directories

        self._compare(self.source, self.replica)

        pass  # TODO

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
                self._handle_junction(i, src, dst)

            elif i.is_symlink():
                self._handle_symlink(i, src, dst)

            elif i.is_file():
                self._handle_file(i, src, dst)

            else:
                self._handle_unknown_file(i, src, dst)

    def _compare(self, src, dst):
        """Compare source and destination folders."""
        src_entries = os.scandir(src)
        dst_entries = os.scandir(dst)

        src_contents = [i.name for i in src_entries]
        dst_contents = [i.name for i in dst_entries]

        self.logger.info(f"Comparing: {src_contents} vs. {dst_contents}")

        for i in dst_entries:
            if i.name not in src_contents:
                self.logger.info(
                    f"Remove: {os.path.abspath(os.path.join(dst, i.name))}"
                )

                os.remove(os.path.join(dst, i.name))

        for i in src_entries:
            if i.name in dst_contents:
                same = filecmp.cmp(i.path, os.path.join(dst, i.name), shallow=False)

                if same:
                    self.logger.debug(
                        f"File {os.path.abspath(i.name)} is already replicated"
                    )

            else:
                self.logger.debug(f"{os.path.abspath(i.path)} is not the same")

                if i.is_dir(follow_symlinks=False):
                    if os.path.exists(os.path.join(dst, i.name)) and os.path.isdir(
                        os.path.join(dst, i.name)
                    ):
                        self._compare(
                            os.path.join(src, i.name), os.path.join(dst, i.name)
                        )

                    elif not os.path.exists(os.path.join(dst, i.name)):
                        self._copyfolder(
                            os.path.join(src, i.name), os.path.join(dst, i.name)
                        )

                    else:  # dst/i.name exists, but as a file -> delete and copy directory from source
                        self.logger.info(
                            f"Remove: {os.path.abspath(os.path.join(dst, i.name))}"
                        )

                        os.remove(os.path.join(dst, i.name))

                        self._copyfolder(
                            os.path.join(src, i.name), os.path.join(dst, i.name)
                        )

                elif i.is_junction():
                    self.logger.info(
                        f"Remove: {os.path.abspath(os.path.join(dst, i.name))}"
                    )

                    os.remove(os.path.join(dst, i.name))

                    self._handle_junction(i, src, dst)

                elif i.is_symlink():
                    self.logger.info(
                        f"Remove: {os.path.abspath(os.path.join(dst, i.name))}"
                    )

                    os.remove(os.path.join(dst, i.name))
                    self._handle_symlink(i, src, dst)

                elif i.is_file():
                    self.logger.info(
                        f"Remove: {os.path.abspath(os.path.join(dst, i.name))}"
                    )

                    os.remove(os.path.join(dst, i.name))
                    self._handle_file(i, src, dst)

                else:
                    self._handle_unknown_file(i, src, dst)

    def _handle_junction(self, entry: os.DirEntry[str], src, dst):
        self.logger.warning(
            f"Junction in path {os.path.realpath(os.path.join(src, entry.name))}"
        )

        self.logger.info(
            f"Copy: {os.path.abspath(entry.path)} to {os.path.abspath(os.path.join(dst, entry.name))}"
        )

        shutil.copy2(entry.path, os.path.join(dst, entry.name))

    def _handle_symlink(self, entry: os.DirEntry[str], src, dst):
        source_link_path = os.readlink(entry.path)

        self.logger.info(
            f"Copy: {os.path.abspath(entry.path)} to {os.path.abspath(os.path.join(dst, entry.name))}"
        )

        self.logger.debug(
            f"Symlink target: {
                self._symlink_path_handler(
                    source_link_path,
                    os.path.abspath(os.path.join(src, source_link_path)),
                )
            }"
        )

        try:
            os.symlink(
                self._symlink_path_handler(
                    source_link_path,
                    os.path.abspath(os.path.join(src, source_link_path)),
                ),
                os.path.join(dst, entry.name),
            )

            shutil.copystat(entry.path, os.path.join(dst, entry.name))
        except OSError as e:
            self.logger.error(f"Failed to copy symlink: {e}, skipping...")

    def _handle_file(self, entry: os.DirEntry[str], src, dst):
        self.logger.info(
            f"Copy: {os.path.abspath(entry.path)} to {os.path.abspath(os.path.join(dst, entry.name))}"
        )

        shutil.copy2(entry.path, os.path.join(dst, entry.name))

    def _handle_unknown_file(self, entry: os.DirEntry[str], src, dst):
        self.logger.warning(
            f"Unknown file type: {os.path.abspath(entry.path)}, attempting copy..."
        )

        self.logger.info(
            f"Copy: {os.path.abspath(entry.path)} to {os.path.abspath(os.path.join(dst, entry.name))}"
        )

        try:
            shutil.copy2(entry.path, os.path.join(dst, entry.name))
        except FileExistsError:
            self.logger.debug(
                f"File {os.path.abspath(os.path.join(dst, entry.name))} already exists, replacing..."
            )

            self.logger.info(
                f"Remove: {os.path.abspath(os.path.join(dst, entry.name))}"
            )

            os.remove(os.path.join(dst, entry.name))

            self.logger.info(
                f"Copy: {os.path.abspath(entry.path)} to {os.path.abspath(os.path.join(dst, entry.name))}"
            )

            shutil.copy2(entry.path, os.path.join(dst, entry.name))
        except Exception as e:
            self.logger.error(
                f"Failed to copy: {os.path.abspath(entry.path)}, error: {e}"
            )
            self.logger.info(f"Skipping {os.path.abspath(entry.path)}")


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
    # TODO: Follow symlinks

    try:
        args = parser.parse_args()
    except argparse.ArgumentError:
        parser.print_help()
        quit()

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

    if not os.chmod in os.supports_follow_symlinks:
        logger.warning("Cannot modify permission bits of symlinks")
    if not os.utime in os.supports_follow_symlinks:
        logger.warning("Cannot modify last access and modification times of symlinks")
    try:
        if not os.chflags in os.supports_follow_symlinks:
            logger.warning("Cannot modify flags of symlinks")
    except AttributeError:
        pass  # os.chflags is not supported on all platforms

    syncer = Synchronizer(args.source, args.replica, args.interval_seconds, args.count)

    try:
        syncer.run()
    except KeyboardInterrupt:
        logger.error(f"Interrupted, task unfinished")


if __name__ == "__main__":
    main()

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
        self,
        source: str,
        replica: str,
        interval_secs: int = 30,
        count: int = 1,
        dont_dangle: bool = False,
        odd: bool = False,
    ):
        """Initialize the Synchronizer class.

        Args:
            source: source folder
            replica: replica folder
            interval_secs: interval between two synchronizations in seconds
            count: count of synchronizations
            dont_dangle: don't copy dangling symlinks too
            odd: try to copy unknown files
        """
        self.source: str = source
        self.replica: str = replica
        self.interval: int = interval_secs
        self.count: int = count
        self.dangle: bool = not dont_dangle
        self.odd: bool = odd

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

        if os.path.commonpath([self.source_abs, self.replica_abs]) in [
            self.source_abs,
            self.replica_abs,
        ]:
            self.logger.error(
                f"Replica {self.replica_abs} or source {self.source_abs} is relative to the other! Quitting"
            )
            quit()

        self.logger.info("Syncing...")

        try:
            self._sync_folder(self.source, self.replica)

        except NotADirectoryError as e:
            if not os.path.isdir(self.replica_abs):
                self.logger.error(
                    f"Replica {self.replica_abs} exists, but is not a directory! Quitting"
                )
                quit()

            else:
                raise e

    def _sync_folder(self, src, dst):
        if not os.path.lexists(dst):
            self._mkdir(dst)

        if not os.path.isdir(dst) or os.path.islink(dst):
            self.logger.warning(f"Folder {dst} is not a directory! Recreating")
            self._remove(dst)
            self._mkdir(dst)

        src_entries = os.scandir(src)
        dst_entries = os.scandir(dst)

        src_contents = os.listdir(src)
        dst_contents = os.listdir(dst)

        self.logger.debug(f"Comparing: {src_contents} vs. {dst_contents}")

        for i in dst_entries:
            if i.name not in src_contents:
                self._remove(os.path.join(dst, i.name))

        for i in src_entries:
            self.logger.debug(f"Syncing entry: {i}")

            if i.name in dst_contents:
                same = self._compare_entry(i, src, dst)

                if same:
                    self.logger.debug(
                        f"Comparison: File {os.path.abspath(i.name)} is already replicated"
                    )

                    continue

            self.logger.debug(f"Comparison: {os.path.abspath(i.path)} is not the same")

            self._sync_item(i, src, dst)

    def _compare_entry(self, entry: os.DirEntry[str], src, dst) -> bool:
        same = False  # By default, assume source and destination are different

        if entry.is_dir(follow_symlinks=False):
            # Directories need to have the contents checked, not the directory objects themselves
            self.logger.debug(f"Comparison: {entry.path} is a directory")
            return same

        try:
            if entry.is_symlink():
                self.logger.debug(f"Comparison: {entry.path} is a symlink")

                source_link_path = os.readlink(entry.path)

                target = self._get_symlink_target_path(
                    source_link_path,
                    os.path.abspath(os.path.join(src, source_link_path)),
                    entry,
                    src,
                    dst,
                )
                name = os.path.join(dst, entry.name)

                try:
                    destination_link_path = os.readlink(name)
                except OSError as e:
                    self.logger.debug(f"OSError: {e.strerror}")
                    destination_link_path = None

                self.logger.debug(f"Comparison: symlink target: {target}")
                self.logger.debug(f"Comparison: symlink name: {name}")

                self.logger.debug(
                    f"Comparison: source link target path: {source_link_path}"
                )
                self.logger.debug(
                    f"Comparison: destination link target path: {source_link_path}"
                )

                if destination_link_path == target and target is not None:
                    same = True

            else:
                same = filecmp.cmp(
                    entry.path, os.path.join(dst, entry.name), shallow=False
                )
                # Also takes care of os.stat() signatures
        except FileNotFoundError as e:
            self.logger.error(f"Comparison: FileNotFoundError: {e}")
            same = False  # Nothing and something is not the same

        return same

    def _sync_item(self, entry: os.DirEntry[str], src, dst):
        """Sync an individual item.

        Args:
            entry: os.DirEntry of the item
            src: current working source path
            dst: current working destination path
        """
        self.logger.debug(f"Item sync: Destination {os.path.join(dst, entry.name)}")

        if entry.is_junction():
            if os.path.exists(os.path.join(dst, entry.name)):
                self._remove(os.path.join(dst, entry.name))

            self._handle_junction(entry, src, dst)

        elif entry.is_dir(follow_symlinks=False):
            if os.path.exists(os.path.join(dst, entry.name)) and os.path.isdir(
                os.path.join(dst, entry.name)
            ):
                self._sync_folder(
                    os.path.join(src, entry.name), os.path.join(dst, entry.name)
                )

            elif not os.path.exists(os.path.join(dst, entry.name)):
                self._sync_folder(
                    os.path.join(src, entry.name), os.path.join(dst, entry.name)
                )

            else:  # dst/entry.name exists but is a file -> delete it and copy directory from source
                self._remove(os.path.join(dst, entry.name))

                if entry.is_junction():
                    self._handle_junction(entry, src, dst)
                else:
                    self._sync_folder(
                        os.path.join(src, entry.name), os.path.join(dst, entry.name)
                    )

        else:
            if os.path.lexists(os.path.join(dst, entry.name)):
                self.logger.debug(f"Item sync: Exists: {os.path.join(dst, entry.name)}")

                self._remove(os.path.join(dst, entry.name))

            if entry.is_symlink():
                self._handle_symlink(entry, src, dst)

            elif entry.is_file():
                self._handle_file(entry, src, dst)

            else:
                self._handle_unknown_file(entry, src, dst)

    def _handle_junction(self, entry: os.DirEntry[str], src, dst):
        """Handle NTFS junctions.

        Args:
            entry: os.DirEntry of the junction
            src: current working source path
            dst: current working destination path
        """
        self.logger.debug("Copy Junction")

        self.logger.warning(
            f"Junction in path {os.path.abspath(os.path.join(src, entry.name))}, recursing as regular folder..."
        )

        self._sync_folder(os.path.join(src, entry.name), os.path.join(dst, entry.name))

    def _handle_symlink(self, entry: os.DirEntry[str], src, dst):
        """Handle symlink copying.

        Args:
            entry: os.DirEntry of the symlink
            src: current working source path
            dst: current working destination path
        """
        self.logger.debug("Copy Symlink")

        source_link_path = os.readlink(entry.path)

        target_is_dir = os.path.isdir(entry.path)  # os.path.isdir follows symlinks
        # On Windows there seems to be no reasonable way to differentiate between an empty
        # file symlink and an empty directory symlink
        if target_is_dir:
            self.logger.debug(f"Symlink {entry.path} points to a directory")
        else:
            self.logger.debug(f"Symlink {entry.path} doesn't point to a directory")

        target = self._get_symlink_target_path(
            source_link_path,
            os.path.abspath(os.path.join(os.path.abspath(src), source_link_path)),
            entry,
            src,
            dst,
        )
        name = os.path.join(dst, entry.name)

        self.logger.debug(f"Symlink target: {target}")
        self.logger.debug(f"Symlink name: {name}")

        try:
            if target is not None:
                self.logger.debug("Copy")

                os.symlink(target, name, target_is_dir)

                shutil.copystat(entry.path, name, follow_symlinks=False)

                self.logger.info(
                    f"Copy: {os.path.abspath(entry.path)} to {os.path.abspath(name)}"
                )

            else:
                self.logger.warning(
                    "Symlink is dangling and --dont-dangle-symlinks is enabled, skipping..."
                )

                if os.path.lexists(os.path.join(dst, entry.name)):
                    # symlink could be the same as source but dangling -> remove from destination
                    # (e.g. symlink became dangling between syncs)
                    self._remove(os.path.join(dst, entry.name))

        except OSError as e:
            self.logger.error(f"Failed to copy symlink: {e}, skipping...")

    def _handle_file(self, entry: os.DirEntry[str], src, dst):
        self.logger.debug("Copy File")

        self._copy(entry.path, os.path.join(dst, entry.name))

    def _handle_unknown_file(self, entry: os.DirEntry[str], src, dst):
        """Handle copying of unknown files.

        Args:
            entry: os.DirEntry of the file
            src: current working source path
            dst: current working destination path
        """
        self.logger.debug("Copy Odd File")

        if not self.odd:
            self.logger.debug("Odd files disabled, skipping...")

            self.logger.warning(
                f"Unknown file: {os.path.abspath(entry.path)}, skipping..."
            )

            return

        self.logger.warning(
            f"Unknown file type: {os.path.abspath(entry.path)}, attempting copy..."
        )

        try:
            self._copy(entry.path, os.path.join(dst, entry.name))

        except FileExistsError:
            self.logger.debug(
                f"File {os.path.abspath(os.path.join(dst, entry.name))} already exists, replacing..."
            )

            self._remove(os.path.join(dst, entry.name))

            self._copy(entry.path, os.path.join(dst, entry.name))

        except Exception as e:
            self.logger.error(
                f"Failed to copy: {os.path.abspath(entry.path)}, error: {e}"
            )
            self.logger.info(f"Skipping {os.path.abspath(entry.path)}")

    def _get_symlink_target_path(
        self,
        symlink_target_path,
        symlink_target_path_absolute,
        entry: os.DirEntry[str],
        src,
        dst=None,
    ) -> str | None:
        """Determine the target path of a symlink.

        Returns a translated absolute path for the original symlink target in case symlink points outside
        the source folder and dangling symlinks are not enabled.

        Translated path of the symlink target is acquired by combining the normalized absolute path
        to the symlink itself, "." to allow for de-translation, and the original path of the symlink.

        Returns None when the symlink is dangling and dangling symlinks are not enabled.

        Args:
            symlink_target_path: path of the symlink target
            symlink_target_path_absolute: absolute path of the symlink target
            entry: os.DirEntry of the symlink
            src: current working source path
            dst: current working destination path

        Returns:
            str: path to which the symlink should point
            None: incorrectly dangling symlink
        """
        self.logger.debug(f"Symlink target path: {symlink_target_path}")
        self.logger.debug(
            f"Symlink target absolute path: {symlink_target_path_absolute}"
        )

        dangling = not os.path.lexists(
            symlink_target_path_absolute
        )  # target can be another symlink

        # Allow reversing symlink translation
        target_abs_non_normal = str(
            os.path.join(os.path.abspath(src), ".", symlink_target_path)
        )

        self.logger.debug(
            f"Non-normalized absolute target path: {target_abs_non_normal}"
        )

        self.logger.debug(f"Dangling: {dangling}")

        if self.dangle:
            self.logger.debug("Symlink: --dont-dangle-symlinks is disabled")

            if self.source_abs == os.path.commonpath(
                [self.source_abs, symlink_target_path_absolute]
            ):
                self.logger.debug(f"Symlink inside source, using {symlink_target_path}")

            else:
                self.logger.debug(
                    f"Symlink path leads outside of source folder, but --dont-dangle-symlinks is disabled, using {symlink_target_path}"
                )

            return symlink_target_path

        elif not dangling:
            if self.source_abs == os.path.commonpath(
                [self.source_abs, symlink_target_path_absolute]
            ):
                self.logger.debug(f"Symlink inside source, using {symlink_target_path}")
                return symlink_target_path

            else:
                self.logger.warning(
                    f"Symlink path leads outside of source folder, using absolute non-normalized path: {target_abs_non_normal}"
                )
                return target_abs_non_normal

        else:
            return None

    def _detranslate_symlink_target_path(
        self,
        symlink_target_path,
    ):
        """Detranslate target path from a previously translated symlink target.

        Args:
            symlink_target_path: path of the symlink target

        Raises:
            ValueError: if the symlink target is not a previously translated symlink target
        """
        self.logger.debug(f"Symlink target path on disk: {symlink_target_path}")

        detranslated = symlink_target_path[symlink_target_path.index("/./") + 3 :]
        # Translated symlinks have their own original directory path normalized before
        # the use of it for translation, meaning there will be no "/./" or "/../" preceding the target path
        #
        # + 3 because the index starts at the first "/" and we want only
        # the path following the "/./" expression

        return detranslated

    def _copy(self, src, dst):
        self.logger.debug("Copy")

        shutil.copy2(src, dst)

        self.logger.info(f"Copy: {os.path.abspath(src)} to {os.path.abspath(dst)}")

    def _remove(self, path):
        self.logger.debug("Remove")

        if os.path.isdir(path) and not os.path.islink(path):
            shutil.rmtree(path)

        else:
            os.remove(path)

        self.logger.info(f"Remove: {os.path.abspath(path)}")

    def _mkdir(self, dst):
        self.logger.debug("Create")
        os.mkdir(dst)
        self.logger.info(f"Create: {dst}")


def main():
    """Run Folder Syncer."""
    parser = argparse.ArgumentParser(
        description="Folder syncer for Veeam technical assessment"
    )

    parser.add_argument("source", help="Path of the source folder")
    parser.add_argument("replica", help="Path of the replica folder")
    parser.add_argument(
        "interval_seconds", help="Time between synchronizations in seconds", type=float
    )
    parser.add_argument("count", help="Number of synchronizations", type=int)
    parser.add_argument("logfile", help="Path to log file")

    # Extras
    parser.add_argument("-v", "--verbose", help="verbose", action="store_true")
    parser.add_argument(
        "--dont-dangle-symlinks",
        help="Don't keep dangling symlinks or translate valid targets (default: keep)",
        action="store_true",
    )  # Initially inverted, but then the folders wouldn't be identical...
    parser.add_argument(
        "--odd-files",
        help="Try to sync unknown files (default: skip)",
        action="store_true",
    )
    # Maybe TODO: Follow symlinks
    # Not implemented: folders are supposed to be identical, so symlinks should be in replica if they are in source

    try:
        args = parser.parse_args()
    except argparse.ArgumentError:
        parser.print_help()
        quit()

    logger = logging.getLogger(__name__)

    file_handler = logging.FileHandler(args.logfile, "w")
    file_handler.setLevel(logging.DEBUG)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG if args.verbose else logging.INFO)

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
        handlers=[console_handler, file_handler],
    )

    logger.debug("Logger initialized")

    if args.interval_seconds < 0:
        logger.error("Interval must be positive!")
        quit()
    if args.count < 0:
        logger.error("Count must be a positive integer!")
        quit()

    if not os.chmod in os.supports_follow_symlinks:
        logger.warning("Cannot modify permission bits of symlinks")
    if not os.utime in os.supports_follow_symlinks:
        logger.warning("Cannot modify last access and modification times of symlinks")
    try:
        if not os.chflags in os.supports_follow_symlinks:
            logger.warning("Cannot modify flags of symlinks")
    except AttributeError:
        pass  # os.chflags is not available on all platforms

    syncer = Synchronizer(
        args.source,
        args.replica,
        args.interval_seconds,
        args.count,
        args.dont_dangle_symlinks,
        args.odd_files,
    )

    try:
        syncer.run()
    except KeyboardInterrupt:
        logger.error(f"Interrupted, task unfinished")


if __name__ == "__main__":
    main()

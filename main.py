import argparse
import logging
import sys


class Synchronizer:
    def __init__(self, source, replica, interval, count):
        self.source = source
        self.replica = replica
        self.interval = interval
        self.count = count

    def run(self):
        logging.info("Folder Syncer Running")

        for i in range(self.count):
            self.sync()


    def sync(self):
        pass

def main():
    parser = argparse.ArgumentParser(description="Folder syncer for Veeam technical assessment")

    parser.add_argument("source", help="Path of the source folder")
    parser.add_argument("replica", help="Path of the replica folder")
    parser.add_argument("interval", help="Time between synchronizations in seconds", type=int)
    parser.add_argument("count", help="Number of synchronizations", type=int)
    parser.add_argument("logfile", help="Path to log file")

    args = parser.parse_args()

    logging.getLogger(__name__).addHandler(logging.StreamHandler(sys.stdout))
    logging.basicConfig(level=logging.DEBUG, filename=args.logfile)


if __name__ == "__main__":
    main()

""" This is the main module to implement the ETL task from Revenue NSW.

    It calls the functions in etl.ingest_member_data to run the etl process.
"""

import sys
import logging

from etl import ingest_member_data as imd

logger = logging.getLogger(__name__)

def usage():
    """
    Show usage
    """
    logger.info("""
    Usage: python3 main.py <input filename1> <input filename2> <input filename3> ...
    """)


def main():
    logging.basicConfig(level=logging.INFO)
    if len(sys.argv) < 2:
        usage()
        sys.exit(1)

    filenames = sys.argv[1:]
    for filename in filenames:
        logger.info(f"Processing file [{filename}]")
        inserted_count = imd.run_etl(filename)
        logger.info(f"Record count inserted: {inserted_count}")
        logger.info(f"Finished processing file [{filename}]")


if __name__ == '__main__':
    main()

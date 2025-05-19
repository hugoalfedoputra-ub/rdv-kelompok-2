import os
import sys
import argparse
from dotenv import load_dotenv

load_dotenv()

# Default values will be used as defaults for argparse
DEFAULT_START_DATE = "2025-05-15"
DEFAULT_END_DATE = "2025-05-17"
DEFAULT_LATITUDE = -7.95
DEFAULT_LONGITUDE = 112.61

# Allow overriding via CLI
DEFAULT_COUCHDB_UNAME_ENV = os.getenv("APP_USER")
DEFAULT_COUCHDB_PWD_ENV = os.getenv("APP_PASSWORD")

from etl_bulk import BulkETL


def main():
    """
    Main function to parse arguments and run the BulkETL process.
    """
    parser = argparse.ArgumentParser(
        description="Run the BulkETL process with specified parameters.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # Add arguments
    parser.add_argument(
        "--start-date",
        type=str,
        default=DEFAULT_START_DATE,
        help="Start date for ETL process (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=DEFAULT_END_DATE,
        help="End date for ETL process (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--latitude",
        type=float,
        default=DEFAULT_LATITUDE,
        help="Latitude for data processing.",
    )
    parser.add_argument(
        "--longitude",
        type=float,
        default=DEFAULT_LONGITUDE,
        help="Longitude for data processing.",
    )
    parser.add_argument(
        "--couchdb-user",
        type=str,
        default=DEFAULT_COUCHDB_UNAME_ENV,
        help="CouchDB username. Overrides APP_USER environment variable if set.",
    )
    parser.add_argument(
        "--couchdb-password",
        type=str,
        # No default here in the argument itself, we'll handle it post-parsing
        # to ensure we check env var if CLI arg is not given.
        # If you set default=DEFAULT_COUCHDB_PWD_ENV here, it might be 'None'
        # and that 'None' would be passed if the arg isn't specified,
        # potentially overriding a later check for the env var.
        # So, we'll check args.couchdb_password first, then env var.
        help="CouchDB password. Overrides APP_PASSWORD environment variable if set.",
    )

    args = parser.parse_args()

    # Determine CouchDB credentials
    # Priority: CLI argument > Environment Variable > Error
    couchdb_uname = args.couchdb_user
    couchdb_pwd = (
        args.couchdb_password
        if args.couchdb_password is not None
        else DEFAULT_COUCHDB_PWD_ENV
    )

    if not couchdb_uname:
        parser.error(
            "CouchDB username not provided. "
            "Set --couchdb-user argument or APP_USER environment variable."
        )
    if not couchdb_pwd:
        parser.error(
            "CouchDB password not provided. "
            "Set --couchdb-password argument or APP_PASSWORD environment variable."
        )

    try:
        bulker = BulkETL(f"http://{couchdb_uname}:{couchdb_pwd}@localhost:5984/")
        bulker.run(args.start_date, args.end_date, args.latitude, args.longitude)
        print("BulkETL process completed successfully.")
    except Exception as e:
        print(f"An error occurred during the BulkETL process: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
    # Example usage
    # python bulk_exec.py --start-date 2025-03-06 --end-date 2025-03-08 --latitude -7.95 --longitude 112.61

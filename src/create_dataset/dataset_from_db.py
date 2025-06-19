import os
import csv
import collections.abc
from dotenv import load_dotenv
import pycouchdb


# --- Flattening Function ---
def flatten_dict(data_dict, parent_key="", separator="_"):
    """
    Flattens a nested dictionary.
    Nested keys are combined using the separator.
    Lists of dictionaries are expanded with an index for each element's keys.
    Lists of non-dictionary items or empty lists are converted to a string representation.
    """
    items = []
    for key, value in data_dict.items():
        new_key = f"{parent_key}{separator}{key}" if parent_key else key

        if isinstance(
            value, collections.abc.MutableMapping
        ):  # If value is a dict-like object
            items.extend(flatten_dict(value, new_key, separator=separator).items())
        elif isinstance(value, list):
            # Check if the list is non-empty and all its elements are dictionaries
            is_list_of_dicts = bool(value) and all(
                isinstance(item, collections.abc.MutableMapping) for item in value
            )

            if is_list_of_dicts:
                # If it's a list of dictionaries, expand each dictionary
                for i, item_in_list in enumerate(value):
                    items.extend(
                        flatten_dict(
                            item_in_list,
                            f"{new_key}{separator}{i}",
                            separator=separator,
                        ).items()
                    )
            else:
                # For empty lists or lists not containing all dictionaries (e.g., list of strings, numbers, mixed),
                # represent the list as a string.
                items.append((new_key, str(value)))
        else:  # For simple data types (string, number, boolean, None)
            items.append((new_key, value))
    return dict(items)


# --- Main Script ---
def main():
    # Load environment variables from .env file
    load_dotenv()
    COUCHDB_HOST = os.getenv("COUCHDB_HOST")
    COUCHDB_PORT = os.getenv("COUCHDB_PORT")
    COUCHDB_USERNAME = os.getenv("COUCHDB_USERNAME")
    COUCHDB_PASSWORD = os.getenv("COUCHDB_PASSWORD")

    # Construct CouchDB URI
    COUCHDB_URI = (
        f"http://{COUCHDB_USERNAME}:{COUCHDB_PASSWORD}@{COUCHDB_HOST}:{COUCHDB_PORT}"
    )

    # Connect to CouchDB server
    server = pycouchdb.Server(COUCHDB_URI)

    # Hard-coded list of table (database) names
    table_names = ["aggregate", "free_weather", "open_meteo", "open_weather"]

    for db_name in table_names:
        print(f"Processing table (database): {db_name}...")

        # Access the database.
        # As per "no error handling" requirement, we assume the database exists.
        # If not, operations on 'db' object will raise an exception later.
        db = server.database(db_name)

        # Fetch all documents. Materialize to a list.
        # db.all_docs() returns a ViewResults object (iterable).
        # Using include_docs=True to get the full documents.
        all_docs_results = list(db.query("_all_docs", include_docs=True))

        if not all_docs_results:
            print(f"No documents found in {db_name}. Skipping to next table.")
            continue

        # Extract actual documents ('doc' field) from the results
        docs_to_process = []
        for row in all_docs_results:
            # Each 'row' object from ViewResults has a 'doc' attribute if include_docs=True
            if row["doc"]:
                docs_to_process.append(row["doc"])

        if not docs_to_process:
            # This case might occur if rows exist but 'doc' is null or missing for all of them.
            print(f"No valid documents with 'doc' field found in {db_name}. Skipping.")
            continue

        # Determine CSV headers from the first document.
        # The problem states: "The schema for all documents have been validated to be the same".
        # This implies the first document is representative for all headers.
        first_doc_flattened = flatten_dict(docs_to_process[0])

        # Sort headers alphabetically for consistent column order in CSV.
        headers = sorted(list(first_doc_flattened.keys()))

        csv_file_name = f"{db_name}.csv"
        print(f"Writing data from {db_name} to {csv_file_name}...")

        with open(
            "data/" + csv_file_name, "w", newline="", encoding="utf-8"
        ) as csvfile:
            # `extrasaction='ignore'` ensures that if a document somehow has more fields
            # than inferred from the first document (via `headers`), those extra fields are ignored.
            # If a document is missing fields present in `headers` (e.g. after flattening),
            # `csv.DictWriter` will write empty values for those missing fields.
            writer = csv.DictWriter(csvfile, fieldnames=headers, extrasaction="ignore")
            writer.writeheader()

            # Write the first document (which was already flattened)
            writer.writerow(first_doc_flattened)

            # Write the rest of the documents
            for doc_content in docs_to_process[1:]:
                flattened_doc_for_row = flatten_dict(doc_content)
                writer.writerow(flattened_doc_for_row)

        print(
            f"Successfully wrote {len(docs_to_process)} documents from {db_name} to {csv_file_name}."
        )

    print("\nAll tables processed.")


if __name__ == "__main__":
    # Ensure you have a .env file in the same directory as this script,
    # or that the environment variables COUCHDB_HOST, COUCHDB_PORT,
    # COUCHDB_USERNAME, COUCHDB_PASSWORD are set in your environment.
    main()

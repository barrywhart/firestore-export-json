import argparse
import json
import sys
from io import BytesIO
from typing import Dict, List
from urllib.parse import urlparse

from google.appengine.api import datastore
from google.appengine.api.datastore_types import EmbeddedEntity
from google.appengine.datastore import entity_bytes_pb2 as entity_pb2
from google.cloud import storage

from converter import records
from converter.exceptions import BaseError
from converter.utils import embedded_entity_to_dict, get_dest_dict, serialize_json

num_files = 0
num_files_processed = 0


def main(args=None):
    if args is None:
        args = sys.argv[1:]

    parser = argparse.ArgumentParser(
        prog="fs_to_json", description="Firestore DB export to JSON"
    )

    parser.add_argument(
        "source_dir",
        type=str,
        action="store",
        default=None,
    )

    parser.add_argument(
        "-c",
        "--no-check-crc",
        help="Turn off the check/computation of CRC values for the records."
        "This will increase performance at the cost of potentially having corrupt data,"
        "mostly on systems without accelerated crc32c.",
        default=False,
        action="store_true",
    )

    parser.add_argument(
        "--max-files",
        type=int,
        action="store",
        default=None,
    )

    args = parser.parse_args(args)
    try:
        results = process_files(
            source_dir=args.source_dir,
            no_check_crc=args.no_check_crc,
            max_files=args.max_files,
        )

        print("Analysis:")
        combined_analysis = combine_file_analysis(results)
        analysis_output = json.dumps(
            combined_analysis, default=serialize_json, ensure_ascii=False, indent=2
        )
        print(analysis_output)
        with open("analysis.json", "w", encoding="utf8") as out:
            out.write(analysis_output)
    except BaseError as e:
        print(str(e))
        sys.exit(1)


def process_files(
    source_dir: str, no_check_crc: bool, max_files: int = None
) -> List[Dict]:
    global num_files
    bucket_name, prefix = parse_gcs_uri(source_dir)
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=prefix))
    if max_files is not None:
        print(f"Found {len(blobs)} files, limiting to {max_files} files")
        blobs = blobs[:max_files]

    num_files = len(blobs)
    print(f"processing {num_files} file(s)")

    results = []
    for blob in blobs:
        print("processing", blob.name)
        result = analyze_file(no_check_crc, blob)
        if result is not None:
            results.append(result)
    print(
        f"processed: {num_files_processed}/{num_files} {num_files_processed/num_files*100}%"
    )
    return results


def analyze_file(no_check_crc: bool, blob: storage.Blob):
    global num_files_processed
    if not blob.name.split("/")[-1].startswith("output-"):
        return

    file_analysis = {}
    json_tree = read_file(blob, no_check_crc)
    for collection, docs in json_tree.items():
        if collection not in file_analysis:
            file_analysis[collection] = {
                "num_records": len(docs),
                "source_files": [blob.name],
            }

    num_files_processed += 1
    if num_files > 0:
        print(
            f"progress: {num_files_processed}/{num_files} {num_files_processed / num_files * 100}%"
        )
    return file_analysis


def combine_file_analysis(file_analysis_list: List[Dict]) -> Dict:
    combined_analysis = {}

    for file_analysis in file_analysis_list:
        for collection, analysis in file_analysis.items():
            if collection not in combined_analysis:
                combined_analysis[collection] = {
                    "num_records": 0,
                    "source_files": [],
                }
            combined_analysis[collection]["num_records"] += analysis["num_records"]
            combined_analysis[collection]["source_files"].extend(
                analysis["source_files"]
            )

    return combined_analysis


def read_file(in_file: storage.Blob, no_check_crc) -> Dict:
    """Read Firebase backup file and convert to JSON."""
    json_tree: Dict = {}
    content = in_file.download_as_bytes()
    io = BytesIO(content)
    reader = records.RecordsReader(io, no_check_crc=no_check_crc)
    for record in reader:
        entity_proto = entity_pb2.EntityProto()
        entity_proto.ParseFromString(record)
        ds_entity = datastore.Entity.FromPb(entity_proto)
        data = {}
        for name, value in list(ds_entity.items()):
            if isinstance(value, EmbeddedEntity):
                dt: Dict = {}
                data[name] = embedded_entity_to_dict(value, dt)
            else:
                data[name] = value

        data_dict = get_dest_dict(ds_entity.key(), json_tree)
        data_dict.update(data)
    return json_tree


def parse_gcs_uri(uri):
    """Parse a GCS URI into bucket and path components.

    Args:
        uri (str): The GCS URI to parse.

    Returns:
        tuple: A tuple containing the bucket name and the path.
    """
    parsed_uri = urlparse(uri)

    if parsed_uri.scheme != "gs":
        raise ValueError(f"URI scheme must be 'gs', got '{parsed_uri.scheme}'")

    bucket = parsed_uri.netloc
    path = parsed_uri.path.lstrip("/")

    return bucket, path


if __name__ == "__main__":
    main()

import pandas as pd
from pathlib import Path
from .downloader import uniprot_data
from .merger import merge_incremental_data
import json


def run_pipeline(
    mode: str,
    output_json: str,
    output_jsonl: str,
    from_date: str = None,
    to_date: str = None,
    base_jsonl: str = None,
    exclude_sequence: bool = False,
    size: int = 500,
    file_format: str = "jsonl",
    query: str = "organism_id:9606 AND reviewed:true"
):
    """
    Run the UniProt data pipeline in either bulk or incremental mode.

    Args:
        mode (str): 'bulk' or 'incremental'
        output_json (str): Path to save raw JSON response
        output_jsonl (str): Path to save flattened + merged JSONL
        from_date (str): Start date for filtering (incremental mode)
        to_date (str): End date (currently unused)
        base_jsonl (str): Path to base JSONL file for incremental merge
        exclude_sequence (bool): Whether to drop sequence.value fields
        size (int): Number of entries per request
        file_format (str): 'json' or 'jsonl'
        query (str): UniProt API query
    """

    if mode not in {"bulk", "incremental"}:
        raise ValueError("Invalid mode! Use 'bulk' or 'incremental'.")

    # ---------------------------- BULK MODE ----------------------------
    if mode == "bulk":
        print("Running BULK import:")
        records, headers = uniprot_data(
            query=query,
            output_json_path=output_json,
            flat_jsonl_path=output_jsonl,
            size=size,
            exclude_sequence=exclude_sequence
        )
        # Save metadata (for compare later)
        _log_and_write_metadata(headers, output_json, query)
        return

    # ------------------------- INCREMENTAL MODE ------------------------
    if mode == "incremental":
        if not from_date:
            raise ValueError("from_date must be provided in incremental mode.")

        temp_path = make_temp_path(output_jsonl)
        print(f"Running INCREMENTAL import from {from_date} to {to_date or 'now'}:")

        # records = uniprot_data(
        #     query=query,
        #     output_json_path=output_json,
        #     flat_jsonl_path=temp_path,
        #     size=size,
        #     exclude_sequence=exclude_sequence
        # )

        records, headers = uniprot_data(  
            query=query,
            output_json_path=output_json,
            flat_jsonl_path=temp_path,
            size=size,
            exclude_sequence=exclude_sequence)
        
        # Save metadata (for compare later)
        _log_and_write_metadata(headers, output_json, query)

        if not records:
            print("No records retrieved. Skipping merge.")
            return

        df = pd.DataFrame(records)
        
        # # Log UniProt version info
        # release = headers.get("X-UniProt-Release")
        # release_date = headers.get("X-UniProt-Release-Date")
        # deployment_date = headers.get("X-API-Deployment-Date")
        
        # print(f"UniProt Release: {release} on {release_date} (Deployed {deployment_date})")

        # if not records:
        #     print("No records retrieved. Skipping merge.")
        #     return

        # df = pd.DataFrame(records)

        # Ensure date columns are converted properly
        for field in ["entryAudit.firstPublicDate", "entryAudit.lastAnnotationUpdateDate"]:
            if field in df.columns:
                df[field] = pd.to_datetime(df[field], errors="coerce")
        
        # Filter updates records 
        filtered = df[df["entryAudit.lastAnnotationUpdateDate"] >= pd.to_datetime(from_date)]
        print(f"[Filtered] Keeping {len(filtered)} updated rows since {from_date}")

        if filtered.empty:
            print("No matching records for incremental update. Skipping merge.")
            return

        filtered.to_json(temp_path, orient="records", lines=True)
        merge_incremental_data(base_jsonl, temp_path, output_jsonl, file_format=file_format)


#  ----------------------- Utility functions -----------------------

    """
    To create a consistent and descriptive temporary file path for incremental UniProt data downloads. 
    New data can be saved separately without overwriting the existing merged or base file.

    This function generates a temporary file path based on the input file name by appending _incremental before the file extension
    Include .json or .jsonl 

    Args:
        path (str): The original file path.
    Returns:
        str: The modified file path with _incremental suffix.

    """

def make_temp_path(path: str) -> str:
    """Generate a temporary file path by appending incremental to filename."""
    suffix = "_incremental"
    if path.endswith(".json"):
        return path.replace(".json", f"{suffix}.json")
    elif path.endswith(".jsonl"):
        return path.replace(".jsonl", f"{suffix}.jsonl")
    return path + suffix

def _log_and_write_metadata(headers, output_json_path, query=None):
    """Print UniProt release info and write metadata file."""
    release = headers.get("X-UniProt-Release")
    release_date = headers.get("X-UniProt-Release-Date")
    deployment_date = headers.get("X-API-Deployment-Date")
    print(f"UniProt Release: {release} on {release_date} (Deployed {deployment_date})")
    
    metadata = {
        "release": release,
        "release_date": release_date,
        "deployment_date": deployment_date,
        "fetched_on": pd.Timestamp.now().isoformat(),
    }

    if query:
        metadata["query"] = query
    # Metadata dictionary is saved as a .json file right next to raw JSON output
    metadata_path = output_json_path.replace(".json", "_metadata.json")
    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=2)
    print(f"Metadata saved to: {metadata_path}")



import pandas as pd
from pathlib import Path
from .downloader import uniprot_data
from .merger import merge_incremental_data


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
        raise ValueError("Invalid mode. Use 'bulk' or 'incremental'.")

    # ---------------------------- BULK MODE ----------------------------
    if mode == "bulk":
        print("Running BULK import as: ")
        uniprot_data(
            query=query,
            output_json_path=output_json,
            flat_jsonl_path=output_jsonl,
            size=size,
            exclude_sequence=exclude_sequence
        )
        return

    # ------------------------- INCREMENTAL MODE ------------------------
    if mode == "incremental":
        if not from_date:
            raise ValueError("from_date must be provided in incremental mode.")

        temp_path = _make_temp_path(output_jsonl)
        print(f"Running INCREMENTAL import from {from_date} to {to_date or 'now'}:")

        records = uniprot_data(
            query=query,
            output_json_path=output_json,
            flat_jsonl_path=temp_path,
            size=size,
            exclude_sequence=exclude_sequence
        )

        if not records:
            print("No records retrieved. Skipping merge.")
            return

        df = pd.DataFrame(records)

        # Ensure date columns are converted properly
        for field in ["entryAudit.firstPublicDate", "entryAudit.lastAnnotationUpdateDate"]:
            if field in df.columns:
                df[field] = pd.to_datetime(df[field], errors="coerce")

        filtered = df[df["entryAudit.lastAnnotationUpdateDate"] >= pd.to_datetime(from_date)]

        print(f"[Filtered] Keeping {len(filtered)} updated rows since {from_date}")

        if filtered.empty:
            print("No matching records for incremental update. Skipping merge.")
            return

        filtered.to_json(temp_path, orient="records", lines=True)
        merge_incremental_data(base_jsonl, temp_path, output_jsonl, file_format=file_format)


def _make_temp_path(path: str) -> str:
    """Generate a temporary file path by appending _incremental to a given filename."""
    suffix = "_incremental"
    if path.endswith(".json"):
        return path.replace(".json", f"{suffix}.json")
    elif path.endswith(".jsonl"):
        return path.replace(".jsonl", f"{suffix}.jsonl")
    return path + suffix  # fallback



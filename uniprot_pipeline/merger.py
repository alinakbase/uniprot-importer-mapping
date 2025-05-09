import pandas as pd
from pathlib import Path
from typing import Literal, Optional

"""
   # Merge JSONL files
python -m uniprot_pipeline.merger \
  --old data/uniprot_bulk.jsonl \
  --new data/uniprot_flat.jsonl \
  --out data/uniprot_merged.jsonl \
  --format jsonl

# Merge JSON files
python -m uniprot_pipeline.merger \
  --old data/uniprot_bulk.json \
  --new data/uniprot_flat.json \
  --out data/uniprot_merged.json \
  --format json 

"""

def read_file(path: str, file_format: str) -> pd.DataFrame:
    """Load data from file in specified format."""
    if file_format == "json":
        return pd.read_json(path)
    elif file_format == "jsonl":
        return pd.read_json(path, lines=True)
    else:
        raise ValueError(f"Unsupported file format: {file_format}")


def write_file(df: pd.DataFrame, path: str, file_format: str):
    """Write DataFrame to file in specified format."""
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    if file_format == "json":
        df.to_json(path, orient="records", indent=2)
    elif file_format == "jsonl":
        df.to_json(path, orient="records", lines=True)
    else:
        raise ValueError(f"Unsupported file format: {file_format}")
    print(f"Save Merged file: {path}")


def merge_incremental_data(
    old_path: str,
    new_path: str,
    output_path: str,
    id_column: str = "primaryAccession",
    sort_by: str = "entryAudit.lastAnnotationUpdateDate",
    file_format: Literal["json", "jsonl"] = "jsonl") -> pd.DataFrame:

    """
    Merge existing and new UniProt data, remove duplicates, sort, and save to file.

    Args:
        old_path (str): Path to existing data file (can be missing).
        new_path (str): Path to new data file.
        output_path (str): Where to save merged file.
        id_column (str): Column used to identify duplicates.
        sort_by (str): Column used to sort entries.
        file_format (str): File format: 'json' or 'jsonl'.

    Returns:
        pd.DataFrame: Final merged DataFrame.
    """

    # Load new data
    if not Path(new_path).exists():
        raise FileNotFoundError(f"New data file not found: {new_path}")
    new_df = read_file(new_path, file_format)

    # Load old data if available
    if Path(old_path).exists():
        print(f"[INFO] Loading old data from: {old_path}")
        old_df = read_file(old_path, file_format)
    else:
        print("No old data found")
        old_df = pd.DataFrame()

    # Combine and deduplicate
    combined = pd.concat([old_df, new_df], ignore_index=True)
    print(f"Combined rows: {len(combined)}")

    # Check required columns
    for col in [id_column, sort_by]:
        if col not in combined.columns:
            raise ValueError(f"Missing required column: '{col}'. Available columns: {list(combined.columns)}")

    # Sort and drop duplicates
    combined.sort_values(by=sort_by, ascending=False, inplace=True)
    combined.drop_duplicates(subset=id_column, keep="first", inplace=True)
    combined.reset_index(drop=True, inplace=True)

    if combined.empty:
        print("Merged result is empty after deduplication.")

    # Save output
    write_file(combined, output_path, file_format)
    return combined


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Merge UniProt bulk and incremental data files.")
    parser.add_argument("--old", required=True, help="Path to old (bulk) data file")
    parser.add_argument("--new", required=True, help="Path to new incremental data file")
    parser.add_argument("--out", required=True, help="Path to save merged output")
    parser.add_argument("--id_column", default="primaryAccession", help="Column used to deduplicate")
    parser.add_argument("--sort_by", default="entryAudit.lastAnnotationUpdateDate", help="Column used to sort")
    parser.add_argument("--format", default="jsonl", choices=["json", "jsonl"], help="File format to read/write")

    args = parser.parse_args()

    merge_incremental_data(
        old_path=args.old,
        new_path=args.new,
        output_path=args.out,
        id_column=args.id_column,
        sort_by=args.sort_by,
        file_format=args.format
    )

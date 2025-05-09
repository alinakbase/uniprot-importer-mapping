import os
import requests
import json
from typing import Optional, List
from .flatten import flatten_json 

def uniprot_data(
    query: str,
    output_json_path: str,
    flat_json_path: Optional[str] = None,
    flat_jsonl_path: Optional[str] = None,
    raw_jsonl_path: Optional[str] = None,
    format: str = "json",
    size: int = 100,
    exclude_sequence: bool = False,
    include_keys: Optional[set] = None,
    max_depth: Optional[int] = None,
) -> Optional[List[dict]]:
    """
    Fetch data from the UniProt REST API and save it in raw and optionally flattened formats.

    Args:
        query (str): The UniProt query string.
        output_json_path (str): Path to save the raw JSON response.
        flat_json_path: Path to save the flattened JSON array.
        flat_jsonl_path: Path to save the flattened JSONL file.
        raw_jsonl_path: Path to save the original (raw) entries in JSONL format.
        format (str): Format of the API response, default is "json".
        size (int): Number of entries to fetch per request.
        exclude_sequence (bool): Whether to exclude the protein sequence field.
        include_keys: Only flatten keys included in this set.
        max_depth: Maximum depth for flattening.

    Returns:
        list[dict] or None: Flattened data if successful, otherwise None.
    """

    url = "https://rest.uniprot.org/uniprotkb/search"
    params = {"query": query, "format": format, "size": size}
    print(f"[Requesting] {url}")
    print(f"[Params] {params}")

    # Perform the API request
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return None

    # Save the raw JSON response to file
    if output_json_path:
        output_dir = os.path.dirname(output_json_path)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
        try:
            with open(output_json_path, "w") as f:
                f.write(response.text)
            print(f"Raw JSON saved to: {os.path.abspath(output_json_path)}")
        except Exception as e:
            print(f"Failed to save raw JSON: {e}")
            return None

    # Parse JSON data
    try:
        json_data = response.json()
        records = json_data.get("results", [])
    except Exception as e:
        print(f"Failed to parse JSON: {e}")
        return None

    if not records:
        print("No records found in the API response.")
        return None

    # Convert include_keys to set for fast lookup
    if include_keys and isinstance(include_keys, list):
        include_keys = set(include_keys)

    # Flatten each record
    flattened = []
    for entry in records:
        row = flatten_json(entry, max_depth=max_depth, include_keys=include_keys)
        if exclude_sequence:
            # Filter out sequence fields if requested
            row = {k: v for k, v in row.items() if not k.startswith("sequence.value")}
        flattened.append(row)

    # Helper to save JSON 
    def save_json(filepath, content):
        if filepath:
            try:
                out_dir = os.path.dirname(filepath)
                if out_dir:
                    os.makedirs(out_dir, exist_ok=True)
                with open(filepath, "w") as f:
                    json.dump(content, f, indent=2)
                print(f"Saved JSON to: {os.path.abspath(filepath)}")
            except Exception as e:
                print(f"Failed to save JSON: {e}")

    # Helper to save JSONL 
    def save_jsonl(filepath, content):
        if filepath:
            try:
                out_dir = os.path.dirname(filepath)
                if out_dir:
                    os.makedirs(out_dir, exist_ok=True)
                with open(filepath, "w") as f:
                    for row in content:
                        f.write(json.dumps(row) + "\n")
                print(f"Saved JSONL to: {os.path.abspath(filepath)}")
            except Exception as e:
                print(f"Failed to save JSONL: {e}")

    # Save processed outputs
    save_json(flat_json_path, flattened)
    save_jsonl(flat_jsonl_path, flattened)
    save_jsonl(raw_jsonl_path, records)
    return flattened, response.headers


# Standalone usage for local testing
if __name__ == "__main__":
    query = "organism_id:9606 AND reviewed:true"

    uniprot_data(
        query=query,
        output_json_path="data/uniprot_bulk.json",
        flat_json_path="data/uniprot_flat.json",
        flat_jsonl_path="data/uniprot_flat.jsonl",
        raw_jsonl_path="data/uniprot_raw.jsonl",
        size=100,
        exclude_sequence=True
    )

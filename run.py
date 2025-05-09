import click
from uniprot_pipeline.pipeline import run_pipeline

@click.command()
@click.option('--mode', type=click.Choice(['bulk', 'incremental']), required=True, help="Pipeline mode")
@click.option('--from-date', default=None, help="Start date for incremental import (YYYY-MM-DD)")
@click.option('--to-date', default=None, help="End date for incremental import (YYYY-MM-DD)")
@click.option('--output-json', required=True, help="Path to save raw JSON response")
@click.option('--output-jsonl', required=True, help="Path to save flattened or merged JSONL")
@click.option('--base', default=None, help="Path to existing base JSONL file (for incremental merge)")
@click.option('--exclude-sequence', is_flag=True, help="Exclude sequence.value fields")
@click.option('--size', default=500, show_default=True, help="Number of entries per API request")
@click.option('--file-format', default="jsonl", type=click.Choice(['json', 'jsonl']), show_default=True, help="Output format")
@click.option('--query', default="organism_id:9606 AND reviewed:true", help="UniProt query string")
def main(mode, from_date, to_date, output_json, output_jsonl, base, exclude_sequence, size, file_format, query):
    """
    CLI entry point for running the UniProt data pipeline.
    """
    run_pipeline(
        mode=mode,
        output_json=output_json,
        output_jsonl=output_jsonl,
        from_date=from_date,
        to_date=to_date,
        base_jsonl=base,
        exclude_sequence=exclude_sequence,
        size=size,
        file_format=file_format,
        query=query
    )

if __name__ == '__main__':
    main()

"""
python run.py \
  --mode incremental \
  --output-json data/uniprot_raw.json \
  --output-jsonl data/uniprot_merged.jsonl \
  --base data/uniprot_bulk.jsonl \
  --from-date 2024-04-01 \
  --exclude-sequence \
  --file-format jsonl \
  --query "organism_id:9606 AND reviewed:true"
"""
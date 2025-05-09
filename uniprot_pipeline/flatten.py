def flatten_json(
    y: dict,
    prefix: str = '',
    max_depth: int = None,
    include_keys: set = None,
    exclude_keys: set = None
) -> dict:
    """
    Recursively flattens a nested JSON object (dicts and lists) into a flat dictionary.
    
    Optionally supports:
    - Max depth flattening
    - Including only specific top-level keys
    - Excluding specific top-level keys

    Args:
        y (dict): The input JSON object to flatten.
        prefix (str): Prefix for keys (used in recursion, leave blank when calling).
        max_depth: Maximum depth to flatten. If None, flattens fully.
        include_keys: Top-level keys to include. If set, only these keys are processed.
        exclude_keys: Top-level keys to exclude.

    Returns:
        dict: A flattened version of the input dictionary.
    """
    out = {}

    def flatten(x, name="", depth=0):
        """Recursive helper to flatten dicts and lists."""
        # Handle inclusion/exclusion at top-level keys (depth == 0)
        if depth == 0:
            top_key = name.partition(".")[0] if "." in name else name
            if include_keys is not None and top_key not in include_keys:
                return
            if exclude_keys is not None and top_key in exclude_keys:
                return

        # Stop recursion if depth limit is reached
        if max_depth is not None and depth >= max_depth:
            out[name] = x
            return

        # Recursive flattening logic
        if isinstance(x, dict):
            for k, v in x.items():
                full_key = f"{name}.{k}" if name else k
                flatten(v, full_key, depth + 1)

        elif isinstance(x, list):
            if all(isinstance(i, dict) for i in x):
                # Flatten each dict in list with index
                for idx, item in enumerate(x):
                    full_key = f"{name}.{idx}" if name else str(idx)
                    flatten(item, full_key, depth + 1)
            else:
                # Preserve non-dict lists as-is
                out[name] = x

        else:
            # Primitive value
            out[name] = x

    flatten(y, prefix, depth=0)
    return out

import json



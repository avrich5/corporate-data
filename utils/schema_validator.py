import json
from pathlib import Path

import jsonschema


def validate_structural_output(data: dict) -> None:
    """
    Validates the generated structural report data against the specs/structural_output.schema.json.
    
    Args:
        data: The structural output dictionary payload.
        
    Raises:
        jsonschema.exceptions.ValidationError: If the data does not conform to the schema.
        FileNotFoundError: If the schema file is missing.
    """
    # Using a relative path starting from the root of the project
    # Assuming this module is run within the project context where specs/ is accessible
    schema_path = Path("specs/structural_output.schema.json")
    
    if not schema_path.exists():
        raise FileNotFoundError(f"Schema not found at path: {schema_path}")
        
    with open(schema_path, "r", encoding="utf-8") as f:
        schema = json.load(f)
        
    jsonschema.validate(instance=data, schema=schema)

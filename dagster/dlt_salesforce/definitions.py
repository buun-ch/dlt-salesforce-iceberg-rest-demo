import sys
from pathlib import Path

# Add dependencies directory to Python path (in project root)
dependencies_dir = Path(__file__).parent.parent / "dependencies"
if str(dependencies_dir) not in sys.path:
    sys.path.insert(0, str(dependencies_dir))

from dagster import Definitions, load_assets_from_modules
from dlt_salesforce import assets  # noqa: TID252

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
)

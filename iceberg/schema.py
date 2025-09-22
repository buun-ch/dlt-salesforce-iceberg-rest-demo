import pyarrow as pa
from dlt.common.libs.pyarrow import columns_to_arrow
from dlt.common.schema import TTableSchema
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DoubleType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
    TimestamptzType,
)


def create_iceberg_schema_from_table_schema(table_schema: TTableSchema) -> Schema:
    """Create Iceberg Schema from dlt TTableSchema using dlt's built-in PyArrow conversion."""

    from pyiceberg.schema import Schema as IcebergSchema
    from pyiceberg.types import IcebergType

    columns = table_schema.get("columns", {})

    try:
        from dlt.common.destination import DestinationCapabilitiesContext

        caps = DestinationCapabilitiesContext.generic_capabilities()
    except ImportError:
        # If import fails, create a minimal caps object
        from dlt.common.destination.capabilities import DestinationCapabilitiesContext

        caps = DestinationCapabilitiesContext.generic_capabilities()

    pa_schema = columns_to_arrow(columns, caps)

    iceberg_fields = []
    field_id = 1

    for field in pa_schema:
        field_type: pa.DataType = field.type  # type: ignore
        field_name: str = str(field.name)
        field_nullable: bool = bool(field.nullable)

        iceberg_type: IcebergType
        if pa.types.is_string(field_type) or pa.types.is_large_string(field_type):
            iceberg_type = StringType()
        elif pa.types.is_integer(field_type):
            iceberg_type = LongType()
        elif pa.types.is_floating(field_type):
            iceberg_type = DoubleType()
        elif pa.types.is_boolean(field_type):
            iceberg_type = BooleanType()
        elif pa.types.is_timestamp(field_type):
            # Check if timestamp has timezone
            if field_type.tz is not None:
                iceberg_type = TimestamptzType()
            else:
                iceberg_type = TimestampType()
        elif pa.types.is_date(field_type):
            iceberg_type = DateType()
        elif pa.types.is_binary(field_type):
            iceberg_type = BinaryType()
        else:
            # Default fallback
            iceberg_type = StringType()

        iceberg_field = NestedField(
            field_id=field_id,
            name=field_name,
            field_type=iceberg_type,
            required=not field_nullable,
        )
        iceberg_fields.append(iceberg_field)
        field_id += 1

    return IcebergSchema(*iceberg_fields)

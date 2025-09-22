#!/usr/bin/env python
"""
Script to dump dlt source environment variable names

Usage:
    python dump_env_vars.py [source_name]

Examples:
    python dump_env_vars.py              # defaults to salesforce
    python dump_env_vars.py salesforce   # explicitly specify salesforce
    python dump_env_vars.py hubspot      # for hubspot source
"""

import importlib
import inspect
from typing import Sequence, Type, get_type_hints

from dlt.common.configuration.specs import BaseConfiguration, known_sections


def get_env_var_name(*sections: str, key: str) -> str:
    """Generate environment variable name using the same logic as dlt"""
    sections_list = [s for s in sections if s]  # Filter out empty sections
    if sections_list:
        env_key = "__".join((*sections, key))
    else:
        env_key = key
    return env_key.upper()


def get_config_fields(
    config_class: Type[BaseConfiguration],
) -> Sequence[tuple[str, str]]:
    """Extract fields from configuration class"""
    fields = []

    # Get class annotations
    if hasattr(config_class, "__annotations__"):
        hints = get_type_hints(config_class)
        for field_name, field_type in hints.items():
            if field_name.startswith("_"):
                continue
            type_str = (
                str(field_type).replace("typing.", "").replace("dlt.common.typing.", "")
            )
            fields.append((field_name, type_str))

    return fields


def dump_dlt_source_env_vars(source_name: str = "salesforce"):
    """Dump dlt source environment variable names for any source"""

    print("=" * 80)
    print(f"{source_name.title()} dlt Source - Environment Variable Names")
    print("=" * 80)
    print()

    # Base sections
    source_section = known_sections.SOURCES  # "sources"

    # Try to dynamically import the source module
    auth_types = []
    client_config_class = None

    try:
        # Try to import the client module for the source
        client_module = importlib.import_module(f"{source_name}.helpers.client")

        # Get all classes from the module
        for name, obj in inspect.getmembers(client_module, inspect.isclass):
            # Check if it's a credential configuration class
            if (
                hasattr(obj, "__module__")
                and obj.__module__ == f"{source_name}.helpers.client"
            ):
                # Check if it's a subclass of BaseConfiguration
                if issubclass(obj, BaseConfiguration):
                    # Special handling for client configuration
                    if "ClientConfiguration" in name:
                        client_config_class = obj
                    # Check if it's an auth class
                    elif name.endswith("Auth"):
                        # Generate a description based on the class name
                        desc = name.replace("Auth", " Authentication")
                        # Convert CamelCase to space-separated words
                        import re

                        desc = re.sub("([A-Z])", r" \1", desc).strip()
                        auth_types.append((name, obj, desc))

        # Sort auth types by name for consistent output
        auth_types.sort(key=lambda x: x[0])

    except ImportError:
        print(f"Warning: Could not import {source_name}.helpers.client")
        print(
            f"Make sure the {source_name} source is installed and has a helpers.client module"
        )
        print()
        return  # Exit early if we can't import the module

    for auth_name, auth_class, description in auth_types:
        print(f"## {auth_name}")
        print(f"   {description}")
        print()

        fields = get_config_fields(auth_class)

        if fields:
            print("   Environment Variables:")
            for field_name, field_type in fields:
                # Include credentials section
                env_var = get_env_var_name(
                    source_section, source_name, "credentials", key=field_name
                )
                print(f'   export {env_var}="..."  # {field_type}')
            print()
        print()

    # Client configuration environment variables
    if client_config_class:
        config_class_name = client_config_class.__name__
        print(f"## {config_class_name}")
        print("   Client Configuration (Common for all auth types)")
        print()

        config_fields = get_config_fields(client_config_class)
        if config_fields:
            print("   Environment Variables:")
            for field_name, field_type in config_fields:
                env_var = get_env_var_name(source_section, source_name, key=field_name)
                print(f'   export {env_var}="..."  # {field_type}')
            print()

    # Show usage examples only if we found auth types
    if auth_types:
        print("=" * 80)
        print("Usage Examples:")
        print("=" * 80)
        print()

        # Use the first auth type for the example
        first_auth_name, first_auth_class, _ = auth_types[0]
        first_auth_fields = get_config_fields(first_auth_class)

        if first_auth_fields:
            print(f"# Example configuration for {first_auth_name}")
            # Show first 3 fields as example
            for field_name, _ in first_auth_fields[:3]:
                env_var = get_env_var_name(
                    source_section, source_name, "credentials", key=field_name
                )
                example_value = f"your_{field_name.lower()}"
                print(f'export {env_var}="{example_value}"')
            print()

        # Show client config examples if available
        if client_config_class:
            print("# Optional: Client configuration")
            config_fields = get_config_fields(client_config_class)
            for field_name, _ in config_fields[
                :2
            ]:  # Show first 2 config fields as example
                env_var = get_env_var_name(source_section, source_name, key=field_name)
                if field_name.lower() == "version":
                    print(f'export {env_var}="60.0"')
                elif field_name.lower() == "domain":
                    print(f'export {env_var}="test"')
                else:
                    print(f'export {env_var}="your_{field_name.lower()}"')
            print()


def print_usage():
    """Print usage information"""
    print("Usage: python dump_env_vars.py <source_name>")
    print()
    print("Dump environment variable names for any dlt source")
    print()
    print("Arguments:")
    print("  source_name    Name of the dlt source (e.g., salesforce, hubspot, stripe)")
    print()
    print("Examples:")
    print("  python dump_env_vars.py salesforce")
    print("  python dump_env_vars.py hubspot")
    print("  python dump_env_vars.py stripe")
    print()
    print("The script will display all available authentication methods and their")
    print(
        "required environment variables in the format SOURCES__<SOURCE>__CREDENTIALS__<FIELD>"
    )


if __name__ == "__main__":
    import sys

    # Check if source name is provided
    if len(sys.argv) < 2:
        print_usage()
        sys.exit(1)

    source_name = sys.argv[1]
    dump_dlt_source_env_vars(source_name)

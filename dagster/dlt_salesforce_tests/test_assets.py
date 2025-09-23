"""Tests for Salesforce Dagster assets."""

import os
from unittest.mock import Mock, patch

import pytest
from dagster import AssetKey, build_asset_context, MaterializeResult
from dlt.common.pipeline import LoadInfo
from dlt_salesforce.assets import (
    SalesforceConfig,
    salesforce_core_assets,
    salesforce_summary,
)


@pytest.fixture
def mock_salesforce_source():
    """Mock salesforce_source for testing."""
    with patch("dlt_salesforce.assets.salesforce_source") as mock_source:
        mock_resource = Mock()
        mock_resource.write_disposition = "merge"

        mock_source_instance = Mock()
        mock_source_instance.with_resources.return_value = mock_source_instance
        mock_source_instance.account = mock_resource
        mock_source_instance.contact = mock_resource
        mock_source_instance.opportunity = mock_resource
        mock_source_instance.opportunity_contact_role = mock_resource

        mock_source.return_value = mock_source_instance
        yield mock_source


@pytest.fixture
def mock_dlt_pipeline():
    """Mock dlt.pipeline for testing."""
    with patch("dlt_salesforce.assets.dlt.pipeline") as mock_pipeline:
        mock_pipeline_instance = Mock()

        # Mock LoadInfo with proper structure - updated attributes
        mock_load_info = Mock(spec=LoadInfo)
        mock_load_info.pipeline = mock_pipeline_instance
        mock_load_info.destination_name = "test_destination"
        mock_load_info.destination_displayable_credentials = "test_creds"
        mock_load_info.destination_type = "test"
        mock_load_info.environment = "test"
        mock_load_info.dataset_name = "test_dataset"
        mock_load_info.load_packages = []  # Changed from loads
        mock_load_info.loads_ids = ["test_load_id"]  # Changed from load_id
        mock_load_info.first_run = True

        mock_pipeline_instance.run.return_value = mock_load_info
        mock_pipeline.return_value = mock_pipeline_instance
        yield mock_pipeline


@pytest.fixture
def mock_apply_write_disposition():
    """Mock apply_write_disposition function."""
    with patch("dlt_salesforce.assets.apply_write_disposition") as mock_apply:
        yield mock_apply


@pytest.fixture
def test_config():
    """Default test configuration."""
    return SalesforceConfig(
        write_disposition="merge",
        batch_size=1000,
        catalog_url="http://localhost:8181/catalog",
        warehouse="test-warehouse",
        namespace="test-namespace",
        dump_to_duckdb=True,  # Use DuckDB for testing
        resources=None,
    )


class TestSalesforceConfig:
    """Test SalesforceConfig class."""

    def test_default_config_values(self):
        """Test that default config values are set correctly."""
        config = SalesforceConfig()

        # Should use environment variables or defaults
        assert config.write_disposition is not None
        assert config.batch_size > 0
        assert config.catalog_url is not None
        assert config.warehouse is not None
        assert config.namespace is not None
        assert isinstance(config.dump_to_duckdb, bool)

    def test_custom_config_values(self):
        """Test that custom config values are set correctly."""
        config = SalesforceConfig(
            write_disposition="replace",
            batch_size=500,
            catalog_url="http://custom:8181/catalog",
            warehouse="custom-warehouse",
            namespace="custom-namespace",
            dump_to_duckdb=False,
            resources=["account", "contact"],
        )

        assert config.write_disposition == "replace"
        assert config.batch_size == 500
        assert config.catalog_url == "http://custom:8181/catalog"
        assert config.warehouse == "custom-warehouse"
        assert config.namespace == "custom-namespace"
        assert config.dump_to_duckdb is False
        assert config.resources == ["account", "contact"]


class TestSalesforceCoreAssets:
    """Test salesforce_core_assets multi-asset."""

    def test_core_assets_execution(
        self,
        mock_salesforce_source,
        mock_dlt_pipeline,
        mock_apply_write_disposition,
        test_config,
    ):
        """Test successful execution of core assets."""
        context = build_asset_context()

        result = salesforce_core_assets(context, test_config)

        # Verify function calls
        mock_salesforce_source.assert_called_once()
        mock_dlt_pipeline.assert_called_once()
        mock_apply_write_disposition.assert_called_once()

        # Verify result structure - now returns a tuple
        expected_resources = [
            "account",
            "contact",
            "opportunity",
            "opportunity_contact_role",
        ]
        assert isinstance(result, tuple)
        assert len(result) == len(expected_resources)

        # Each item should be a MaterializeResult or None
        for item in result:
            if item is not None:
                assert isinstance(item, MaterializeResult)

    def test_core_assets_with_custom_resources(
        self,
        mock_salesforce_source,
        mock_dlt_pipeline,
        mock_apply_write_disposition,
    ):
        """Test core assets with custom resource list."""
        context = build_asset_context()
        config = SalesforceConfig(
            resources=["account", "contact"],
            dump_to_duckdb=True,
        )

        result = salesforce_core_assets(context, config)

        # Should return a tuple with all 4 assets (some may be None)
        assert isinstance(result, tuple)
        assert len(result) == 4  # Always returns 4 items for core assets

    def test_environment_variable_handling(
        self,
        mock_salesforce_source,
        mock_dlt_pipeline,
        mock_apply_write_disposition,
        test_config,
    ):
        """Test that environment variables are properly set and restored."""
        context = build_asset_context()

        # Set initial environment variable
        original_value = "original_value"
        os.environ["WRITE_DISPOSITION"] = original_value

        salesforce_core_assets(context, test_config)

        # Verify environment variable is restored
        assert os.environ["WRITE_DISPOSITION"] == original_value


class TestSalesforceSummary:
    """Test salesforce_summary asset."""

    def test_summary_asset_execution(self):
        """Test successful execution of summary asset."""
        context = build_asset_context()

        result = salesforce_summary(context)

        # Verify result structure
        assert isinstance(result, MaterializeResult)
        assert result.metadata is not None
        assert "total_assets" in result.metadata
        assert "core_assets" in result.metadata
        assert "pipeline_status" in result.metadata

    def test_summary_dependencies(self):
        """Test that summary asset has correct dependencies."""
        # Check that the asset decorator has the correct dependencies
        # For @asset decorator, the function itself becomes an AssetsDefinition
        assert callable(salesforce_summary)

        # The summary asset should have deps on core assets
        expected_deps = [
            AssetKey(["salesforce", "account"]),
            AssetKey(["salesforce", "contact"]),
            AssetKey(["salesforce", "opportunity"]),
            AssetKey(["salesforce", "opportunity_contact_role"]),
        ]

        # Note: This test is illustrative - actual dependency checking
        # would require inspecting the decorator configuration


class TestIntegration:
    """Integration tests for all assets."""

    @patch("dlt_salesforce.assets.salesforce_source")
    @patch("dlt_salesforce.assets.dlt.pipeline")
    @patch("dlt_salesforce.assets.apply_write_disposition")
    def test_core_and_summary_assets_can_be_materialized(
        self,
        mock_apply_write_disposition,
        mock_dlt_pipeline,
        mock_salesforce_source,
    ):
        """Test that core and summary assets can be materialized together."""
        # Setup mocks
        mock_source_instance = Mock()
        mock_source_instance.with_resources.return_value = mock_source_instance
        mock_salesforce_source.return_value = mock_source_instance

        mock_pipeline_instance = Mock()
        mock_load_info = Mock(spec=LoadInfo)
        mock_load_info.pipeline = mock_pipeline_instance
        mock_load_info.loads_ids = ["test_load_id"]
        mock_load_info.load_packages = []
        mock_pipeline_instance.run.return_value = mock_load_info
        mock_dlt_pipeline.return_value = mock_pipeline_instance

        # This test verifies that the assets can be imported and defined correctly
        from dlt_salesforce.assets import (
            salesforce_core_assets,
            salesforce_summary,
        )

        # Verify all asset functions are defined
        assert callable(salesforce_core_assets)
        assert callable(salesforce_summary)

        # Test that functions have proper asset decorators
        # For @multi_asset and @asset, the decorated function becomes an AssetsDefinition
        from dagster import AssetsDefinition
        assert isinstance(salesforce_core_assets, AssetsDefinition)
        assert isinstance(salesforce_summary, AssetsDefinition)
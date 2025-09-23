# Salesforce Sample Data

This directory contains several Snowfakery recipes for generating realistic Salesforce test data at different scales.

## Setup

Install prerequisites:

```bash
uv tool install snowfakery
uv tool install cumulusci
```

Clone Snowfakery repo and copy the recipe files:

```bash
git clone https://github.com/SFDO-Tooling/Snowfakery.git
cp *.yml ./Snowfakery
cd Snowfakery
```

## Connect to Salesforce

Connect to existing org (Developer Edition, Sandbox, etc.):

```bash
cci org connect dev
```

Set default org:

```bash
cci org default dev
```

## Available Recipes

### 1. salesforce_small.recipe.yml

Small-scale dataset for basic testing:

- **Account**: 50 companies
- **Contact**: 150 people
- **Opportunity**: 100 deals
- **Use case**: Initial testing, development environments
- **Execution time**: ~10 seconds

```bash
cci task run generate_and_load_from_yaml \
  -o generator_yaml salesforce_small.recipe.yml \
  --org dev
```

### 2. salesforce_medium_scale.recipe.yml ⭐ Recommended

Medium-scale dataset with realistic business relationships:

- **Account**: 200 companies
- **Contact**: 800 people (average 4 per company)
- **Opportunity**: 600 deals (average 3 per company)
- **OpportunityContactRole**: 1,200 deal contacts
- **Use case**: Demo environments, user training, comprehensive testing
- **Execution time**: ~30 seconds

```bash
cci task run generate_and_load_from_yaml \
  -o generator_yaml salesforce_medium_scale.recipe.yml \
  --org dev
```

### 3. salesforce_large_scale.recipe.yml

Large-scale dataset for performance testing:

- **Account**: 1,000 companies
- **Contact**: 5,000 people (average 5 per company)
- **Opportunity**: 3,000 deals (average 3 per company)
- **OpportunityContactRole**: 6,000 deal contacts
- **Use case**: Performance testing, load testing, large org simulation
- **Execution time**: ~2-3 minutes

```bash
cci task run generate_and_load_from_yaml \
  -o generator_yaml salesforce_large_scale.recipe.yml \
  --org dev
```

## CSV Export (Alternative)

Generate CSV files instead of uploading directly:

```bash
snowfakery salesforce_medium_scale.recipe.yml \
  --output-format csv \
  --output-folder csv
```

## Data Characteristics

- **Industry Distribution**: Technology (15%), Healthcare (12%), Finance (12%), etc.
- **Company Types**: Customer-Direct (40%), Prospect (25%), Partners (35%)
- **Deal Stages**: Closed Won (20%), Closed Lost (15%), Active Pipeline (65%)
- **Contact Roles**: Sales/Marketing Managers, Technical roles, Executives

## Resources

- [Snowfakery Documentation](https://snowfakery.readthedocs.io/)
    - [Using Snowfakery with Salesforce - Snowfakery documentation](https://snowfakery.readthedocs.io/en/docs/salesforce.html)
- [CumulusCI Documentation](https://cumulusci.readthedocs.io/)

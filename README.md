# DBT Project for Delta Tables

This repository contains DBT models structured into three stages: staging, intermediate, and prepared. These models are designed to populate final Delta tables in Databricks, facilitating analytics and reporting.

## Project Structure

- `models/`
  - `staging/` - Contains initial staging models which perform preliminary transformations on raw data.
  - `intermediate/` - Contains models that perform more complex transformations and business logic.
  - `prepared/` - Contains models that prepare data for final output, including aggregations and final cleansing.
- `dbt_profiles.yml` - Configuration file for setting up DBT profiles.
- `data/` - Sample data for testing and development.
- `tests/` - Contains DBT tests for data validation.
- `macros/` - DBT macros used across models for common tasks like surrogate key generation.

## Setup

### Prerequisites

- DBT (Data Build Tool)
- Access to a Databricks environment
- An appropriate Databricks cluster configured to run Delta Lake.

### Installation

1. Clone this repository:
   ```bash
   git clone <repository-url>
   cd dbt_project

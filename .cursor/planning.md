# Automated Secure View Model Generation Plan

## Overview
Create an automated GitHub Actions workflow that periodically checks a mapping table for secure views and generates corresponding dbt models with no_materialization config.

## Components Needed

1. Python Script for Model Generation
   - Query Snowflake mapping table (SNOW_SV_LIST)
   - Extract source/upstream table and secure view pairs
   - Generate dbt model files with proper structure
   - Handle model cleanup/removal for deprecated mappings

2. GitHub Actions Workflow
   - Schedule regular runs (e.g., daily/weekly)
   - Set up Snowflake connection
   - Execute model generation script
   - Create PR with changes if needed

## Implementation Steps

### 1. Create Snowflake Connection Setup
- [ ] Create service account for GitHub Actions
- [ ] Set up repository secrets for Snowflake credentials
- [ ] Create connection handling utilities

### 2. Develop Model Generation Script
- [ ] Create Python script to:
  - Query SNOW_SV_LIST table
  - Parse table relationships
  - Generate model files using template
  - Handle file operations (create/update/delete)
  - Add logging and error handling
  - Include dry-run option

### 3. Set Up GitHub Actions Workflow
- [ ] Create workflow YAML with:
  - Scheduled trigger
  - Snowflake authentication
  - Python environment setup
  - Script execution
  - PR creation logic
  - Error notifications

### 4. Testing & Validation
- [ ] Test script locally
- [ ] Validate generated models
- [ ] Test GitHub Actions workflow
- [ ] Verify PR creation process

### 5. Documentation & Maintenance
- [ ] Document setup process
- [ ] Add monitoring/alerting
- [ ] Create maintenance guide

## Technical Details

### Model Template Structure
```sql
-- semantic ref for lineage continuity
-- {{ ref('SOURCE_TABLE') }}

{{
    config(
        materialized='no_materialization'
    )
}}

select
    *
from {{target.database}}.{{target.schema}}.SECURE_VIEW_NAME
```

### Required Snowflake Permissions
- Read access to SNOW_SV_LIST table
- Read access to source tables
- Read access to secure views

### GitHub Actions Schedule
Initial recommendation: Run daily at off-peak hours
```yaml
schedule:
  - cron: '0 2 * * *'  # Run at 2 AM UTC daily
```

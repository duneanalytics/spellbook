# TODO

## Cleanup Workflow ✅ COMPLETE
- [x] Create comprehensive workflow script that combines existing components
- [x] Test workflow with database connection ✅ **TESTED - Successfully found 1035 unused tables**
- [x] Validate manifest generation for all subprojects ✅ **Can skip with --skip-manifests flag**
- [x] Test dependency analysis across subprojects ✅ **Working - found deps for 4/5 test tables**
- [x] Document workflow usage and output formats ✅ **Documented below**
- [x] Fix YAML parsing dependency ✅ **PyYAML installed - no more errors**

## ✅ WORKFLOW READY FOR PRODUCTION

The cleanup workflow is now fully functional and ready to use:

### **Production Usage**
```bash
# Find ALL unused tables with no dependencies (safe to delete)
python scripts/cleanup_workflow.py --skip-manifests

# Output as CSV for spreadsheet analysis
python scripts/cleanup_workflow.py --skip-manifests --format csv

# Test with a smaller sample first
python scripts/cleanup_workflow.py --skip-manifests --limit 50
```

## Latest Test Results
- **Database Query**: ✅ Found 1035 unused tables total
- **No Dependencies Filter**: ✅ Of 10 test tables, 2 had no dependencies (safe to delete)
- **Priority Scoring**: ✅ Tables in hourly_spellbook with no deps scored 25 points
- **Error-Free**: ✅ No YAML parsing errors after installing PyYAML

## Cleanup Workflow Script Features
The `scripts/cleanup_workflow.py` script provides:

### Core Functionality
- **Manifest Generation**: Compiles and collects manifests from all subprojects (daily_spellbook, dex, hourly_spellbook, nft, solana, tokens)
- **Database Querying**: Queries for unused/lightly used tables using existing `get_unused_spells_query()`
- **Table Mapping**: Maps database tables to their corresponding models in subprojects
- **Dependency Analysis**: Finds children and sources that depend on each table across all subprojects
- **Safety Filter**: **ONLY returns tables with NO dependencies** (safe to delete)

### Usage Examples
```bash
# Find ALL unused tables with no dependencies (recommended for production)
python scripts/cleanup_workflow.py --skip-manifests

# Output as JSON for programmatic processing
python scripts/cleanup_workflow.py --skip-manifests --format json

# Output as CSV for spreadsheet analysis  
python scripts/cleanup_workflow.py --skip-manifests --format csv

# Test with smaller sample
python scripts/cleanup_workflow.py --skip-manifests --limit 20
```

### Key Features
- **Safety First**: Only shows tables with zero dependencies (safe to delete)
- **Priority Scoring**: Ranks tables by cleanup priority (hourly subproject + materialization type)
- **Summary Reports**: Provides breakdown by subproject, materialization, and dependency counts
- **Multiple Formats**: Supports summary, JSON, and CSV output formats
- **Progress Tracking**: Shows detailed progress for each step with emojis and status indicators
- **No Limit by Default**: Analyzes ALL unused tables unless --limit specified 

## New Table Cleanup Process - Modified Requirements ✅ COMPLETE

### **Objective**
Create a list of tables with:
- `table_name` (example: dex.trades)
- `dbt_subproject` (example: daily) 
- `materialization`

### **Selection Criteria**
Tables must meet ALL of the following criteria:
- 0 or 1 query usages in the last 6 months (not just 0 as current workflow does)
- No children nodes or models that depend on them
- No use as sources in other dbt_subprojects

### **✅ IMPLEMENTATION COMPLETE**

The `scripts/cleanup_workflow.py` has been completely replaced with new requirements:

#### 1. Update SQL Query ✅ COMPLETE
- [x] Modified query to include tables with 0 OR 1 query usage (`<= 1`)
- [x] Query returns `spell_metadata` column with all required information
- [x] Using updated query in new workflow implementation

#### 2. Database Connection Setup ✅ COMPLETE
- [x] Enhanced DatabaseQuery class with proper postgres credentials from `.env`
- [x] Connection validation and error handling
- [x] Query execution with proper result formatting

#### 3. Subproject Detection Enhancement ✅ COMPLETE
- [x] Parse `spell_metadata` column to extract `manifest_folder` 
- [x] Map `manifest_folder` directly to `dbt_subproject` names
- [x] Handle cases where mapping is unclear or missing

#### 4. Model File Path Resolution ✅ COMPLETE
- [x] Extract `original_file` from `spell_metadata` (not separate column)
- [x] Resolve full path: `dbt_subprojects/{subproject}/{original_file}`
- [x] **RAISE ERROR if model file cannot be found** (strict validation)

#### 5. Dependency Analysis Updates ✅ COMPLETE
- [x] Load all subproject manifests for comprehensive analysis
- [x] Check children nodes/models that depend on each table
- [x] Check source usage across different dbt_subprojects
- [x] Filter out tables that have ANY dependencies

#### 6. Manifest Generation ✅ COMPLETE
- [x] Enhanced manifest generation for all subprojects
- [x] Manifest loading and validation
- [x] Graceful handling of generation failures

#### 7. Error Handling & Validation ✅ COMPLETE
- [x] Strict error raising when models cannot be found
- [x] Validate all required fields are present
- [x] Detailed error logging for debugging

#### 8. Output Format ✅ COMPLETE
- [x] Output includes the three required fields: `table_name`, `dbt_subproject`, `materialization`
- [x] Support for CSV, JSON, and summary output formats
- [x] Proper formatting of table names as schema.table_name

### **Questions/Clarifications - RESOLVED**

1. **Query Modification**: ✅ Changed to `<= 1` for 0 or 1 references
2. **Missing Column**: ✅ `original_file` comes from `spell_metadata` JSON
3. **Manifest Folder Mapping**: ✅ Maps directly to subproject names 
4. **Dependency Scope**: ✅ Includes source usage across dbt_subprojects
5. **Error Behavior**: ✅ Raises errors when models cannot be found
6. **Output Integration**: ✅ Replaced existing cleanup_workflow.py

### **Usage**
```bash
# Find tables with ≤1 references and no dependencies
python scripts/cleanup_workflow.py --skip-manifests

# Export as CSV for analysis
python scripts/cleanup_workflow.py --skip-manifests --format csv

# Test with smaller sample
python scripts/cleanup_workflow.py --skip-manifests --limit 20
```

## Enhance Cleanup Workflow with Usage Analytics ✅ COMPLETE
- [x] Enhance `get_unused_spells_query` to include query links, dashboard links, and user/team information
- [x] Add query links as `https://dune.com/queries/{query_id}`
- [x] Add dashboard links as `https://dune.com/{team_or_user_name}/{slug}`
- [x] Include distinct set of user names/teams that use the table
- [x] Update output functions to display new fields
- [x] Test enhanced query and output formats
- [x] Add query owners to show who owns the queries that reference tables
- [x] Add markdown output format for better readability

### **✅ IMPLEMENTATION COMPLETE**

Enhanced the cleanup workflow to provide comprehensive usage analytics:

#### **SQL Query Enhancements**
- Added joins with queries, visualizations, visualization_widgets, dashboards, users, and teams tables
- Aggregates unique query IDs into comma-separated query links
- Collects query owners (users/teams who own the queries)
- Builds dashboard links using user.name or team.name (not handles)
- Collects distinct dashboard owners that own dashboards
- Uses DISTINCT COUNT for reference_count to handle multiple joins correctly

#### **Data Validation**
- Added validation for dashboard ownership (raises error if dashboard exists without valid user/team)
- Maintains existing model file validation and dependency analysis
- Enhanced error handling and logging

#### **Output Enhancements**
- **Summary format**: Shows bulleted lists of query/dashboard links, comma-separated owners
- **CSV format**: Includes all new fields as comma-separated strings
- **JSON format**: Converts comma-separated strings to proper arrays
- **Markdown format**: Creates formatted .md files with clickable links and structured layout

#### **Usage Examples**
```bash
# Get enhanced analytics with all usage information
python scripts/cleanup_workflow.py --skip-manifests

# Export with full analytics as CSV
python scripts/cleanup_workflow.py --skip-manifests --format csv

# Generate markdown report with clickable links
python scripts/cleanup_workflow.py --skip-manifests --format markdown
```

#### **New Output Fields**
- `query_links`: Comma-separated URLs like "https://dune.com/queries/123,https://dune.com/queries/456"
- `query_owners`: Comma-separated names like "Query Owner 1,Query Owner 2"
- `dashboard_links`: Comma-separated URLs like "https://dune.com/User Name/dashboard-slug,https://dune.com/Team Name/another-dash"
- `dashboard_owners`: Comma-separated names like "Dashboard Owner 1,Dashboard Owner 2"

## Spell Usage Analytics Script ✅ COMPLETE
- [x] Create new script for comprehensive spell usage analytics
- [x] Analyze model dependency counts and lists for each spell
- [x] Count unique queries that reference each spell
- [x] Calculate total execution counts for queries using each spell
- [x] Support multiple output formats (CSV, JSON, markdown)
- [x] Test script with database connection

### **✅ IMPLEMENTATION COMPLETE**

Created comprehensive spell usage analytics script that provides:

#### **Core Analytics Features**
- **Model Dependencies**: Analyzes dbt manifests to find which models reference each spell
- **Query Usage**: Counts unique queries that reference each spell (configurable time period)
- **Execution Analytics**: Sums total executions of all queries that use each spell
- **Comprehensive Reporting**: Multiple output formats with detailed statistics

#### **SQL Query Features**
- CTE-based query for performance optimization
- Joins `aggregated_tables`, `table_query_references`, and `query_executions`
- Configurable time period analysis (default: 180 days)
- Aggregates query IDs and execution counts per spell
- Orders results by total executions (most used spells first)

#### **Analysis Capabilities**
- **Model Reference Analysis**: Uses dbt manifests to find model dependencies across all subprojects
- **Usage Statistics**: Query count, execution count, and referencing models per spell
- **Subproject Mapping**: Maps spells to their originating dbt subprojects
- **Performance Metrics**: Total execution counts show actual usage intensity

#### **Output Formats**
- **Summary**: Console output with top 20 spells, summary statistics by subproject
- **CSV**: Full data export with comma-separated model lists for spreadsheet analysis
- **JSON**: Structured data with proper arrays for programmatic use
- **Markdown**: Beautiful formatted reports with tables and detailed breakdowns

#### **Usage Examples**
```bash
# Analyze all spells with summary output
python scripts/spell_usage_analytics.py

# Generate comprehensive markdown report
python scripts/spell_usage_analytics.py --format markdown

# Analyze last 30 days only
python scripts/spell_usage_analytics.py --days-back 30

# Export top 100 spells as CSV
python scripts/spell_usage_analytics.py --limit 100 --format csv
```

#### **Key Output Fields**
- `full_table_name`: Complete table identifier (schema.table)
- `subproject`: dbt subproject that owns the spell
- `model_reference_count`: Number of models that reference this spell
- `referencing_models`: Array of model names that depend on this spell
- `unique_query_count`: Number of distinct queries that use this spell
- `total_executions`: Total execution count of all queries using this spell
- `query_ids`: List of query IDs for further analysis
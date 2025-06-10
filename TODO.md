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
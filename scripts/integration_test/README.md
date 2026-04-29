# Integration Tests

This directory contains integration tests where applicable for scripts that are used to help automate some code review steps.

## Installation

If dependencies have not been installed, run this from the root of the repo (`spellbook`)
```bash
uv sync --locked
```

## Usage
We won't set this up to run automatically because it's still easy to overwhelm the API and cause a timeout. But we will 
leave this test here for future extension of this test.

From the test directory: 

For the price token checker `check_tokens.py`, generate input files with `generate_test_files_check_tokens.py`
This will create diff files in test_diffs_tokens directory from the most recent PRs that included edits to `models/prices/prices_tokens.sql`
```python
uv run python generate_test_files_check_tokens.py
```

Once the test files have been generated, run an integration test with `test_check_tokens.py`. This will iterate
through each test file and confirm that only Assertion Errors occur or no errors. Assertion Errors are returned
when the  API does not match what the values are proposed by the API. 
```python
uv run python test_check_tokens.py
```

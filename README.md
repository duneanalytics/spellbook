# Abstractions

This repository tracks user-created abstractions to the Dune Analytics data platform. Contributions in the form of issues and pull requests are very much welcome here.

## Views

Intially, abstractions in Dune exists in the form of SQL views. In the folder `schema/{{schema_name}}` views for the `{{schema_name}}`-schema are defined. 

Each file should only have _one_ view, and the filename should match the name of the view. I.e. if the view is defined as `CREATE VIEW view_ctokens AS`, then the file should be called `view_ctokens.sql`. 

View names should be prefixed by `view_` and column names should be `lowercase_snake_case`. 

More info in `pull_request_template.md`.


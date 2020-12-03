# Labels

You can populate the labels in Dune by submitting a query to this repo!
The query MUST return `address bytea`, `label text`, `author text` `type text`.
It may optionally use a `{{timestamp}}` template variable which Dune will use to incrementally sync this label query.
The timestamp will be replaced with the last timestamp a label was synced using this strategy.
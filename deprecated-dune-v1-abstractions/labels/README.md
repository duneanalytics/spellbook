# Labels

You can populate the labels in Dune Analytics by submitting a Dune query to this repo! This is a great way to do scalable and novel tagging of addresses.

The query MUST return `address bytea`, `label text`, `type text`, `author text`. The last column should be your Dune username.

You can also optionally add a `{{timestamp}}` template variable which Dune will use to incrementally sync this label query. The timestamp will be replaced with the last timestamp a label was synced using this strategy.

* Read more about labels in our docs [here](https://hackmd.io/k71ZUSTxQVKGqOcvR6OXnw?view#%F0%9F%93%A5-Adding-labels).
* You can also browse addresses and add new ones at our [labels page](https://duneanalytics.com/labels) 

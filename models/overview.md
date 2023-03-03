{% docs __overview__ %}

### Welcome!

Welcome to the documentation for Spellbook! 🧙‍♂️🧙‍♀️

#### Navigation

Looking at the source code on [Github](https://github.com/duneanalytics/spellbook):

- The `Spells` section mirrors the `models` directory - i.e. abstracted tables built by the community
- The `Sources` section mirrors the `sources` directory - i.e. raw and decoded data provided by Dune

### Graph Exploration

You can click the blue icon on the bottom-right corner of the page to view the lineage graph of Dune spells.

On spell pages, you'll see the immediate parents and children of the spell you're exploring. By clicking the `Expand`
button at the top-right of this lineage pane, you'll be able to see all of the spells that are used to build,
or are built from, the spell you're exploring.

Once expanded, you'll be able to use the `--select` and `--exclude` spell selection syntax to filter the
spells in the graph. For more information on spell selection, check out the [dbt docs](https://docs.getdbt.com/docs/model-selection-syntax).

Note that you can also right-click on spells to interactively filter and explore the graph.

---

### More information

- [Why we built spellbook](https://dune.com/blog/spellbook)
- Read the [Dune docs](https://dune.com/docs/spellbook)
- Contribute on [Github](https://github.com/duneanalytics/spellbook)

{% enddocs %}
# Best practices

## How to efficiently join tables
The DuneSQL query planner can reveal a lot of helpful information to efficiently write certain logic within queries. One takeaway is how the planner interprets joins. The order of the tables or subqueries within the joins can improve efficiency of the query, resulting in better performance.

In general, in order to consume less memory and run faster, larger tables should be written first – as in the left side of the join. A common query example:
```sql
select *
from decoded d
join transactions t
on d.tx_hash = t.hash
```

The decoded source tables are smaller than the base transactions tables. For a more efficient output, the query could be tweaked to:
```sql
select *
from transactions t
join decoded d
on d.tx_hash = t.hash
```
If it is a left or right join, the join type must be switched as well:
```sql
select * 
from decoded d
left join transactions t
on d.tx_hash = t.hash
```
For a more efficient output, the query could be tweaked to:
```sql
select *
from transactions t
right join decoded d
on d.tx_hash = t.hash
```
With that said, this is more important to keep in mind when seeing spells are taking longer than anticipated to run rather than retroactively fixing older spells which don’t have the correct order in joins. The Dune team has taken some time to fix existing spells which ran longer than expected, by tweaking joins within. One example is PR [here](https://github.com/duneanalytics/spellbook/pull/4003/files). In most cases, the query planner will figure out the optimal join on its own. This is recommended when the planner gets it incorrect, resulting in poor performance.

## How to efficiently use UNION statements
In general, the query planner shows it’s most efficient to use UNION ALL or UNION DISTINCT, rather than simply UNION. If there are duplicates in the data being unioned, use UNION DISTINCT, otherwise use UNION ALL. This is a good general rule of thumb to follow for all spells.

## Leverage DuneSQL data types & functions
For best performance, maintain the DuneSQL data types from the source tables or explicitly cast to these types as necessary. Basic SQL operations on these types will be more efficient. The main data types in consideration:
- `varbinary`
- `uint256 / int256`

When working with these data types, rather than cast to other types to meet requirements for a certain function, it’s best to leverage the new functions for these types. The main functions in consideration:
- `bytearray_substring()`
- `bytearray_to_uint256()`
- `bytearray_length()`
- …many more of this type

## Leverage Jinja syntax & functionality
Where possible, apply Jinja syntax as much as possible:
- source & refs
- for loops
- variables
- for more information: https://docs.getdbt.com/docs/build/jinja-macros#jinja

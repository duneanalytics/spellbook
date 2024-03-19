{{ config(
     schema = 'depin'
	  , alias = 'revenue'
	  , materialized = 'view'
) }}
with
    results as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("geodnet_revenue")
                ]
            )
        }}
    )
select 
    date, 
    chain,
    name,
    revenue
from results


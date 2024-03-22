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
                    ref("geodnet_polygon_revenue")
                ]
            )
        }}
    )
select 
    date, 
    blockchain,
    project,
    revenue
from results


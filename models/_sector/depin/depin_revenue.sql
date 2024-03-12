{{ config(materialized="table") }}
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


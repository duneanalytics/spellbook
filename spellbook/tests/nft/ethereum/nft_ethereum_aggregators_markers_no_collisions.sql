WITH unit_test as (
    SELECT a.*
    from {{ ref('nft_ethereum_aggregators_markers') }} a
    inner join {{ ref('nft_ethereum_aggregators_markers') }} b
    on a.router_name != b.router_name
    and a.hash_marker LIKE '%'|| b.hash_marker
)

select * from unit_test

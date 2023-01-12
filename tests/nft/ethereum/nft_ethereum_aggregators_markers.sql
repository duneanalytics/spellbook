WITH unit_test as (
    SELECT *
    from {{ ref('nft_ethereum_aggregators_markers') }} a
    inner join {{ ref('nft_ethereum_aggregators_markers') }} b
    on a.router_website != b.router_website
    and a.hash_marker LIKE '%'|| b.hash_marker
)

select * from unit_test

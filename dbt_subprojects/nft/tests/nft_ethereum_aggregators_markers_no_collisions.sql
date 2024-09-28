WITH unit_test as (
    SELECT a.*
    from {{ ref('nft_ethereum_aggregators_markers') }} a
    inner join {{ ref('nft_ethereum_aggregators_markers') }} b
    on a.router_name != b.router_name
    and bytearray_starts_with(bytearray_reverse(a.hash_marker),bytearray_reverse(b.hash_marker))
)

select * from unit_test

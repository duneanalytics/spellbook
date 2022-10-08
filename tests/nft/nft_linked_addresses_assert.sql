-- Check against manually selected seed data
WITH match_address_count as
(
    SELECT COUNT(1) as address_count
    FROM {{ ref('nft_linked_addresses') }} a
    JOIN {{ ref('nft_linked_addresses_postgres') }} test_data ON test_data.master_address = a.master_address and test_data.alternative_address = a.alternative_address
),

seed_data_count as
(
    SELECT COUNT(1) as seed_address_count
    FROM {{ ref('nft_linked_addresses_postgres') }}
)

select 1 as result
from match_address_count
join seed_data_count
where address_count < seed_address_count * 0.95
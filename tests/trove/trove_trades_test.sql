with unit_test1 as (
    -- arbitrum
    select case
        when amount_usd = 86.0625 then true
        else false
    end as test
    from {{ ref('trove_trades') }}
    where tx_hash = '0x3a95b7e26348123249dc31eac622e6be9f5c667c8c5344d546a02d99844fe03b'
        and seller = '0x9b80261b7be199b293d0587899b2109abbb45201'
        and nft_contract_address = '0x7480224ec2b98f28cee3740c80940a2f489bf352'
),
unit_test2 as (
    -- ethereum
    select case
        when amount_usd = 70.9535 then true
        else false
    end as test
    from {{ ref('trove_trades') }}
    where tx_hash = '0x4723cb13fd4403a46c6c4032546c6f422b99f01e61e854a984d692856001acbb'
        and buyer = '0x9634d22bed13b660c1970ed5b1f328d4b6a4361f'
        and currency_contract = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
)

select * from (
    select 'test1' as test_no, * from unit_test1
    union all
    select 'test2' as test_no, * from unit_test2
)
where test = false

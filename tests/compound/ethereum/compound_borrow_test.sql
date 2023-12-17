with unit_test1 as (
    -- cErc20 borrow
    select case
        when abs(amount - 44.5596938306667) < 1e-9 then true
        else false
    end as test
    from {{ ref('compound_ethereum_borrow') }}
    where tx_hash = 0x0326269a2ba51eab9601cad4de2507abbbf709c19cc897ffa2c1d0482bd5c692
        and borrower = 0xe219205d3dddf9275170ae5ec8aefa11cd41d6f4
        and token_address = 0x0d8775f648430679a709e98d2b0cb6250d2887ef
),
unit_test2 as (
    -- cEther borrow
    select case
        when amount = 0.5 then true
        else false
    end as test
    from {{ ref('compound_ethereum_borrow') }}
    where tx_hash = 0xb1a56cee663a771eead84c4a4be15ece601c56df708b369aeacb5c271dda6699
        and borrower = 0xabfe00f81c2b9734c2fecec3f1996e18611ce658
        and token_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
),
unit_test3 as (
    -- cErc20 repayBorrow
    select case
        when usd_amount = -2127.8925117492 then true
        else false
    end as test
    from {{ ref('compound_ethereum_borrow') }}
    where tx_hash = 0x30cf18690db1e78604a53f7b66ef255747af2b0f40ea1a92c44de849372be0d9
        and repayer = 0x67bbbccddd394c2cfa15d3958904858b21040cc6
        and token_address = 0x2260fac5e5542a773aa44fbcfedf7c193bc2c599
),
unit_test4 as (
    -- cEther repayBorrow
    select case
        when usd_amount = -1385143.24 then true
        else false
    end as test
    from {{ ref('compound_ethereum_borrow') }}
    where tx_hash = 0x7bf84079666cd48a06d0cbf93b28d0adb20dde5a4b6135689fa5746449136e53
        and repayer = 0xf859a1ad94bcf445a406b892ef0d3082f4174088
        and token_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
)

select * from (
    select 'test1' as test_no, * from unit_test1
    union all
    select 'test2' as test_no, * from unit_test2
    union all
    select 'test3' as test_no, * from unit_test3
    union all
    select 'test4' as test_no, * from unit_test4
)
where test = false

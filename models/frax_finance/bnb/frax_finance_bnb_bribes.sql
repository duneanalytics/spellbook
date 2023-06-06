{{ config(
    alias = 'bribes',
    materialized = 'table',
    file_format = 'delta',
    unique_key = ['week_start', 'week_end', 'contract_address'],
    post_hook='{{ expose_spells(\'["bnb"]\',
                                    "project",
                                    "frax_finance",
                                    \'["vahid"]\') }}'
    )
}}


with base_date as (
    WITH t1 AS (
        SELECT
            explode(
                sequence(
                    to_date('2023-01-05', 'yyyy-MM-dd'),
                    current_date,
                    interval '7 days'
                )
            ) AS week_array
    )
    SELECT
        week_array AS week_start,
        weekofyear(date_sub(week_array, 4)) AS week,
        date_add(week_array, 7) AS week_end
    FROM
        t1
),
pairs as (
    select
        distinct contract_address,
        output_0 as token0,
        output_1 as token1
    from
        {{ source('thena_fi_bnb', 'Pair_call_tokens') }}
    where
        (
            output_0 in (
                '0x90c97f71e18723b0cf0dfa30ee176ab653e89f40',
                '0x64048a7eecf3a2f1ba9e144aac3d7db6e58f555e'
            )
            or output_1 in (
                '0x90c97f71e18723b0cf0dfa30ee176ab653e89f40',
                '0x64048a7eecf3a2f1ba9e144aac3d7db6e58f555e'
            )
        )
        and date(call_block_time) = (
            select
                max(date(call_block_time))
            from
                {{ source('thena_fi_bnb', 'Pair_call_tokens') }}
            where
                (
                    output_0 in (
                        '0x90c97f71e18723b0cf0dfa30ee176ab653e89f40',
                        '0x64048a7eecf3a2f1ba9e144aac3d7db6e58f555e'
                    )
                    or output_1 in (
                        '0x90c97f71e18723b0cf0dfa30ee176ab653e89f40',
                        '0x64048a7eecf3a2f1ba9e144aac3d7db6e58f555e'
                    )
                )
        )
        and call_success = true
),
reserves as (
    select
        a.contract_address,
        call_block_time,
        CAST(output__reserve0 AS DECIMAL(38, 0)) / POW(10, 18) AS reserve0,
        CAST(output__reserve1 AS DECIMAL(38, 0)) / POW(10, 18) AS reserve1,
        row_number() over(
            partition by date(call_block_time),
            a.contract_address
            order by
                call_block_time desc
        ) as rn
    from
        {{ source('thena_fi_bnb', 'Pair_call_getReserves') }} a
        inner join pairs b on a.contract_address = b.contract_address
    where
        date(call_block_time) >= date('2023-01-04')
),
TVL as (
    select
        a.call_block_time as block_time,
        a.contract_address,
        b.token0,
        b.token1,
case
            when token0 = '0x90c97f71e18723b0cf0dfa30ee176ab653e89f40' then reserve0
            when token1 = '0x90c97f71e18723b0cf0dfa30ee176ab653e89f40' then reserve1
        end as frax_reserve,
case
            when token0 = '0x64048a7eecf3a2f1ba9e144aac3d7db6e58f555e' then reserve0
            when token1 = '0x64048a7eecf3a2f1ba9e144aac3d7db6e58f555e' then reserve1
        end as frxETH_reserve
    from
        reserves a
        left join pairs b on a.contract_address = b.contract_address
    where
        rn = 1
),
prices_raw as (
    select
        *,
        row_number() over(
            partition by date(hour),
            contract_address
            order by
                hour desc
        ) as rn
    from
        {{ ref('dex_prices') }}
    where
        blockchain = 'ethereum'
        and contract_address in (
            '0x5e8422345238f34275888049021821e8e08caa1f',
            '0x853d955acef822db058eb8505911ed77f175b99e',
            '0x3432b6a60d23ca0dfca7761b7ab56459d9c964d0'
        )
        and date(hour) >= date('2023-01-04')
),
prices as (
    select
        day,
        contract_address,
        median_price as price
    from
        prices_raw
    where
        rn = 1
),
TVL_USD as (
    select
        date(block_time) as block_time,
        a.contract_address,
        frax_reserve,
        frxETH_reserve,
        cast(frax_reserve as double) * b.price as frax_tvl_usd,
        cast(frxETH_reserve as double) * c.price as frxETH_tvl_usd
    from
        TVL a
        left join (
            select
                day,
                price
            from
                prices
            where
                contract_address = '0x853d955acef822db058eb8505911ed77f175b99e'
        ) b on date(a.block_time) = b.day
        left join (
            select
                day,
                price
            from
                prices
            where
                contract_address = '0x5e8422345238f34275888049021821e8e08caa1f'
        ) c on date(a.block_time) = c.day
),
TVL_sum as (
    select
        date(block_time) as block_time,
        contract_address,
        sum(frax_reserve) as frax_reserve,
        sum(frxETH_reserve) as frxETH_reserve,
        sum(frax_tvl_usd) as frax_tvl,
        sum(frxETH_tvl_usd) as frxETH_tvl
    from
        TVL_USD
    group by
        1,
        2
),
cte as (
    select
        *
    from
        (
            VALUES
                (
                    '0x8a420aaca0c92e3f97cdcfdd852e01ac5b609452',
                    'sAMM-ETH/frxETH'
                ),
                (
                    '0x49ad051f4263517bd7204f75123b7c11af9fd31c',
                    'sAMM-MAI/FRAX'
                ),
                (
                    '0xc2d245919db52ea4d5d269b0c710056639176ead',
                    'sAMM-sfrxETH/frxETH'
                ),
                (
                    '0x8d65dbe7206a768c466073af0ab6d76f9e14fc6d',
                    'sAMM-USDT/FRAX'
                ),
                (
                    '0x314d95096e49fde9ebb68ad7e162b6edd8d4352a',
                    'vAMM-BNBx/FRAX'
                ),
                (
                    '0x0d8401cbc650e82d1f21a7461efc6409ef55c4db',
                    'vAMM-frxETH/FRAX'
                ),
                (
                    '0x338ca7ed1d6bede03799a36a6f90e107d24dc6ad',
                    'sAMM-FRAX/CUSD'
                ),
                (
                    '0xfd66a4a4c921cd7194abab38655476a06fbaea05',
                    'sAMM-DOLA/FRAX'
                ),
                (
                    '0x7fcfe6b06c1f6aad14884ba24a7f315c1c0c2cef',
                    'sAMM-FRAX/BUSD'
                ),
                (
                    '0x3c9bd1f914d6f0e0cd27a1f77e120c061d1fdbed',
                    'vAMM-FRAX/FXS'
                )
        ) as my_table (contract_address, contract_name)
),
base_date_with_contracts as (
    select
        a.*,
        b.contract_address,
        b.contract_name
    from
        base_date a
        cross join cte b
),
bribe_fee_c as (
    select
        case
            when call_block_time < date('2023-04-01') then 'old_bribe_c'
            else 'new_bribe_c'
        end as c_version,
        case
            when _type like '%Fees%' then 'Fee contract'
            else 'bribe contract'
        end as c_type,
        _token0,
        _token1,
        output_0 as bribe_address
    from
        {{ source('thena_bnb','BribeFactoryV2_call_createBribe')}}
    union
    all
    select
        case
            when call_block_time < date('2023-04-01') then 'old_bribe_c'
            else 'new_bribe_c'
        end as c_version,
        case
            when _type like '%Fees%' then 'Fee contract'
            else 'bribe contract'
        end as c_type,
        _token0,
        _token1,
        output_0 as bribe_address
    from
        {{ source('thena_bnb','BribeFactoryV2_call_createBribe')}}
),
all_addresses as (
    select
        a.contract_address,
        b.bribe_address,
        b.c_version,
        b.c_type,
        b._token0 as token0,
        b._token1 as token1
    from
        pairs a
        left join bribe_fee_c b on a.token0 = b._token0
        and a.token1 = b._token1
),
bribe_base as (
    select
        block_time,
        case
            when block_time < date('2023-04-01') then 'old_bribe_c'
            else 'new_bribe_c'
        end as c_type,
        a.contract_address as bribe_address,
        b.contract_address as contract_address,
        bytea2numeric_v3(substring(data, 67, 64)) / pow(10, 18) as amount,
        cast(
            from_unixtime(bytea2numeric_v3(substring(data, 131))) as timestamp
        ) as start_time
    from
                   {{ source('bnb', 'logs') }} a
        join all_addresses b on a.contract_address = b.bribe_address
    where
        topic1 = '0x6a6f77044107a33658235d41bedbbaf2fe9ccdceb313143c947a5e76e1ec8474'
        and substring(data, 1, 66) = '0x000000000000000000000000e48a3d7d0bc88d552f730b62c006bc925eadb9ee'
        and a.contract_address in (
            select
                bribe_address
            from
                all_addresses
            where
                c_type = 'bribe contract'
        )
),
bribes as (
    select
        a.start_time,
        a.contract_address,
        sum(amount) as bribe
    from
        bribe_base a
        left join pairs c on a.contract_address = c.contract_address
    group by
        1,
        2
),
bribes_received as (
    select
        block_time as collection_time,
        tx_hash,
        a.contract_address as bribe_address,
        c.contract_address,
        concat('0x', substring(topic3, 27, 42)) as reward_token_collected,
        round(bytea2numeric_v3(substr(data, 3)) / pow(10, 18)) as reward_amount,
        case
            when substring(topic3, 27, 42) = 'e48a3d7d0bc88d552f730b62c006bc925eadb9ee' then round(
                bytea2numeric_v3(substr(data, 3)) / pow(10, 18) * b.price
            )
        end as reward_amount_usd
    from
        {{ source('bnb', 'logs') }} a
        left join (
            select
                *
            from
                prices
            where
                contract_address = '0x3432b6a60d23ca0dfca7761b7ab56459d9c964d0'
        ) b on date_trunc('day', a.block_time) = b.day
        join all_addresses c on a.contract_address = c.bribe_address
    where
        topic1 = '0x540798df468d7b23d11f156fdb954cb19ad414d150722a7b6d55ba369dea792e'
        and topic2 = '0x0000000000000000000000008811da0385ccf1848b21475a42ea4d07fc5d964a'
        and a.contract_address in (
            select
                bribe_address
            from
                all_addresses
            where
                c_type = 'bribe contract'
        )
        and block_time > cast('2023-01-02' as date)
),
weekly_rewards as (
    select
        collection_time,
        bribe_address,
        contract_address,
        sum(reward_amount_usd) as fxs_collected_usd,
        sum(reward_amount) as fxs_collected
    from
        bribes_received
    group by
        1,
        2,
        3
),
fees_collected as (
    select
        block_time as collection_time,
        tx_hash,
        a.contract_address as bribe_address,
        c.contract_address,
        case
            when concat('0x', substring(topic3, 27, 42)) = c.token0 then round(
                bytea2numeric_v3(substr(data, 3)) / pow(10, 18),
                4
            )
        end as fee_reward_token0,
        case
            when concat('0x', substring(topic3, 27, 42)) = c.token1 then round(
                bytea2numeric_v3(substr(data, 3)) / pow(10, 18),
                4
            )
        end as fee_reward_token1
    from
         {{ source('bnb', 'logs') }} a
        join all_addresses c on a.contract_address = c.bribe_address
    where
        topic1 = '0x540798df468d7b23d11f156fdb954cb19ad414d150722a7b6d55ba369dea792e'
        and topic2 = '0x0000000000000000000000008811da0385ccf1848b21475a42ea4d07fc5d964a'
        and a.contract_address in (
            select
                bribe_address
            from
                all_addresses
            where
                c_type = 'Fee contract'
        )
        and block_time > cast('2023-01-02' as date)
),
fee_weekly_rewards as (
    select
        collection_time,
        contract_address,
        sum(fee_reward_token0) as fee_reward_token0,
        sum(fee_reward_token1) as fee_reward_token1
    from
        fees_collected
    group by
        1,
        2
)
select
    week_start,
    week_end,
    case
        when week_start = cast('2023-01-05' as date) then 0
        else a.week
    end as week,
    a.contract_address,
    a.contract_name,
    d.bribe as bribe_last_week,
    round(d.bribe * e.price) as bribe_last_week_usd,
    b.frax_reserve as start_frax,
    c.frax_reserve as end_frax,
    f.fxs_collected_usd,
    f.fxs_collected,
    round(
        f.fxs_collected_usd - COALESCE((d.bribe * e.price), 0)
    ) as gross_profit,
    g.fee_reward_token0,
    g.fee_reward_token1,
    b.frxETH_reserve as start_frxETH,
    c.frxETH_reserve as end_frxETH,
    round(b.frax_TVL) as start_frax_tvl,
    round(c.frax_TVL) as end_frax_tvl,
    round(b.frxETH_TVL) as start_frxETH_tvl,
    round(c.frxETH_TVL) as end_frxETH_tvl
from
    base_date_with_contracts a
    left join TVL_sum b on a.week_start = b.block_time
    and a.contract_address = b.contract_address
    left join TVL_sum c on a.week_end = c.block_time
    and a.contract_address = c.contract_address
    left join bribes d on a.contract_address = d.contract_address
    and TO_DATE(a.week_start, 'yyyy-MM-dd') = TO_DATE(d.start_time, 'yyyy-MM-dd')
    left join (
        select
            day,
            price
        from
            prices
        where
            contract_address = '0x3432b6a60d23ca0dfca7761b7ab56459d9c964d0'
    ) e on date(a.week_start) = e.day
    left join weekly_rewards f on (
        date_add(a.week_start, 7) <= f.collection_time
        and date_add(a.week_start, 14) >= f.collection_time
    )
    and a.contract_address = f.contract_address
    left join fee_weekly_rewards g on (
        date_add(a.week_start, 7) <= g.collection_time
        and date_add(a.week_start, 14) >= g.collection_time
    )
    and a.contract_address = g.contract_address
order by week_start desc 
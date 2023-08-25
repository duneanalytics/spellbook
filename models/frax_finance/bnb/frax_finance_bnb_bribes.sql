{{ config(
    alias = alias('bribes'),
    materialized = 'table',
    tags = ['dunesql'],
    file_format = 'delta',
    unique_key = ['week_start', 'week_end', 'contract_address'],
    post_hook = '{{ expose_spells(\'["bnb"]\',
                                    "project",
                                    "frax_finance",
                                    \'["vahid"]\') }}'
) }} 


with base_date as (
    SELECT
        week_array AS week_start,
        week(date_add('DAY', -4, week_array)) as week,
        date_add('DAY', 7, week_array) as week_end
    FROM
        UNNEST(
            SEQUENCE(
                cast('2023-01-05' AS date),
                current_date,
                INTERVAL '7' DAY
            )
        ) AS t1(week_array)
),
base_date2 as (
    SELECT
        date_array AS day
    FROM
        UNNEST(
            SEQUENCE(
                cast('2023-01-05' AS date),
                current_date,
                INTERVAL '1' DAY
            )
        ) AS t1(date_array)
),

c_info_as_per_nader as (
    --This table has to be updated by Frax team when there is a new gauge. Here you only need to write the contract address and name
    select
        *
    from
        (
            VALUES
                (
                    0x8a420aaca0c92e3f97cdcfdd852e01ac5b609452,
                    'sAMM-ETH/frxETH'
                ),
                (
                    0x49ad051f4263517bd7204f75123b7c11af9fd31c,
                    'sAMM-MAI/FRAX'
                ),
                (
                    0xc2d245919db52ea4d5d269b0c710056639176ead,
                    'sAMM-sfrxETH/frxETH'
                ),
                (
                    0x8d65dbe7206a768c466073af0ab6d76f9e14fc6d,
                    'sAMM-USDT/FRAX'
                ),
                (
                    0x314d95096e49fde9ebb68ad7e162b6edd8d4352a,
                    'vAMM-BNBx/FRAX'
                ),
                (
                    0x0d8401cbc650e82d1f21a7461efc6409ef55c4db,
                    'vAMM-frxETH/FRAX'
                ),
                (
                    0x338ca7ed1d6bede03799a36a6f90e107d24dc6ad,
                    'sAMM-FRAX/CUSD'
                ),
                (
                    0xfd66a4a4c921cd7194abab38655476a06fbaea05,
                    'sAMM-DOLA/FRAX'
                ),
                (
                    0x7fcfe6b06c1f6aad14884ba24a7f315c1c0c2cef,
                    'sAMM-FRAX/BUSD'
                ),
                (
                    0x3c9bd1f914d6f0e0cd27a1f77e120c061d1fdbed,
                    'vAMM-FRAX/FXS'
                )
        ) as my_table (contract_address, contract_name)
),
c_info_as_per_bribe_factory as (
    -- this table automatically captures all the (changing) bribe addresses for the above contract_addresses. DON'T TOUCH THIS
    select
        min(call_block_time) as effective_date,
        case
            when _type like '%Fees%' then 'Fee contract'
            else 'bribe contract'
        end as c_type,
        substr(split_part(_type, ':', 2), 2) AS contract_name,
        _token0,
        _token1,
        output_0 as bribe_address
    from
        {{ source('thena_bnb','BribeFactoryV2_call_createBribe')}}
    group by
        2,
        3,
        4,
        5,
        6
    union
    all
    select
        min(call_block_time) as effective_date,
        case
            when _type like '%Fees%' then 'Fee contract'
            else 'bribe contract'
        end as c_type,
        substr(split_part(_type, ':', 2), 2) AS contract_name,
        _token0,
        _token1,
        output_0 as bribe_address
    from
        {{ source('thena_fi_bnb','BribeFactoryV2_call_createBribe')}}
    group by
        2,
        3,
        4,
        5,
        6
),
all_addresses as (
    select
        a.effective_date,
        lead(a.effective_date, 1) over (
            partition by b.contract_address
            order by
                a.effective_date
        ) as end_date,
        a.bribe_address,
        a.c_type,
        a.contract_name,
        a._token0 as token0,
        a._token1 as token1,
        b.contract_address
    from
        c_info_as_per_bribe_factory a
        join c_info_as_per_nader b on a.contract_name = b.contract_name
),
reserves as (
    select
        a.contract_address,
        call_block_time,
        output__reserve0 / cast(pow(10, 18) as uint256) as reserve0,
        output__reserve1 / cast(pow(10, 18) as uint256) as reserve1,
        row_number() over(
            partition by date(call_block_time),
            a.contract_address
            order by
                call_block_time desc
        ) as rn
    from
        {{ source('thena_fi_bnb', 'Pair_call_getReserves') }} a
        inner join c_info_as_per_nader b on a.contract_address = b.contract_address
    where
        date(call_block_time) >= date('2023-01-04')
)
,
TVL_with_gaps as (
    select
        a.call_block_time as block_time,
        a.contract_address,
        b.token0,
        b.token1,
case
            when token0 = 0x90C97F71E18723b0Cf0dfa30ee176Ab653E89F40 then reserve0
            when token1 = 0x90C97F71E18723b0Cf0dfa30ee176Ab653E89F40 then reserve1
        end as Frax_reserve,
case
            when token0 = 0x64048A7eEcF3a2F1BA9e144aAc3D7dB6e58F555e then reserve0
            when token1 = 0x64048A7eEcF3a2F1BA9e144aAc3D7dB6e58F555e then reserve1
        end as FrxETH_reserve
    from
        reserves a
        left join all_addresses b on a.contract_address = b.contract_address
    where
        rn = 1
),
TVL_spine as (
    select
        a.*,
        c.contract_address as ca
    from
        base_date2 a
        left join (
            select
                contract_address
            from
                all_addresses
        ) c on 1 = 1
),
TVL as (
    select
        day,
        a.ca as contract_address,
        coalesce(
            token0,
            lag(token0) IGNORE NULLS OVER (
                partition by a.ca
                ORDER BY
                    day
            )
        ) as token0,
        coalesce(
            token1,
            lag(token1) IGNORE NULLS OVER (
                partition by a.ca
                ORDER BY
                    day
            )
        ) as token1,
        coalesce(
            Frax_reserve,
            last_value(Frax_reserve) IGNORE NULLS OVER (
                partition by a.ca
                ORDER BY
                    day ROWS BETWEEN UNBOUNDED PRECEDING
                    AND CURRENT ROW
            )
        ) as Frax_reserve,
        coalesce(
            FrxETH_reserve,
            last_value(FrxETH_reserve) IGNORE NULLS OVER (
                partition by a.ca
                ORDER BY
                    day ROWS BETWEEN UNBOUNDED PRECEDING
                    AND CURRENT ROW
            )
        ) as FrxETH_reserve
    from
        TVL_spine a
        left join TVL_with_gaps b on a.day = date(b.block_time)
        and a.ca = b.contract_address
),
prices_raw as (
    select
        *,
        row_number() over(
            partition by date(minute),
            contract_address
            order by
                minute desc
        ) as rn
    from
         {{ source('prices', 'usd') }}
    where
        blockchain = 'ethereum'
        and contract_address in (
            0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2,
            0x853d955aCEf822Db058eb8505911ED77F175b99e,
            0x3432B6A60D23Ca0dFCa7761B7ab56459D9C964D0
        )
        and date(minute) >= date('2023-01-04')
),
prices as (
    select
        date(minute) as day,
        contract_address,
        price
    from
        prices_raw
    where
        rn = 1
),
TVL_USD as (
    select
        a.day as day,
        a.contract_address,
        Frax_reserve,
        FrxETH_reserve,
        cast(Frax_reserve as double) * b.price as Frax_TVL_USD,
        cast(FrxETH_reserve as double) * c.price as FrxETH_TVL_USD
    from
        TVL a
        left join (
            select
                day,
                price
            from
                prices
            where
                contract_address = 0x853d955acef822db058eb8505911ed77f175b99e
        ) b on date(a.day) = b.day
        left join (
            select
                day,
                price
            from
                prices
            where
                contract_address = 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2
        ) c on date(a.day) = c.day
),
TVL_sum as (
    select
        day,
        contract_address,
        max(Frax_reserve) as Frax_reserve,
        max(FrxETH_reserve) as FrxETH_reserve,
        max(Frax_TVL_USD) as Frax_TVL,
        max(FrxETH_TVL_USD) as FrxETH_TVL
    from
        TVL_USD
    group by
        1,
        2
),
bribe_base as (
    select
        block_time,
        contract_address as bribe_address,
        round(
            cast(
                bytearray_to_uint256(bytearray_substring(data, 33, 32)) as double
            ) / pow(10, 18)
        ) as amount,
        from_unixtime(
            cast(
                bytearray_to_int256(bytearray_substring(data, 65)) as double
            )
        ) as start_time
    from
        {{ source('bnb', 'logs') }}
    where
        topic0 = 0x6a6f77044107a33658235d41bedbbaf2fe9ccdceb313143c947a5e76e1ec8474
        and bytearray_substring(data, 1, 32) = 0x000000000000000000000000e48a3d7d0bc88d552f730b62c006bc925eadb9ee
        and contract_address in (
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
        a.bribe_address,
        b.contract_address,
        sum(
            case
                when (
                    token0 = 0x64048A7eEcF3a2F1BA9e144aAc3D7dB6e58F555e
                    and token1 = 0x90C97F71E18723b0Cf0dfa30ee176Ab653E89F40
                ) then amount / 2
                when token0 = 0x90C97F71E18723b0Cf0dfa30ee176Ab653E89F40 then amount
                when token1 = 0x90C97F71E18723b0Cf0dfa30ee176Ab653E89F40 then amount
            end
        ) as Frax_bribe,
        sum(
            case
                when token0 = 0x64048A7eEcF3a2F1BA9e144aAc3D7dB6e58F555e
                and token1 = 0x90C97F71E18723b0Cf0dfa30ee176Ab653E89F40 then amount / 2
                when token0 = 0x64048A7eEcF3a2F1BA9e144aAc3D7dB6e58F555e then amount
                when token1 = 0x64048A7eEcF3a2F1BA9e144aAc3D7dB6e58F555e then amount
            end
        ) as FrxETH_bribe
    from
        bribe_base a
        left join all_addresses b on a.bribe_address = b.bribe_address
    group by
        1,
        2,
        3
),
bribes_received as (
    select
        block_time as collection_time,
        tx_hash,
        a.contract_address as bribe_address,
        c.contract_address,
        bytearray_substring(topic2, 13, 21) as reward_token_collected,
        round(
            cast(bytearray_to_uint256(data) as double) / pow(10, 18)
        ) as reward_amount,
        case
            when bytearray_substring(topic2, 13, 21) = 0xe48A3d7d0Bc88d552f730B62c006bC925eadB9eE then round(
                cast(bytearray_to_uint256(data) as double) / pow(10, 18) * b.price
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
                contract_address = 0x3432B6A60D23Ca0dFCa7761B7ab56459D9C964D0
        ) b on date_trunc('day', a.block_time) = b.day
        left join all_addresses c on a.contract_address = c.bribe_address
    where
        topic0 = 0x540798df468d7b23d11f156fdb954cb19ad414d150722a7b6d55ba369dea792e
        and topic1 = 0x0000000000000000000000008811da0385ccf1848b21475a42ea4d07fc5d964a
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
        sum(reward_amount_usd) as FXS_collected_usd
    from
        bribes_received
    where
        reward_token_collected = 0xe48A3d7d0Bc88d552f730B62c006bC925eadB9eE
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
            when bytearray_substring(topic2, 13, 21) = c.token0 then round(
                cast(bytearray_to_uint256(data) as double) / pow(10, 18),
                4
            )
        end as fee_reward_token0,
        case
            when bytearray_substring(topic2, 13, 21) = c.token0 then round(
                cast(bytearray_to_uint256(data) as double) / pow(10, 18),
                4
            )
        end as fee_reward_token1
    from
        {{ source('bnb', 'logs') }} a
        left join all_addresses c on a.contract_address = c.bribe_address
    where
        topic0 = 0x540798df468d7b23d11f156fdb954cb19ad414d150722a7b6d55ba369dea792e
        and topic1 = 0x0000000000000000000000008811da0385ccf1848b21475a42ea4d07fc5d964a
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
),
base_date_with_contracts as (
    select
        a.*,
        b.contract_address,
        b.contract_name
    from
        base_date a
        cross join c_info_as_per_nader b
)
select
    week_start,
    week_end,
    case
        when week_start = cast('2023-01-05' as date) then 0
        else week
    end as week,
    a.contract_address,
    a.contract_name,
    d.Frax_bribe as lwb_frax,
    round(d.Frax_bribe * i.price) as lwb_frax_usd,
    b.Frax_reserve as start_frax,
    c.Frax_reserve as end_frax,
    round(b.Frax_TVL) as start_frax_tvl,
    round(c.Frax_TVL) as end_frax_tvl,
    f.fxs_collected_usd,
    round(
        coalesce(f.FXS_collected_usd, 0) - (
            COALESCE((d.FrxETH_bribe * h.price), 0) + COALESCE((d.Frax_bribe * i.price), 0)
        )
    ) as gross_profit,
    round(g.fee_reward_token0) as fee_reward_token0,
    round(g.fee_reward_token1) as fee_reward_token1,
    d.FrxETH_bribe as lwb_frxeth,
    round(d.FrxETH_bribe * h.price) as lwb_frxeth_usd,
    b.FrxETH_reserve as start_frxeth,
    c.FrxETH_reserve as end_frxeth,
    round(b.FrxETH_TVL) as start_frxeth_tvl,
    round(c.FrxETH_TVL) as end_frxeth_tvl
from
    base_date_with_contracts a
    left join TVL_sum b on a.week_start = b.day
    and a.contract_address = b.contract_address
    left join TVL_sum c on a.week_end = c.day
    and a.contract_address = c.contract_address
    left join bribes d on a.week_start = d.start_time
    and a.contract_address = d.contract_address
    left join weekly_rewards f on (
        date_add('week', 1, a.week_start) <= f.collection_time
        and date_add('week', 2, a.week_start) >= f.collection_time
    )
    and a.contract_address = f.contract_address
    left join fee_weekly_rewards g on (
        date_add('week', 1, a.week_start) <= g.collection_time
        and date_add('week', 2, a.week_start) >= g.collection_time
    )
    and a.contract_address = g.contract_address
    left join (
        select
            day,
            price
        from
            prices
        where
            contract_address = 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2
    ) h on date(a.week_start) = h.day
    left join (
        select
            day,
            price
        from
            prices
        where
            contract_address = 0x853d955aCEf822Db058eb8505911ED77F175b99e
    ) i on date(a.week_start) = i.day
order by
    week_start desc 
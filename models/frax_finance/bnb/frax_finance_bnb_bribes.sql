{{ config(
    alias = alias('bribes'),
    materialized = 'table',
    tags = ['dunesql'],
    file_format = 'delta',
    unique_key = ['week_start', 'week_end', 'contract_address'],
    post_hook='{{ expose_spells(\'["bnb"]\',
                                    "project",
                                    "frax_finance",
                                    \'["vahid"]\') }}'
)
}}

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
                0x90C97F71E18723b0Cf0dfa30ee176Ab653E89F40,
                0x64048A7eEcF3a2F1BA9e144aAc3D7dB6e58F555e
            )
            or output_1 in (
                0x90C97F71E18723b0Cf0dfa30ee176Ab653E89F40,
                0x64048A7eEcF3a2F1BA9e144aAc3D7dB6e58F555e
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
                        0x90C97F71E18723b0Cf0dfa30ee176Ab653E89F40,
                        0x64048A7eEcF3a2F1BA9e144aAc3D7dB6e58F555e
                    )
                    or output_1 in (
                        0x90C97F71E18723b0Cf0dfa30ee176Ab653E89F40,
                        0x64048A7eEcF3a2F1BA9e144aAc3D7dB6e58F555e
                    )
                )
        )
        and call_success = true
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
        inner join pairs b on a.contract_address = b.contract_address
    where
        date(call_block_time) >= date('2023-01-04')


),
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
        left join pairs b on a.contract_address = b.contract_address
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
                pairs
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
        sum(Frax_reserve) as Frax_reserve,
        sum(FrxETH_reserve) as FrxETH_reserve,
        sum(Frax_TVL_USD) as Frax_TVL,
        sum(FrxETH_TVL_USD) as FrxETH_TVL
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
                    0x486B3cbE3fac82492220f646D890cA805BE2726a,
                    0x8a420aaca0c92e3f97cdcfdd852e01ac5b609452,
                    'sAMM-ETH/frxETH'
                ),
                (
                    0xbA86cb779d30fe8Adcd64f15931A63f6bae261DD,
                    0x49ad051f4263517bd7204f75123b7c11af9fd31c,
                    'sAMM-MAI/FRAX'
                ),
                (
                    0x068875E5BBe89Ab1eF0A8E62e5650107875f0C39,
                    0xc2d245919db52ea4d5d269b0c710056639176ead,
                    'sAMM-sfrxETH/frxETH'
                ),
                (
                    0x7AF074B8312462b6FeD7a170a44fd0188FCa3ceE,
                    0x8d65dbe7206a768c466073af0ab6d76f9e14fc6d,
                    'sAMM-USDT/FRAX'
                ),
                (
                    0xE00680165abb4dAdEeD75fC4332BA7d2b809832A,
                    0x314d95096e49fde9ebb68ad7e162b6edd8d4352a,
                    'vAMM-BNBx/FRAX'
                ),
                (
                    0x2e28f2a1113e3cDE6C417114EA3da13fD5b09291,
                    0x0d8401cbc650e82d1f21a7461efc6409ef55c4db,
                    'vAMM-frxETH/FRAX'
                ),
                (
                    0xdF6b30f954Bc8f1c798Fe5784aC8D6508Ae544de,
                    0x338ca7ed1d6bede03799a36a6f90e107d24dc6ad,
                    'sAMM-FRAX/CUSD'
                ),
                (
                    0x6d39e2A90f55276734AABC51C978D0e66De6e822,
                    0xfd66a4a4c921cd7194abab38655476a06fbaea05,
                    'sAMM-DOLA/FRAX'
                ),
                (
                    0x0F4150Bd732D06F5cf8dBdFAa9121B94b2aaEF7c,
                    0x7fcfe6b06c1f6aad14884ba24a7f315c1c0c2cef,
                    'sAMM-FRAX/BUSD'
                ),
                (
                    0x970E2CDe8c2A116d3F346cE2fE60dC0f188D10c4,
                    0x3c9bd1f914d6f0e0cd27a1f77e120c061d1fdbed,
                    'vAMM-FRAX/FXS'
                )
        ) as my_table (bribe_address, contract_address, contract_name)
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
        {{ source('thena_fi_bnb','BribeFactoryV2_call_createBribe')}}
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
bribe_base_2 as (
    select
        block_time,
        case
            when block_time < date('2023-04-01') then 'old_bribe_c'
            else 'new_bribe_c'
        end as c_version,
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
bribe_base as (
    select
        a.*,
        b.contract_address as contract_address
    from
        bribe_base_2 a
        left join all_addresses b on a.bribe_address = b.bribe_address
        and a.c_version = b.c_version
),
bribes as (
    select
        a.start_time,
        a.contract_address,
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
        {{ source('bnb', 'logs') }}  a
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
    b.Frax_reserve as Start_Frax,
    c.Frax_reserve as End_Frax,
    round(b.Frax_TVL) as Start_Frax_TVL,
    round(c.Frax_TVL) as End_Frax_TVL,
    f.FXS_collected_usd,
    round(
        coalesce(f.FXS_collected_usd, 0) - (
            COALESCE((d.FrxETH_bribe * h.price), 0) + COALESCE((d.Frax_bribe * i.price), 0)
        )
    ) as Gross_profit,
    round(g.fee_reward_token0) as fee_reward_token0,
    round(g.fee_reward_token1) as fee_reward_token1,
    d.FrxETH_bribe as lwb_frxETH,
    round(d.FrxETH_bribe * h.price) as lwb_frxETH_usd,
    b.FrxETH_reserve as Start_FrxETH,
    c.FrxETH_reserve as End_FrxETH,
    round(b.FrxETH_TVL) as Start_FrxETH_TVL,
    round(c.FrxETH_TVL) as End_FrxETH_TVL
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


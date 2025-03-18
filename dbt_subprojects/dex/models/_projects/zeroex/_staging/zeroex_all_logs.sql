{{
    config(
        schema = 'zeroex',
        alias = 'all_logs',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'tx_hash', 'index'],
        partition_by = ['blockchain', 'block_time']
    )
}}

{% set blockchain_sources = [
    'ethereum', 'arbitrum', 'optimism', 'polygon', 'base', 'avalanche_c', 
    'bnb', 'scroll', 'linea', 'fantom', 'blast', 'mantle', 'worldchain', 'berachain', 'celo',
    'mode', 'unichain'
] %}

-- Create the base_filtered_logs CTE as a union of all blockchain sources
WITH base_filtered_logs AS (
    {% for blockchain in blockchain_sources %}
    SELECT
        '{{ blockchain }}' as blockchain,
        block_time,
        block_number,
        tx_hash,
        index,
        contract_address,
        topic0,
        topic1,
        topic2,
        data,
        tx_index,
        tx_from,
        tx_to
    FROM
        {{ source(blockchain, 'logs') }} AS logs
    WHERE 1=1
        {% if is_incremental() %}
            AND {{ incremental_predicate('block_time') }}
        {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
), 

-- Generate zeroex_tx data for each blockchain
{% set start_date = "2020-01-01" %}
zeroex_tx AS (
    {% for blockchain in blockchain_sources %}
    SELECT * FROM 
    (
        {{ zeroex_settler_txs_cte(blockchain = blockchain, start_date = start_date) }}
    )
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
),

-- Create the bundled_tx_check CTE
bundled_tx_check as (
    select 
        blockchain,
        tx_hash, 
        block_time,
        block_number, 
        count(*) tx_cnt
    from zeroex_tx
    {% if is_incremental() %}
        WHERE {{ incremental_predicate('block_time') }}
    {% endif %}
    group by 1,2,3,4
), 

-- Include the swap_signatures CTE
swap_signatures as (
    SELECT signature FROM (
        VALUES 
        (0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822),
        (0x66251e495e6e69e208ab08e2bc259dbe2ef482a8c4a93b8984b03a1eb27e1b9e),
        (0xdde2f3711ab09cdddcfee16ca03e54d21fb8cf3fa647b9797913c950d38ad693),
        (0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67),
        (0x19b47279256b2a23a1665c810c8d55a1758940ee09377d4f8d26497a3577dc83),
        (0x2170c741c41531aec20e7c107c24eecfdd15e69c9bb0a8dd37b1840b9e0b207b),
        (0x143f1f8e861fbdeddd5b46e844b7d3ac7b86a122f36e8c463859ee6811b1f29c), --tokenExchange
        (0x8b3e96f2b889fa771c53c981b40daf005f63f637f1869f707052d15a3dd97140), --tokenExchange
        (0xb2e76ae99761dc136e598d4a629bb347eccb9532a5f8bbd72e18467c3c34cc98), --tokenExchange
        (0xac75f773e3a92f1a02b12134d65e1f47f8a14eabe4eaf1e24624918e6a8b269f), --otcOrderFilled
        (0x085d06ecf4c34b237767a31c0888e121d89546a77f186f1987c6b8715e1a8caa), --BuyGem
        (0xc2c0245e056d5fb095f04cd6373bc770802ebd1e6c918eb78fdef843cdb37b0f), --DodoSwap
        (0x103ed084e94a44c8f5f6ba8e3011507c41063177e29949083c439777d8d63f60),
        (0xa4228e1eb11eb9b31069d9ed20e7af9a010ca1a02d4855cee54e08e188fcc32c),
        (0x34660fc8af304464529f48a778e03d03e4d34bcd5f9b6f0cfbf3cd238c642f7f),
        (0xdc004dbca4ef9c966218431ee5d9133d337ad018dd5b5c5493722803f75c64f7),
        (0xa5a79273c52413fd319bf0be43c422824dc76fc4f69c671d8805d1aaf3cecc77),
        (0x823eaf01002d7353fbcadb2ea3305cc46fa35d799cb0914846d185ac06f8ad05),
        (0x3b841dc9ab51e3104bda4f61b41e4271192d22cd19da5ee6e292dc8e2744f713),
        (0x0874b2d545cb271cdbda4e093020c452328b24af12382ed62c4d00f5c26709db),
        (0x606ecd02b3e3b4778f8e97b2e03351de14224efaa5fa64e62200afc9395c2499),
        (0x176648f1f11cda284c124490086be42a926ddf0ae887ebe7b1d6b337d8942756),
        (0x298c349c742327269dc8de6ad66687767310c948ea309df826f5bd103e19d207),
        (0xcd3829a3813dc3cdd188fd3d01dcf3268c16be2fdd2dd21d0665418816e46062),
        (0xb3e2773606abfd36b5bd91394b3a54d1398336c65005baf7bf7a05efeffaf75b),
        (0xad7d6f97abf51ce18e17a38f4d70e975be9c0708474987bb3e26ad21bd93ca70),
        (0x54787c404bb33c88e86f4baf88183a3b0141d0a848e6a9f7a13b66ae3a9b73d1),
        (0x6ac6c02c73a1841cb185dff1fe5282ff4499ce709efd387f7fc6de10a5124320),
        (0x1f5359759208315a45fc3fa86af1948560d8b87afdcaf1702a110ce0fbc305f3)
    ) AS t(signature)
)

-- Now generate the final materialized output
SELECT
    logs.blockchain,
    logs.tx_hash,
    logs.block_time,
    logs.block_number,
    logs.index,
    logs.contract_address,
    logs.topic0,
    logs.topic1,
    logs.topic2,
    st.method_id,
    st.tag,
    st.settler_address,
    st.zid,
    logs.tx_to,
    logs.tx_from,
    st.taker,
    logs.tx_index,
    (try_cast(bytearray_to_uint256(bytearray_substring(logs.DATA, 21,12)) as int256)) as amount, 
    case when logs.topic0 = signature or logs.contract_address = st.settler_address then 'swap' end as log_type,
    logs.data,
    row_number() over (partition by logs.tx_hash, logs.blockchain order by logs.index) rn,
    st.cow_rn,
    case when btx.tx_cnt > 1 then 1 else 0 end as bundled_tx
FROM
    base_filtered_logs AS logs
JOIN
    zeroex_tx st ON logs.tx_hash = st.tx_hash 
                AND logs.block_time = st.block_time 
                AND logs.block_number = st.block_number
                AND logs.blockchain = st.blockchain
JOIN bundled_tx_check btx ON logs.tx_hash = btx.tx_hash 
                         AND logs.block_time = btx.block_time 
                         AND logs.block_number = btx.block_number
                         AND logs.blockchain = btx.blockchain
LEFT JOIN swap_signatures on logs.topic0 = signature
WHERE 1=1
    AND ( 
            logs.topic0 IN (0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65,
                0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef,
                0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c)
            OR logs.contract_address = st.settler_address
            OR logs.topic0 = signature
    )
    AND st.zid != 0xa00000000000000000000000 
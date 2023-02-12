{{ config(
    alias ='loans',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "nft",
                                \'["ivankitanovski", "hosuke"]\') }}'
)}}

{% set loans_models = [
ref('nftfi_ethereum_loans')
, ref('x2y2_ethereum_loans')
, ref('arcade_v1_ethereum_loans')
, ref('arcade_v2_ethereum_loans')
, ref('bend_dao_ethereum_loans')
] %}

-- -- aggregated loans
with loans as (
    {% for loans_model in loans_models %}
    SELECT blockchain,
           evt_tx_hash,
           evt_block_time,
           repay_time,
           borrower,
           lender,
           collectionContract,
           tokenId,
           principal_raw,
           currency,
           apr,
           duration,
           source
    FROM {{ loans_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
),

loans_with_prices as (
    select evt_tx_hash,
           evt_block_time,
           borrower,
           lender,
           collectionContract,
           tokenId,
           apr,
           duration,
           source,
           principal_raw / case
                               when currency = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' THEN 1e18
                               when currency = '0x6b175474e89094c44da98b954eedeac495271d0f' THEN 1e18 * price
                               when currency = '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48' THEN 1e6 * price
               end               as eth,
           principal_raw * case
                               when currency = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' THEN price / 1e18
                               when currency = '0x6b175474e89094c44da98b954eedeac495271d0f' THEN 1.0 / 1e18
                               when currency = '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48' THEN 1.0 / 1e6
               end               as usd,
           case
               when not repay_time is null then 'REPAID'
               when (repay_time is null and evt_block_time + interval '1' day * duration < current_date)
                   then 'DEFAULTED'
               else 'ACTIVE' end as status
            ,
           currency,
           principal_raw
    from loans l
    left join {{ source('prices', 'usd') }} p
        on date_trunc('minute', evt_block_time) = p.minute
        and p.minute  > cast ('2020-05-15' as timestamp)
        and p.contract_address='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
    where not price is null
)

select l.*,
       coalesce(t.name, 'Awesome NFT') as collectionName
from loans_with_prices l
left join {{ ref('tokens_nft') }} t
    on l.collectionContract=t.contract_address
order by evt_block_time asc
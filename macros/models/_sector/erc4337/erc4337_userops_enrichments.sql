{% macro erc4337_userops_enrichments(
    blockchain='',
    base_models=[],
    wrapped_gas_address='',
    gas_symbol='',
    deployed_date='',
    transactions_model=null,
    prices_model=null
    )
%}

WITH userops_base_union AS (
    SELECT      blockchain
                , version
                , block_month
                , block_time
                , entrypoint_contract
                , tx_hash
                , sender
                , userop_hash
                , success
                , paymaster
                , op_fee
                , beneficiary
    FROM (
        {% for erc4337_model in base_models %}
          SELECT blockchain
                , version
                , block_month
                , block_time
                , entrypoint_contract
                , tx_hash
                , sender
                , userop_hash
                , success
                , paymaster
                , op_fee
                , beneficiary
        FROM {{ erc4337_model }}
        {% if is_incremental() %}
        WHERE block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
    )
)

, txs as (
    select
          hash as tx_hash
        , tx."from" as tx_from
        , tx.to as tx_to
        , '{{gas_symbol}}' as gas_symbol
        , cast(gas_used as double) * gas_price / 1e18 as tx_fee
    from {{ transactions_model }} tx
    where hash in (
        select tx_hash from userops_base_union
    )
    and block_time > date '{{deployed_date}}'
    {% if is_incremental() %}
        and block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
)
, price as (
    select symbol, decimals, minute, price
    from {{ prices_model }}
    where minute > date '{{deployed_date}}'
        and contract_address={{wrapped_gas_address}}
        and blockchain='{{ blockchain }}'
    {% if is_incremental() %}
        and minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
)

select
      userop.blockchain
    , userop.version
    , userop.block_month
    , userop.block_time
    , userop.entrypoint_contract
    , userop.tx_hash
    , userop.sender
    , userop.userop_hash
    , userop.success
    , userop.paymaster
    , userop.op_fee
    , userop.op_fee * price.price as op_fee_usd
    , txs.tx_from as bundler
    , txs.tx_to
    , txs.gas_symbol
    , txs.tx_fee
    , txs.tx_fee * price.price as tx_fee_usd
    , userop.beneficiary
from userops_base_union userop
left join txs
    on userop.tx_hash = txs.tx_hash
left join price
    on date_trunc('minute', userop.block_time) = price.minute

{% endmacro %}

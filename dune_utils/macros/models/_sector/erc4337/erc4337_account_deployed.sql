{% macro erc4337_account_deployed(
    blockchain='',
    version='',
    account_deployed_evt_model=null
    )
%}

    select 
          '{{blockchain}}' as blockchain
        , '{{version}}' as version
        , evt_block_time as block_time
        , cast(date_trunc('month', evt_block_time) as date) as block_month
        , userOpHash as userop_hash
        , contract_address as entrypoint_contract
        , evt_tx_hash as tx_hash
        , sender
        , paymaster
        , factory
    from {{account_deployed_evt_model}}
    {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}

{% endmacro %}
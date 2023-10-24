{% macro rabbithole_referral_rewards(
    blockchain
    ,QuestFactory_evt_MintFeePaid
    )
%}

with model as (
    select
        '{{blockchain}}' as blockchain
        ,'rabbithole' as project
        ,'v2' as version
        ,evt_block_number as block_number
        ,evt_block_time as block_time
        ,cast(date_trunc('day',evt_block_time) as date) as block_date
        ,cast(date_trunc('month',evt_block_time) as date) as block_month
        ,evt_tx_hash as tx_hash
        ,'Quest' as category
        ,referrerAddress as referrer_address
        ,cast(null as varbinary) as referee_address     -- will be overwritten as tx_from
        ,{{var('ETH_ERC20_ADDRESS')}} as currency_contract
        ,referrerAmountWei as reward_amount_raw
        ,contract_address as project_contract_address     -- the drop contract
        ,evt_index as sub_tx_id
    from {{QuestFactory_evt_MintFeePaid}}
    {% if is_incremental() %}
    where {{incremental_predicate('evt_block_time')}}
    {% endif %}
)

{{ add_tx_from_and_to('model', blockchain) }}
{% endmacro %}

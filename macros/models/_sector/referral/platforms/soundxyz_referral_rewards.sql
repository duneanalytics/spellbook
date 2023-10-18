{% macro soundxyz_referral_rewards(
    blockchain
    ,evt_Minted
    )
%}
with model as (
    select
        '{{blockchain}}' as blockchain
        ,'soundxyz' as project
        ,'v1' as version
        ,evt_block_number as block_number
        ,evt_block_time as block_time
        ,cast(date_trunc('day',evt_block_time) as date) as block_date
        ,cast(date_trunc('month',evt_block_time) as date) as block_month
        ,evt_tx_hash as tx_hash
        ,'NFT' as category
        ,affiliate as referrer_address
        ,buyer as referee_address     -- will be overwritten as tx_from
        ,{{var('ETH_ERC20_ADDRESS')}} as currency_contract
        ,affiliateFee as reward_amount_raw
        ,contract_address as project_contract_address     -- the drop contract
        ,evt_index as sub_tx_id
    from {{evt_Minted}}
    {% if is_incremental() %}
    where evt_block_time > date_trunc('day', now() - interval '1' day)
    {% endif %}
)
{{ add_tx_from_and_to('model', blockchain) }}
{% endmacro %}


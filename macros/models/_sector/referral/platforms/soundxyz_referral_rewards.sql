{% macro soundxyz_referral_rewards(
    blockchain
    ,evt_Minted_models
    )
%}


with model as (
    {% for evt_Minted in evt_Minted_models %}
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
    where {{incremental_predicate('evt_block_time')}}
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)

{{
    add_tx_columns(
        model_cte = 'model'
        , blockchain = blockchain
        , columns = ['from', 'to']
    )
}}

{% endmacro %}
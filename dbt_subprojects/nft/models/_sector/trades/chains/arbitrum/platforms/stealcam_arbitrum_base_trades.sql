{{ config(
    schema = 'stealcam_arbitrum',
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id']
    )
}}


{% set project_start_date = "TIMESTAMP '2023-03-10'" %}

with stealcam as (
select
    *
    ,case when value > uint256 '0' then cast((value-(0.001*pow(10,18)))/11.0+(0.001*pow(10,18)) as uint256) else uint256 '0' end as surplus_value
FROM {{ source('stealcam_arbitrum', 'Stealcam_evt_Stolen') }} sc
{% if is_incremental() %}
where {{incremental_predicate('evt_block_time')}}
{% endif %}
{% if not is_incremental() %}
WHERE evt_block_time >= {{project_start_date}}
{% endif %}

)

,base_trades as (
    SELECT 'arbitrum' AS blockchain
    , 'stealcam' AS project
    , 'v1' AS project_version
    , sc.evt_block_time AS block_time
    , date_trunc('day',sc.evt_block_time) AS block_date
    , date_trunc('month',sc.evt_block_time) AS block_month
    , sc.evt_block_number AS block_number
    , 'Buy' AS trade_category
    , CASE WHEN sc.value=uint256 '0' THEN 'primary' ELSE 'secondary' END AS trade_type
    , sc."from" AS seller
    , sc.to AS buyer
    , sc.contract_address AS nft_contract_address
    , sc.id AS nft_token_id
    , uint256 '1' AS nft_amount
    , 0x82af49447d8a07e3bd95bd0d56f35241523fbab1 AS currency_contract
    , sc.value AS price_raw
    , sc.contract_address AS project_contract_address
    , sc.evt_tx_hash AS tx_hash
    , CAST(double '0.1'*surplus_value AS uint256) AS platform_fee_amount_raw
    , CAST(double '0.45'*surplus_value AS uint256) AS royalty_fee_amount_raw
    , cast(null as varbinary) AS platform_fee_address
    , m._creator AS royalty_fee_address
    , sc.evt_index as sub_tx_trade_id
    FROM stealcam sc
    INNER JOIN {{ source('stealcam_arbitrum', 'Stealcam_call_mint') }} m ON m.call_success
        AND m.id=sc.id
        {% if is_incremental() %}
        and {{incremental_predicate('m.call_block_time')}}
        {% endif %}
        {% if not is_incremental() %}
        AND m.call_block_time >= {{project_start_date}}
        {% endif %}
)
{{add_nft_tx_data('base_trades','arbitrum')}}

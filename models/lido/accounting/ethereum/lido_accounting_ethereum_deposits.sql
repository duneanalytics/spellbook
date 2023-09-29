{{ config(
        alias = alias('deposits'),
        tags = ['dunesql'], 
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "lido_accounting",
                                \'["ppclunghe", "gregshestakovlido"]\') }}'
        )
}}

{% set project_start_date = '2020-12-18' %} 

	SELECT  block_time as period, 
        sum(cast(value as DOUBLE)) as amount_staked, 
        0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 AS token,
        tx_hash
        FROM  {{source('ethereum','traces')}} 
        {% if is_incremental() %}
        WHERE date_trunc('hour', block_time) >= date_trunc('hour', now() - interval '1' day)
        {% else %}
        WHERE date_trunc('hour', block_time) >= cast('{{ project_start_date }}' as timestamp) 
        {% endif %}  
        AND to = 0x00000000219ab540356cbb839cbe05303d7705fa
        AND call_type = 'call'
        AND success = True 
        AND "from" in (0xae7ab96520de3a18e5e111b5eaab095312d7fe84, 0xB9D7934878B5FB9610B3fE8A5e441e8fad7E293f, 0xFdDf38947aFB03C621C71b06C9C70bce73f12999)
        group by 1,3,4
                
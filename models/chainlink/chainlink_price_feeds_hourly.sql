{{ config(
        alias ='price_feeds_hourly',
        post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "chainlink",
                                \'["msilb7"]\') }}'
        )
}}

SELECT *
FROM
(
        SELECT
                blockchain,
                hour,
                block_date,
                feed_name,
                proxy_address,
                aggregator_address,
                underlying_token_address, 
                oracle_price_avg,
                underlying_token_price_avg
        FROM {{ ref('chainlink_optimism_price_feeds_hourly') }}
        
        /*
        UNION ALL
        <add future blockchains here>
        */
)

{{ config(
        alias = 'minute',
        materialized = 'table',
        file_format = 'delta',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "time",
                                    \'["ilemi"]\') }}'
        )
}}

--later for duneSQL use this syntax https://dune.com/queries/1764158?d=11
SELECT explode(sequence(
    to_timestamp('1999-01-04 00:00:00'), 
    to_timestamp('2100-01-01 23:59:00'), 
    interval '1' minute
))
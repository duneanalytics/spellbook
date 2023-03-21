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
SELECT explode(sequence(to_date('1999-01-04'), to_date('2100-01-01'), interval 1 minute))
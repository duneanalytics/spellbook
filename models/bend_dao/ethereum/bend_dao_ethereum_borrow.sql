 {{
  config(
        alias='borrow',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "bend_dao",
                                    \'["Henrystats"]\') }}')
}}

SELECT 
    * 
FROM 
{{ ref('bend_dao_ethereum_events') }}
WHERE evt_type = 'Borrow'


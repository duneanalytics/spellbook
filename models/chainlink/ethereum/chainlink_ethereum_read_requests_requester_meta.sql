{{
  config(
    tags=['dunesql'],
    alias=alias('read_requests_requester_meta'),
    materialized = 'view',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "chainlink",
                                \'["linkpool_jon"]\') }}'
  )
}}

{% set ens = 'Exponential Premium Price Oracle(ENS)' %}

SELECT requester_address, requester_name FROM (VALUES
  (0xCF7fe2e614f568989869F4AADe060F4EB8a105BE, '{{ens}}'),
) AS tmp_requester_meta(requester_address, requester_name)

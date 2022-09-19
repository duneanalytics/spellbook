{{config(alias='arb_traders')}}

SELECT * FROM {{ ref('labels_arb_traders_ethereum') }}
{{config(alias = alias('smart_dex_traders'), post_hook='{{ expose_spells(\'["ethereum"]\', "sector", "labels", \'["stone"]\') }}')}}

SELECT * FROM {{ ref('labels_smart_dex_traders_ethereum') }}
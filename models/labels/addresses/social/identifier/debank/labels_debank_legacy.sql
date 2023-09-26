{{ config(
	tags=['legacy'],
    alias = alias('debank', legacy_model=True),
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "labels",
                                    \'["stakeridoo"]\') }}'
)}}

select 1
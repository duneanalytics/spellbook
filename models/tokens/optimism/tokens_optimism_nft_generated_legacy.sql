 {{
  config(
    tags = ['legacy'],
    alias= alias('tokens_optimism_nft_generated', legacy_model=True),
        post_hook='{{ expose_spells(\'["optimism"]\',
                                    "sector",
                                    "tokens",
                                    \'["msilb7"]\') }}'
  )
}}
select 1
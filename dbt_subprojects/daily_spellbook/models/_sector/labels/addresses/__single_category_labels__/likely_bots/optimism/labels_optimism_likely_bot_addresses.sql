{{
    config(
        alias = 'likely_bot_addresses_optimism',
        post_hook='{{ expose_spells(\'["optimism"]\',
        "sector",
        "labels",
        \'["msilb7"]\') }}'
    )
}}

{{ get_likely_bot_addresses('optimism') }}

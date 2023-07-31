# Can become a spell later on, but took too long to run
{{ config(
	tags=['legacy'],
	alias = alias('erc20', legacy_model=True)
)
}}

select
    1 as unique_transfer_id
    , 1 as blockchain
    , 1 as wallet_address
    , 1 as token_address
    , 1 as evt_block_time
    , 1 as amount_raw
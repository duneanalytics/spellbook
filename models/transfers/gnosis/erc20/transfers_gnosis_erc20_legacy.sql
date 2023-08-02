{{ config(
	tags=['legacy'],
    schema = 'gnosis_transfers',
    alias = alias('erc20', legacy_model=True),
    ) }}


select
'1' as unique_transfer_id
, 'gnosis' as blockchain, '0x' as wallet_address, '0x' as token_address
, timestamp '2023-01-01' as evt_block_time, '1' as amount_raw

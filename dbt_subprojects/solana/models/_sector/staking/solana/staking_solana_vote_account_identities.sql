{{ config(
    schema = 'staking_solana'
    , alias = 'vote_account_identities'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['block_time', 'vote_identity', 'vote_account']
    , post_hook='{{ expose_spells(\'["solana"]\',
                                "sector",
                                "staking",
                                \'["ilemi"]\') }}')
}}

-- https://docs.rs/solana-vote-program/1.18.18/solana_vote_program/vote_instruction/enum.VoteInstruction.html
SELECT 
    ix.account_arguments[1] as vote_account
    , ix.account_arguments[4] as vote_identity
    , tx.block_time
FROM solana.vote_transactions tx
LEFT JOIN unnest(instructions) as ix ON true
WHERE ix.executing_account = 'Vote111111111111111111111111111111111111111'
AND cardinality(ix.account_arguments) >= 4
AND bytearray_substring(from_base58(ix.data),1,1) = 0x00 --init
AND tx.success
{% if is_incremental() %}
WHERE {{incremental_predicate('tx.block_time')}}
{% endif %}

UNION ALL 

SELECT 
    ix.account_arguments[1] as vote_account
    , ix.account_arguments[2] as vote_identity
    , tx.block_time
FROM solana.vote_transactions tx
LEFT JOIN unnest(instructions) as ix ON true
WHERE ix.executing_account = 'Vote111111111111111111111111111111111111111'
AND cardinality(ix.account_arguments) >= 3
AND bytearray_substring(from_base58(ix.data),1,1) = 0x04 --UpdateValidatorIdentity
AND tx.success
{% if is_incremental() %}
WHERE {{incremental_predicate('tx.block_time')}}
{% endif %}
{% macro orca_whirlpool_two_hop_swaps() %}
with calls as (
    {{ orca_whirlpool_decoded_calls('whirlpool_call_twoHopSwapV2', 'whirlpool_call_two_hop_swap_v2', [
        ['account_whirlpoolOne', 'account_whirlpool_one'],
        ['account_whirlpoolTwo', 'account_whirlpool_two'],
        ['account_tokenMintInput', 'account_token_mint_input'],
        ['account_tokenMintIntermediate', 'account_token_mint_intermediate'],
        ['account_tokenMintOutput', 'account_token_mint_output'],
        ['account_tokenOwnerAccountInput', 'account_token_owner_account_input'],
        ['account_tokenOwnerAccountOutput', 'account_token_owner_account_output'],
        ['account_tokenVaultOneInput', 'account_token_vault_one_input'],
        ['account_tokenVaultOneIntermediate', 'account_token_vault_one_intermediate'],
        ['account_tokenVaultTwoIntermediate', 'account_token_vault_two_intermediate'],
        ['account_tokenVaultTwoOutput', 'account_token_vault_two_output']
    ], bounded=true) }}
), instructions as (
    select i.tx_id, i.block_slot, i.outer_instruction_index, i.inner_instruction_index, i.stack_height
    from {{ source('solana', 'instruction_calls') }} i
    inner join (
        select distinct call_tx_id, call_block_slot, call_outer_instruction_index from calls
    ) c on i.tx_id = c.call_tx_id and i.block_slot = c.call_block_slot
        and i.outer_instruction_index = c.call_outer_instruction_index
    where 1=1
    {% if is_incremental() %}
        and {{ incremental_predicate('i.block_time') }}
    {% else %}
        and i.block_time >= timestamp '2024-06-05'
    {% endif %}
), boundaries as (
    select c.call_tx_id, c.call_outer_instruction_index,
        coalesce(c.call_inner_instruction_index, 0) as start_index,
        coalesce(min(n.inner_instruction_index), 2147483647) as end_index
    from calls c
    inner join instructions p on p.tx_id = c.call_tx_id
        and p.block_slot = c.call_block_slot
        and p.outer_instruction_index = c.call_outer_instruction_index
        and coalesce(p.inner_instruction_index, 0) = coalesce(c.call_inner_instruction_index, 0)
    left join instructions n on n.tx_id = p.tx_id and n.block_slot = p.block_slot
        and n.outer_instruction_index = p.outer_instruction_index
        and c.call_is_inner
        and n.inner_instruction_index > p.inner_instruction_index
        and n.stack_height <= p.stack_height
    where not c.call_is_inner or p.stack_height is not null
    group by 1, 2, 3
), matched as (
    select c.call_tx_id, c.call_outer_instruction_index, b.start_index,
        min(case when t.token_mint_address = c.account_tokenMintInput
            and t.from_token_account = c.account_tokenOwnerAccountInput
            and t.to_token_account = c.account_tokenVaultOneInput then t.inner_instruction_index end) as input_idx,
        min(case when t.token_mint_address = c.account_tokenMintIntermediate
            and t.from_token_account = c.account_tokenVaultOneIntermediate
            and t.to_token_account = c.account_tokenVaultTwoIntermediate then t.inner_instruction_index end) as middle_idx,
        min(case when t.token_mint_address = c.account_tokenMintOutput
            and t.from_token_account = c.account_tokenVaultTwoOutput
            and t.to_token_account = c.account_tokenOwnerAccountOutput then t.inner_instruction_index end) as output_idx,
        count_if(t.token_mint_address = c.account_tokenMintInput
            and t.from_token_account = c.account_tokenOwnerAccountInput
            and t.to_token_account = c.account_tokenVaultOneInput) as input_count,
        count_if(t.token_mint_address = c.account_tokenMintIntermediate
            and t.from_token_account = c.account_tokenVaultOneIntermediate
            and t.to_token_account = c.account_tokenVaultTwoIntermediate) as middle_count,
        count_if(t.token_mint_address = c.account_tokenMintOutput
            and t.from_token_account = c.account_tokenVaultTwoOutput
            and t.to_token_account = c.account_tokenOwnerAccountOutput) as output_count
    from calls c
    inner join boundaries b on b.call_tx_id = c.call_tx_id
        and b.call_outer_instruction_index = c.call_outer_instruction_index
        and b.start_index = coalesce(c.call_inner_instruction_index, 0)
    inner join {{ ref('orca_whirlpool_v2_token_transfers') }} t
        on t.tx_id = c.call_tx_id and t.block_slot = c.call_block_slot
        and t.block_date = c.call_block_date
        and t.outer_instruction_index = c.call_outer_instruction_index
        and t.inner_instruction_index > b.start_index and t.inner_instruction_index < b.end_index
    where 1=1
    {% if is_incremental() %}
        and {{ incremental_predicate('t.block_date') }}
    {% else %}
        and t.block_date >= date '2024-06-05'
    {% endif %}
    group by 1, 2, 3
)
select
    case when hop = 1 then c.account_whirlpoolOne else c.account_whirlpoolTwo end as account_whirlpool,
    c.call_outer_instruction_index,
    coalesce(c.call_inner_instruction_index, 0) + case when hop = 1 then 0 else 2 end as call_inner_instruction_index,
    c.call_is_inner, c.call_tx_signer, c.call_tx_id, c.call_tx_index, c.call_block_time, c.call_block_slot,
    c.call_outer_executing_account,
    case when hop = 1 then c.account_tokenMintInput else c.account_tokenMintIntermediate end as swap_tokenA,
    case when hop = 1 then c.account_tokenVaultOneInput else c.account_tokenVaultTwoIntermediate end as swap_tokenAVault,
    case when hop = 1 then c.account_tokenMintIntermediate else c.account_tokenMintOutput end as swap_tokenB,
    case when hop = 1 then c.account_tokenVaultOneIntermediate else c.account_tokenVaultTwoOutput end as swap_tokenBVault,
    case when hop = 1 then m.input_idx else m.middle_idx end as input_transfer_index,
    case when hop = 1 then m.middle_idx else m.output_idx end as output_transfer_index
from calls c
inner join matched m on m.call_tx_id = c.call_tx_id
    and m.call_outer_instruction_index = c.call_outer_instruction_index
    and m.start_index = coalesce(c.call_inner_instruction_index, 0)
cross join unnest(array[1, 2]) as hops(hop)
where m.input_count = 1 and m.middle_count = 1 and m.output_count = 1
    and m.input_idx < m.middle_idx and m.middle_idx < m.output_idx
{% endmacro %}

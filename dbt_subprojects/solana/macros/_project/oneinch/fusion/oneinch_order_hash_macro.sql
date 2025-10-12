{% macro oneinch_order_hash_macro() %}

case
    when account_protocol_dst_acc = call_outer_executing_account and account_integrator_dst_acc = call_outer_executing_account
    then sha256(
        substr(call_data, 9, 74 - 9) || 0x00 || 0x00  || from_base58(account_src_mint) || from_base58(account_dst_mint) || from_base58(account_maker_receiver)
    )
    
    when account_protocol_dst_acc = call_outer_executing_account
    then sha256(
        substr(call_data, 9, 74 - 9) || 0x00 || from_base58(account_integrator_dst_acc)  || from_base58(account_src_mint) || from_base58(account_dst_mint) || from_base58(account_maker_receiver)
    )

    when account_integrator_dst_acc = call_outer_executing_account
    then sha256(
        substr(call_data, 9, 74 - 9) || from_base58(account_protocol_dst_acc) || 0x00 || from_base58(account_src_mint) || from_base58(account_dst_mint) || from_base58(account_maker_receiver)
    )

    else sha256(
        substr(call_data, 9, 74 - 9) || from_base58(account_protocol_dst_acc) || from_base58(account_integrator_dst_acc) || from_base58(account_src_mint) || from_base58(account_dst_mint) || from_base58(account_maker_receiver)
    )
end

{% endmacro %}
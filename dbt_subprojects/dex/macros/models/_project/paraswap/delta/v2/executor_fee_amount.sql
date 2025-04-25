{% macro executor_fee_amount() %}
 -- NB: the v1 AugustusExectuor (0x6bb000067005450704003100632eb93ea00c0000) has the following shape. On adding new executors needs to be-reconsidered 
    -- struct ExecutorData {
--         // The address of the src token
--         address srcToken;
--         // The address of the dest token
--         address destToken;
--         // The amount of fee to be paid for the swap 
--         uint256 feeAmount;                                                                   <- the field in question
--         // The calldata to execute the swap
--         bytes calldataToExecute;
--         // The address to execute the swap
--         address executionAddress;
--         // The address to receive the fee, if not set the tx.origin will receive the fee
--         address feeRecipient;
--     }
CASE
    WHEN executor = 0x6bb000067005450704003100632eb93ea00c0000 THEN varbinary_to_uint256(varbinary_substring(executorData,  161, 32))
    -- WHEN executor = 0x0500b5050c40e06ed700005dd7cb0ef0b0d0a000 THEN "TODO: join augustus executor contract call and return the output"
    ELSE 0
END as "executorFeeAmount"
{% endmacro %}
CREATE OR REPLACE VIEW zeroex.view_current_staking_params AS (
    SELECT
        evt_block_number AS block_number_set
        , "epochDurationInSeconds" AS epoch_duraction_in_seconds
        , "cobbDouglasAlphaNumerator" AS cobb_douglas_alpha_numerator
        , "cobbDouglasAlphaDenominator" AS cobb_douglas_alpha_denominator
        , "rewardDelegatedStakeWeight" AS reward_delegated_stake_weight
    FROM zeroex_v3."StakingProxy_evt_ParamsSet"
    ORDER BY evt_block_number DESC
    LIMIT 1
);

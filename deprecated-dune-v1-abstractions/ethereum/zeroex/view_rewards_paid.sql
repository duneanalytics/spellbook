CREATE OR REPLACE VIEW zeroex.view_rewards_paid AS (
    SELECT
        HEX_TO_INT(RIGHT(rp."poolId"::VARCHAR,5)) AS pool_id
        , epoch - 1 AS epoch_id
        , rp."membersReward" / 1e18 AS members_reward_eth
        , rp."operatorReward" / 1e18 AS operator_reward_eth
    FROM zeroex_v3."StakingProxy_evt_RewardsPaid" rp
);

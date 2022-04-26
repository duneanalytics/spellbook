CREATE OR REPLACE FUNCTION keep3r_network.fn_calculate_credits (_tbl regclass)
    RETURNS SETOF keep3r_network.type_credits_accountance
    AS $func$
BEGIN
    RETURN QUERY EXECUTE $$ WITH df as (
        SELECT timestamp::TIMESTAMP,
            evt_index::INTEGER,
            event::VARCHAR,
            job::VARCHAR,
            amount::FLOAT,
            period_credits::FLOAT
        FROM $$ || _tbl || $$
    ),
    indexed_df as (
        select *,
            -- DEV: creates an ID for every time job gets rewarded
            SUM(
                case
                    when event in (
                        'CreditsReward',
                        'JobMigrationIn',
                        'JobMigrationOut'
                    ) then 1
                    else 0
                end
            ) over (
                partition by job
                order by TIMESTAMP,
                    evt_index
            ) as reward_id
        from df
    ),
    processed_df as (
        select TIMESTAMP,
            evt_index,
            event,
            job,
            amount as amount,
            extract(
                epoch
                from TIMESTAMP - first_value(TIMESTAMP) over (
                        partition by job,
                        reward_id
                        order by TIMESTAMP
                    )
            ) last_reward_age,
            -- calculates time since reward
            first_value(
                case
                    when (
                        event in (
                            'CreditsReward',
                            'JobMigrationIn',
                            'JobMigrationOut'
                        )
                    ) then amount
                    else 0
                end
            ) over (
                partition by job,
                reward_id
                order by TIMESTAMP
            ) last_reward_credits,
            -- queries credits at reward
            first_value(period_credits) over (
                partition by job,
                reward_id
                order by TIMESTAMP
            ) last_reward_period_credits,
            -- queries period_credits at reward
            SUM(
                case
                    when (event = 'KeeperWork') then amount
                    else 0
                end
            ) over (
                partition by job,
                reward_id
                order by TIMESTAMP rows between unbounded preceding and current row
            ) cum_spent_by_reward_id,
            -- sums all spent credits
            reward_id
        from indexed_df
    ),
    filled_df as (
        select TIMESTAMP,
            evt_index,
            event,
            job,
            amount,
            last_reward_age,
            case
                when event = 'JobMigrationIn' then lag(last_reward_credits) over (
                    order by TIMESTAMP,
                        evt_index
                )
                when event = 'JobMigrationOut' then 0
                else last_reward_credits
            end as last_reward_credits,
            case
                when event = 'JobMigrationIn' then lag(last_reward_period_credits) over (
                    order by TIMESTAMP,
                        evt_index
                )
                when event = 'JobMigrationOut' then 0
                else last_reward_period_credits
            end as last_reward_period_credits,
            cum_spent_by_reward_id,
            reward_id
        from processed_df
    )
select TIMESTAMP,
    evt_index,
    event,
    job,
    amount,
    COALESCE(
        (
            last_reward_credits + least(last_reward_age::NUMERIC / 432000, 1) * last_reward_period_credits - cum_spent_by_reward_id
        ),
        case
            when (
                event in (
                    'CreditsReward',
                    'JobMigrationIn',
                    'JobMigrationOut'
                )
            ) then amount
        end
    ) cum_amount
from filled_df
order by TIMESTAMP,
    evt_index $$;
END
$func$
LANGUAGE plpgsql;


{{config(
      alias='balances'
)}}

WITH
  pools_list as ( -- This CTE lists all the pools contained in the Balancer protocol
    SELECT
      pool as pools
    FROM
       {{source('balancer_ethereum', 'BFactory_evt_LOG_NEW_POOL') }}
  ),
  transfer_details AS ( --This CTE details the inflows and outflows of ERC20 tokens into and out of the differenct balancer pools and vault
    SELECT
      'join' as transfer_type,
      e.`to` as pool,
      e.evt_block_time,
      e.contract_address AS token,
      value as amount
    FROM
      {{ source( 'erc20_ethereum', 'evt_Transfer') }} e
      INNER JOIN pools_list p ON e.`to` = p.pools
    WHERE
      evt_block_time >= to_date('2020-02-27') -- first pool creation date V1
    UNION ALL
    SELECT
      'join' as transfer_type,
      e.`to` as pool,
      e.evt_block_time,
      e.contract_address AS token,
      value as amount
    FROM
    {{ source( 'erc20_ethereum', 'evt_Transfer') }} e
    WHERE
      e.evt_block_time >= to_date('2021-04-19 ') --first date the Balancer V2 Vault contract was deployed
      AND e.`to` = lower('0xBA12222222228d8Ba445958a75a0704d566BF2C8')
    UNION ALL
    SELECT
      'exit' as transfer_type,
      e.`from` as pool,
      e.evt_block_time,
      e.contract_address AS token,
      - value as amount
    FROM
      {{ source( 'erc20_ethereum' ,'evt_Transfer') }} e
      INNER JOIN pools_list p ON e.`from` = p.pools
    WHERE
      evt_block_time >= to_date('2020-02-28')
    UNION ALL
    SELECT
      'exit' as transfer_type,
      e.`from` as pool,
      e.evt_block_time,
      e.contract_address AS token,
      - value AS amount
    FROM
      {{ source( 'erc20_ethereum', 'evt_Transfer') }} e
    WHERE
      e.evt_block_time >= to_date('2021-04-19')
      AND e.`from` = lower('0xBA12222222228d8Ba445958a75a0704d566BF2C8')
  ),
  daily_delta_balance_by_token AS ( --This CTE shows the net  daily balances of ERC20 tokens in the Balancer pools and Vault V2
    SELECT
      pool,
      token,
      date_trunc('day', evt_block_time) as day,
      SUM(COALESCE(amount, 0)) AS amount
    FROM
      transfer_details
    GROUP BY
      1,
      2,
      3
  ),
  cumulative_balance_by_token AS ( --This CTE shows the cumulative changes of balances in the Balancer Pools and and Vault V2
    SELECT
      pool,
      token,
      day,
      LEAD(day, 1, now()) OVER (
        PARTITION BY
          pool,
          token
        ORDER BY
          day
      ) AS day_of_next_change,
      SUM(amount) OVER (
        PARTITION BY
          pool,
          token
        ORDER BY
          day ROWS BETWEEN UNBOUNDED PRECEDING
          AND CURRENT ROW
      ) AS cumulative_amount
    FROM
      daily_delta_balance_by_token
  ),
  calendar AS (
    SELECT
      explode(
        sequence(to_date('2020-02-27'), current_date, interval 1 day) -- the day the first balancer pool was created V1 and hence generate a series from there
      ) as day
  )
, running_cumulative_balance_by_token_with_group AS ( --This CTE checks for tokens inside pools and if there are no transactions on a specfic day , maps the cumulative_amount                                                 -- from the previous day of last transaction
    SELECT
      pool as pool_contract_address,
      token as token_contract_address,
      c.day,
      cumulative_amount,
     sum(
        case
          when c.day is not null then 1
          else 0
        end
      ) over (
        partition by
          pool,
          token
        order by
          c.day
      ) as group_num
    FROM
      calendar c
     --LEFT JOIN cumulative_balance_by_token b ON b.day <= c.day AND c.day < b.day_of_next_change
     LEFT JOIN cumulative_balance_by_token b ON b.day = c.day
  ),
  running_cumulative_balance_by_token as ( 
    select
      pool_contract_address,
      token_contract_address,
      day,
      first_value(cumulative_amount) over (
        partition by
          pool_contract_address,
          token_contract_address,
          group_num
      ) as cumulative_amount
    from
      running_cumulative_balance_by_token_with_group
  )
SELECT
  *
FROM
  running_cumulative_balance_by_token
SELECT
  nxm.insert_quotation_trades(
    '2022-07-28', --! Deployment date
    '2022-10-18')
WHERE
  NOT EXISTS(
    SELECT *
    FROM nxm.quotation_trades
    WHERE evt_block_time >= '2022-07-28' AND evt_block_time < '2022-10-18'
  );
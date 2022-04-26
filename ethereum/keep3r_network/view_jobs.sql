CREATE OR REPLACE VIEW keep3r_network.view_jobs AS (
  select
  '0x' || encode(keep3r,'hex') keep3r,
  '0x' || encode(job,'hex') job,
  '0x' || encode(owner,'hex') job_owner
  from (

  select
  contract_address as keep3r,
  -- fixes inverted event emision
  "_jobOwner" as job,
  _job as owner
  from keep3r_network."Keep3r_evt_JobAddition"

  union all

  select
  contract_address as keep3r,
  -- fixes inverted event emision
  "_jobOwner" as job,
  _job as owner
  from keep3r_network."Keep3r_v2_evt_JobAddition"
  where contract_address in ('\xeb02addCfD8B773A5FFA6B9d1FE99c566f8c44CC')

  union all

  select
  contract_address as keep3r,
  _job as job,
  "_jobOwner" as owner
  from keep3r_network."Keep3r_v2_evt_JobAddition"
  where contract_address not in ('\xeb02addCfD8B773A5FFA6B9d1FE99c566f8c44CC')
  ) u
)

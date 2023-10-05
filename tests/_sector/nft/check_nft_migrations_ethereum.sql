WITH migrated as (
    select
    project
    , project_version
    ,sum(price_raw/pow(10,18)) as mig_total_amount
    ,sum(platform_fee_amount_raw/pow(10,18)) as mig_total_platform_amount
    ,sum(royalty_fee_amount_raw/pow(10,18)) as mig_total_royalty_amount

    from {{ ref('nft_ethereum_trades_beta')}}
    where (project, project_version) in (select distinct project, version from {{ ref('nft_events_old') }})
    group by 1,2
)

, reference as (
    select
    project
    , version as project_version
    ,sum(amount_raw/pow(10,18)) as ref_total_amount
    ,sum(platform_fee_amount_raw/pow(10,18)) as ref_total_platform_amount
    ,sum(royalty_fee_amount_raw/pow(10,18)) as ref_total_royalty_amount

    from {{ ref('nft_events_old')}}
    where blockchain = 'ethereum'
    and (project, version) in (select project, project_version from migrated)
    group by 1,2
)

, test as (
    select
    mig.project
    ,mig.project_version

    ,mig_total_amount
    ,ref_total_amount
    ,abs((mig_total_amount - ref_total_amount)/ref_total_amount) < 0.001 as check_amount

    ,mig_total_platform_amount
    ,ref_total_platform_amount
    ,abs((mig_total_platform_amount - ref_total_platform_amount)/ref_total_platform_amount)  < 0.001 as check_platform_amount

    ,mig_total_royalty_amount
    ,ref_total_royalty_amount
    ,abs((mig_total_royalty_amount - ref_total_royalty_amount)/ref_total_royalty_amount)  < 0.001 as check_royalty_amount
    from migrated mig
    inner join reference ref
    on mig.project = ref.project and mig.project_version = ref.project_version
)

select *
from test
where not check_amount
    or (not check_platform_amount and project != 'superrare')
    or not check_royalty_amount


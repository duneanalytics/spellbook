{{ config(
       schema = 'dns_ton'
       , alias = 'address_book'
       , materialized = 'view'
       , post_hook='{{ expose_spells(\'["ton"]\',
                                   "project",
                                   "dns_ton",
                                   \'["pshuvalov"]\') }}'
   )
 }}

-- choose the best domain to associate with the address
with _domains as (
  select domain, dns_nft_item_owner as owner, true as delegated from 
  {{ ref('dns_ton_domain_latest_info') }}
  where 
  -- someone owns the domain and delegated it by himself to the same address
  (
    dns_nft_item_owner = delegation_initiator 
    and dns_nft_item_owner = delegated_to_wallet
  )

  union all

  -- someone owns the domain and it is not delegated
  select domain, dns_nft_item_owner as owner, false as delegated from {{ ref('dns_ton_domain_latest_info') }}
  where dns_nft_item_owner = dns_nft_item_minter and delegated_to_wallet is null
), _out as (
  -- aggregate domains by owner
  select owner,
  array_agg(domain) as domains, filter(array_agg(case when delegated then domain else null end), x -> x is not null) as delegated_domains
  from _domains
  group by 1
), _best_match as (
  select owner, case
  -- only one domain delegated to the address
  when cardinality(delegated_domains) = 1 then delegated_domains[1]
  -- multiple domains delegated to the address, choose the shortest one
  when cardinality(delegated_domains) > 1 then array_sort(delegated_domains, (x, y) -> IF(length(x) > length(y), 1, IF(length(x) = length(y), IF(x > y, 1, -1), -1)))[1]
  -- no domains delegated to the address, choose the shortest one from all domains
  when cardinality(delegated_domains) = 0 then array_sort(domains, (x, y) -> IF(length(x) > length(y), 1, IF(length(x) = length(y), IF(x > y, 1, -1), -1)))[1]
  else null
  end as domain
  from _out
)
select domain, owner as address from _best_match where domain is not null

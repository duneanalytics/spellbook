{{ config(
       schema = 'ton'
       , alias = 'proxy_ton_addresses'
       , materialized = 'view'
   )
 }}

 {#
 Proxy TON is a special kind of Jetton which partially implement TEP-74 standrard. 
 The main problem is in ignoring amount field of the internal_transfer message which may lead
 to wrong results. So it is recommended to avoid Proxy TON transfers when building
 aggregation across all jettons
 #}

 select '0:8CDC1D7640AD5EE326527FC1AD0514F468B30DC84B0173F0E155F451B4E11F7C' as address -- Ston.fi Proxy contract for TON
 union all
 select '0:671963027F7F85659AB55B821671688601CDCF1EE674FC7FBBB1A776A18D34A3' as address -- Ston.fi Proxy contract for TON v2
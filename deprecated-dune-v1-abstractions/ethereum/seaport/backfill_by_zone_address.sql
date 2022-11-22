-- below zone address is going to be added to OpenSea, 
-- '0x110b2b128a9ed1be5ef32d8e4e41640df5c2cd'
-- Please execute following queries to update the `platform` column of the existing data

-- update seaport.transactions       
update seaport.transactions
   set platform = 'OpenSea'
 where zone_address = '\x110b2b128a9ed1be5ef3232d8e4e41640df5c2cd'
;

-- update seaport.transfers
update seaport.transfers
   set platform = 'OpenSea'
 where zone_address = '\x110b2b128a9ed1be5ef3232d8e4e41640df5c2cd'
;


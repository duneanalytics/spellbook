--unmap contracts linked to this creator to help us overwrite them now.

UPDATE ovm2.get_contracts
SET contract_project = NULL
WHERE creator_address = '\x0000000000ffe8b47b3e2130213b802212439497';

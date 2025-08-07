{% macro
    angstrom_bundle_volume_events(   
        angstrom_contract_addr, 
        controller_v1_contract_addr,
        blockchain,
        project = null,
        version = null
    )
%}

-------------------- TO TEST ------------------

-- single, TOB: 23077861 - 0xb72c702151c9004f3f327a82cfe451f69a206c21b82fa98419791ebc0bc29b94
-- single, USER: 23077829 - 0x32716081b3461e4f4770e14d97565c003aecf647837d151a8380f6b9722e7faf
-- multi, TOB: 
    -- 23085211 - 0xbb0cb5d7062a838a9b590a202a6e9b6478aa7e9a78824a21576dae1662b7dbcb
    -- 23085199 - 0xf07e41f652e68359a2c2fa1e571fdd05fa0eb4430da3941ce96744ac873408b1
    -- 23085183 - 0x627d33d7a00554446b2e4d109bc695c5d5b1131ed68980a24250e36103102c89
-- multi, USER: 
    -- 23084306 - 0x5f0a2eb5ea030dc3f18d03901ffe4ec161bb5fb5942e9904a3d1a75d5e6e53cc
    -- 23084299 - 0xd46f57a0e3aaa61a5f711cd7d2cf90f083e7e37d9125dd07e300a27d554c9c46
    -- 23083864 - 0x6e299e112769472208e63bd05bf40787ff9168c4731c6daa601c25b67f125d95

-----------------------------------------------



SELECT *
FROM ({{ angstrom_bundle_tob_order_volume(angstrom_contract_addr, blockchain) }})
WHERE recipient IS NULL


{% endmacro %}



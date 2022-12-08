{{config(alias='underlying_tokens')}}

WITH

arth_usd as (
    SELECT 
        '0xaf6b98b5dc17f4a9a5199545a1c29ee427266da4' as pool, 
        '0' as token_id,
        LOWER('0x8B02998366F7437F6c4138F4b543EA5c000cD608') as token_address
    
    UNION 

    SELECT 
        '0xaf6b98b5dc17f4a9a5199545a1c29ee427266da4' as pool, 
        '1' as token_id,
        LOWER('0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56') as token_address
        
    UNION 
    
    SELECT 
        '0xaf6b98b5dc17f4a9a5199545a1c29ee427266da4' as pool, 
        '2' as token_id,
        LOWER('0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d') as token_address

    UNION 
    
    SELECT 
        '0xaf6b98b5dc17f4a9a5199545a1c29ee427266da4' as pool, 
        '3' as token_id,
        LOWER('0x55d398326f99059fF775485246999027B3197955') as token_address
), 

ausd_3eps as (
    SELECT 
        '0xa74077eb97778f4e94d79ea60092d0f4831d05a6' as pool, 
        '0' as token_id,
        LOWER('0xDCEcf0664C33321CECA2effcE701E710A2D28A3F') as token_address
    
    UNION 

    SELECT 
        '0xa74077eb97778f4e94d79ea60092d0f4831d05a6' as pool, 
        '1' as token_id,
        LOWER('0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56') as token_address
        
    UNION 
    
    SELECT 
        '0xa74077eb97778f4e94d79ea60092d0f4831d05a6' as pool, 
        '2' as token_id,
        LOWER('0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d') as token_address

    UNION 
    
    SELECT 
        '0xa74077eb97778f4e94d79ea60092d0f4831d05a6' as pool, 
        '3' as token_id,
        LOWER('0x55d398326f99059fF775485246999027B3197955') as token_address
), 

czusd_val3eps as (
    SELECT 
        '0x1050f11db32049e36e354c7e48a401a2a4eeea05' as pool, 
        '0' as token_id,
        LOWER('0xE68b79e51bf826534Ff37AA9CeE71a3842ee9c70') as token_address
    
    UNION 

    SELECT 
        '0x1050f11db32049e36e354c7e48a401a2a4eeea05' as pool, 
        '1' as token_id,
        LOWER('0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56') as token_address
        
    UNION 
    
    SELECT 
        '0x1050f11db32049e36e354c7e48a401a2a4eeea05' as pool, 
        '2' as token_id,
        LOWER('0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d') as token_address

    UNION 
    
    SELECT 
        '0x1050f11db32049e36e354c7e48a401a2a4eeea05' as pool, 
        '3' as token_id,
        LOWER('0x55d398326f99059fF775485246999027B3197955') as token_address
), 

debridge_usd as (
    SELECT 
        '0x5a7d2f9595ea00938f3b5ba1f97a85274f20b96c' as pool, 
        '0' as token_id,
        LOWER('0x1dDcaa4Ed761428ae348BEfC6718BCb12e63bFaa') as token_address
    
    UNION 

    SELECT 
        '0x5a7d2f9595ea00938f3b5ba1f97a85274f20b96c' as pool, 
        '1' as token_id,
        LOWER('0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56') as token_address
        
    UNION 
    
    SELECT 
        '0x5a7d2f9595ea00938f3b5ba1f97a85274f20b96c' as pool, 
        '2' as token_id,
        LOWER('0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d') as token_address

    UNION 
    
    SELECT 
        '0x5a7d2f9595ea00938f3b5ba1f97a85274f20b96c' as pool, 
        '3' as token_id,
        LOWER('0x55d398326f99059fF775485246999027B3197955') as token_address
), 

mai_val3eps as (
    SELECT 
        '0x68354c6e8bbd020f9de81eaf57ea5424ba9ef322' as pool, 
        '0' as token_id,
        LOWER('0x3F56e0c36d275367b8C502090EDF38289b3dEa0d') as token_address
    
    UNION 

    SELECT 
        '0x68354c6e8bbd020f9de81eaf57ea5424ba9ef322' as pool, 
        '1' as token_id,
        LOWER('0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56') as token_address
        
    UNION 
    
    SELECT 
        '0x68354c6e8bbd020f9de81eaf57ea5424ba9ef322' as pool, 
        '2' as token_id,
        LOWER('0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d') as token_address

    UNION 
    
    SELECT 
        '0x68354c6e8bbd020f9de81eaf57ea5424ba9ef322' as pool, 
        '3' as token_id,
        LOWER('0x55d398326f99059fF775485246999027B3197955') as token_address
), 

nbusd_val3eps as (
    SELECT 
        '0x48f805b120c7c8297535ebae5a9195e78bc0e7b8' as pool, 
        '0' as token_id,
        LOWER('0xF71b4b8AA71F7923c94C7e20B8a434a4d9368eee') as token_address
    
    UNION 

    SELECT 
        '0x48f805b120c7c8297535ebae5a9195e78bc0e7b8' as pool, 
        '1' as token_id,
        LOWER('0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56') as token_address
        
    UNION 
    
    SELECT 
        '0x48f805b120c7c8297535ebae5a9195e78bc0e7b8' as pool, 
        '2' as token_id,
        LOWER('0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d') as token_address

    UNION 
    
    SELECT 
        '0x48f805b120c7c8297535ebae5a9195e78bc0e7b8' as pool, 
        '3' as token_id,
        LOWER('0x55d398326f99059fF775485246999027B3197955') as token_address
), 

usdd_3eps as (
    SELECT 
        '0xc2cf01f785c587645440ccd488b188945c9505e7' as pool, 
        '0' as token_id,
        LOWER('0xd17479997F34dd9156Deef8F95A52D81D265be9c') as token_address
    
    UNION 

    SELECT 
        '0xc2cf01f785c587645440ccd488b188945c9505e7' as pool, 
        '1' as token_id,
        LOWER('0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56') as token_address
        
    UNION 
    
    SELECT 
        '0xc2cf01f785c587645440ccd488b188945c9505e7' as pool, 
        '2' as token_id,
        LOWER('0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d') as token_address

    UNION 
    
    SELECT 
        '0xc2cf01f785c587645440ccd488b188945c9505e7' as pool, 
        '3' as token_id,
        LOWER('0x55d398326f99059fF775485246999027B3197955') as token_address
), 

usdl_val3eps as (
    SELECT 
        '0xd3763ec7f4b0e6b3b28b497ba97226178b1e6249' as pool, 
        '0' as token_id,
        LOWER('0xD295F4b58D159167DB247de06673169425B50EF2') as token_address
    
    UNION 

    SELECT 
        '0xd3763ec7f4b0e6b3b28b497ba97226178b1e6249' as pool, 
        '1' as token_id,
        LOWER('0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56') as token_address
        
    UNION 
    
    SELECT 
        '0xd3763ec7f4b0e6b3b28b497ba97226178b1e6249' as pool, 
        '2' as token_id,
        LOWER('0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d') as token_address

    UNION 
    
    SELECT 
        '0xd3763ec7f4b0e6b3b28b497ba97226178b1e6249' as pool, 
        '3' as token_id,
        LOWER('0x55d398326f99059fF775485246999027B3197955') as token_address
), 

usdn_val3eps as (
    SELECT 
        '0x98a7f4c12e5f8447d59400c8e4f7470aea8ec056' as pool, 
        '0' as token_id,
        LOWER('0x03ab98f5dc94996F8C33E15cD4468794d12d41f9') as token_address
    
    UNION 

    SELECT 
        '0x98a7f4c12e5f8447d59400c8e4f7470aea8ec056' as pool, 
        '1' as token_id,
        LOWER('0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56') as token_address
        
    UNION 
    
    SELECT 
        '0x98a7f4c12e5f8447d59400c8e4f7470aea8ec056' as pool, 
        '2' as token_id,
        LOWER('0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d') as token_address

    UNION 
    
    SELECT 
        '0x98a7f4c12e5f8447d59400c8e4f7470aea8ec056' as pool, 
        '3' as token_id,
        LOWER('0x55d398326f99059fF775485246999027B3197955') as token_address
), 

usds_val3eps as (
    SELECT 
        '0xada30b8c461241efa5aee9206a20539b89f1aa09' as pool, 
        '0' as token_id,
        LOWER('0xDE7d1CE109236b12809C45b23D22f30DbA0eF424') as token_address
    
    UNION 

    SELECT 
        '0xada30b8c461241efa5aee9206a20539b89f1aa09' as pool, 
        '1' as token_id,
        LOWER('0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56') as token_address
        
    UNION 
    
    SELECT 
        '0xada30b8c461241efa5aee9206a20539b89f1aa09' as pool, 
        '2' as token_id,
        LOWER('0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d') as token_address

    UNION 
    
    SELECT 
        '0xada30b8c461241efa5aee9206a20539b89f1aa09' as pool, 
        '3' as token_id,
        LOWER('0x55d398326f99059fF775485246999027B3197955') as token_address
), 

valdai_val3eps as (
    SELECT 
        '0x245e8bb5427822fb8fd6ce062d8dd853fbcfabf5' as pool, 
        '0' as token_id,
        LOWER('0x1AF3F329e8BE154074D8769D1FFa4eE058B1DBc3') as token_address
    
    UNION 

    SELECT 
        '0x245e8bb5427822fb8fd6ce062d8dd853fbcfabf5' as pool, 
        '1' as token_id,
        LOWER('0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56') as token_address
        
    UNION 
    
    SELECT 
        '0x245e8bb5427822fb8fd6ce062d8dd853fbcfabf5' as pool, 
        '2' as token_id,
        LOWER('0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d') as token_address

    UNION 
    
    SELECT 
        '0x245e8bb5427822fb8fd6ce062d8dd853fbcfabf5' as pool, 
        '3' as token_id,
        LOWER('0x55d398326f99059fF775485246999027B3197955') as token_address
), 

valtusd_val3eps as (
    SELECT 
        '0xab499095961516f058245c1395f9c0410764b6cd' as pool, 
        '0' as token_id,
        LOWER('0x14016E85a25aeb13065688cAFB43044C2ef86784') as token_address
    
    UNION 

    SELECT 
        '0xab499095961516f058245c1395f9c0410764b6cd' as pool, 
        '1' as token_id,
        LOWER('0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56') as token_address
        
    UNION 
    
    SELECT 
        '0xab499095961516f058245c1395f9c0410764b6cd' as pool, 
        '2' as token_id,
        LOWER('0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d') as token_address

    UNION 
    
    SELECT 
        '0xab499095961516f058245c1395f9c0410764b6cd' as pool, 
        '3' as token_id,
        LOWER('0x55d398326f99059fF775485246999027B3197955') as token_address
), 

all as (

SELECT * FROM arth_usd

UNION 

SELECT * FROM ausd_3eps 

UNION 

SELECT * FROM czusd_val3eps

UNION 

SELECT * FROM debridge_usd

UNION 

SELECT * FROM mai_val3eps

UNION 

SELECT * FROM nbusd_val3eps

UNION 

SELECT * FROM usdd_3eps

UNION 

SELECT * FROM usdl_val3eps

UNION 

SELECT * FROM usdn_val3eps

UNION 

SELECT * FROM usds_val3eps

UNION 

SELECT * FROM valdai_val3eps

UNION 

SELECT * FROM valtusd_val3eps
) 

SELECT 
    'bnb' as blockchain, 
    '1' as version, 
    'ellipsis_finance' as project
    * 
FROM 
all 
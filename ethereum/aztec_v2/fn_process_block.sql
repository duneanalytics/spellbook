-- taken from https://github.com/simplestreet/bytea_bitwise_operation
CREATE OR REPLACE FUNCTION aztec_v2.f_bytea_to_bit(
    IN i_bytea BYTEA
)
RETURNS BIT VARYING
AS
$BODY$
DECLARE
    w_bit BIT VARYING := b'';
BEGIN
    w_bit := ('x' || ltrim(i_bytea::text, '\x'))::bit varying;
RETURN w_bit;
END;
$BODY$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION aztec_v2.f_bit_to_bytea(
    IN i_bit BIT VARYING
)
RETURNS bytea
AS
$BODY$
DECLARE
    w_panel_data_len INTEGER;
    w_str_bit TEXT := '';
    w_bytea BYTEA := NULL::BYTEA;
BEGIN
    /* Get number of bytes */
    w_panel_data_len := octet_length(i_bit);

    IF length(i_bit) % 8 != 0 THEN
        RAISE 'Can not convert to bytea. The passed argument is % bits', length(i_bit);
    END IF;

    FOR i IN 0 .. w_panel_data_len - 1 LOOP
        w_str_bit := w_str_bit || lpad(to_hex(substring(i_bit from (i * 8) + 1  for 8)::int), 2, '0');
    END LOOP;

    w_bytea := decode(w_str_bit, 'hex');

RETURN w_bytea;
END;
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION aztec_v2.f_bytea_rshift(
    IN i_bytea BYTEA,
    IN i_num INTEGER
)
RETURNS BYTEA
AS
$BODY$
DECLARE
    w_bit BIT VARYING := b'';
    w_bytea BYTEA := null::BYTEA;
BEGIN
    w_bit := aztec_v2.f_bytea_to_bit(i_bytea);
    w_bytea := aztec_v2.f_bit_to_bytea(w_bit >> i_num);
RETURN w_bytea;
END;
$BODY$
LANGUAGE plpgsql;


drop type if exists aztec_v2.inner_proof_data_struct cascade;

drop type if exists aztec_v2.proof_data_struct cascade;

drop type if exists aztec_v2.proof_bridge_data_struct cascade;

-- Infomation for bridge usage in a rollup. inputs can have up to 2 different assets of equal value; same for output assets
create type aztec_v2.proof_bridge_data_struct as (
  -- 1-based bridge id. values can be mapped with contract method getSupportedBridges or getSupportedBridge(id)
  addressId numeric,
  -- bridge name, currently hardcoded
  name text,
  -- first input asset ID
  inputAssetIdA numeric,
  -- first input asset
  outputAssetIdA numeric,
  -- first output asset (optional)
  inputAssetIdB numeric,
  -- second output asset (optional)
  outputAssetIdB numeric,
  -- auxData used as input by each specific bridge
  auxData numeric,
  -- true/false flag for second input being present
  secondInputInUse boolean,
  -- true/false flag for second output being present
  secondOutputInUse boolean
);

-- Transaction data
create type aztec_v2.inner_proof_data_struct as (
  proofType text,
  noteCommitment1 bytea,
  noteCommitment2 bytea,
  nullifier1 bytea,
  nullifier2 bytea,
  publicValue numeric,
  publicOwner bytea,
  assetId numeric
);

create type aztec_v2.proof_data_struct as (
  rollupId numeric,
  rollupSize numeric,
  dataStartIndex numeric,
  oldDataRoot bytea,
  newDataRoot bytea,
  oldNullRoot bytea,
  newNullRoot bytea,
  oldDataRootsRoot bytea,
  newDataRootsRoot bytea,
  oldDefiRoot bytea,
  newDefiRoot bytea,
  -- array of data for bridges used in rollup. Length will always be 32, bridge with address = 0 is empty data
  bridges aztec_v2.proof_bridge_data_struct [],
  -- sum of deposits made to a bridge. total value is denominated in inputAssetIdA. Mapping is 1-1 with bridges list
  defiDepositSums numeric [],
  -- asset IDs that fees were paid into
  assetIds numeric [],
  -- transaction fees paid with each asset. Mapping 1-1 with assetIds. Denominated in specific asset
  totalTxFees numeric [],
  defiInteractionNotes bytea [],
  prevDefiInteractionHash bytea,
  rollupBeneficiary bytea,
  numRollupTxs numeric,
  innerProofs aztec_v2.inner_proof_data_struct []
);

create
or replace function aztec_v2.fn_process_aztec_block(data bytea) returns aztec_v2.proof_data_struct as 
$$
declare 

proofData aztec_v2.proof_data_struct;

-- placeholders
bridgeId bytea;
defiDepositSums numeric [];
assetIds numeric [];
totalTxFees numeric [];
defiInteractionNotes bytea [];
innerProofs aztec_v2.inner_proof_data_struct [];
innerProof aztec_v2.inner_proof_data_struct;
bridge aztec_v2.proof_bridge_data_struct;
bridges aztec_v2.proof_bridge_data_struct [];
bridgeName text;
bridgeNum numeric;
assetId numeric;
proofType text;

-- bridge decoding byte holders
inputAssetIdA bytea;
outputAssetIdA bytea;
inputAssetIdB bytea;
outputAssetIdB bytea;
auxData bytea;
bitConfig bytea;
secondInputInUse boolean;
secondOutputInUse boolean;

-- counter variables
startIndex integer = 11 * 32;
innerProofStartIndex integer;
innerProofDataLength integer;
innerProofByteData bytea;
proofId integer;
rollupSize integer;
innerOffset integer;

-- fixed constants
NUM_BRIDGE_CALLS_PER_BLOCK integer = 32;
NUMBER_OF_ASSETS integer = 16;
LENGTH_ROLLUP_HEADER_INPUTS integer = 4544;
INNER_PROOF_ENCODED_LENGTH integer = 1;
EMPTY_BYTES_12 bytea = '\x000000000000000000000000';
EMPTY_BYTES_28 bytea = '\x00000000000000000000000000000000000000000000000000000000';
EMPTY_BYTES_32 bytea = '\x0000000000000000000000000000000000000000000000000000000000000000';

begin

rollupSize = bytea2numeric(substring(data, 61, 4), false);

for i in 0..NUM_BRIDGE_CALLS_PER_BLOCK - 1 
loop 
  bridgeId = substring(data, startIndex + 1, 32);
  bridgeNum = bytea2numeric(substring(bridgeId, length(bridgeId) - 3, 4), false)::bigint & ((1::bigint << 32) - 1);
  
 -- currently hardcoded here but should be able to fetch dynamically from aztec_v2.RollupProcessor_call_getSupportedBridge
 case bridgeNum
   when 1
   then 
     bridgeName = 'ElementBridge';
   when 2, 3
   then
     bridgeName = 'LidoBridge';
   when 4
   then 
     bridgeName = 'AceOfZkBridge';
  when 5
  then
     bridgeName = 'CurveStEthBridge';
  else
     bridgeName = 'N/A';
 end case;
  
  inputAssetIdA = aztec_v2.f_bytea_rshift(bridgeId, 32);
  outputAssetIdA = aztec_v2.f_bytea_rshift(bridgeId, 92);
  inputAssetIdB = aztec_v2.f_bytea_rshift(bridgeId, 62);
  outputAssetIdB = aztec_v2.f_bytea_rshift(bridgeId, 122);
  auxData = aztec_v2.f_bytea_rshift(bridgeId, 184);
  bitConfig = aztec_v2.f_bytea_rshift(bridgeId, 152);
  
  if (get_bit(bitConfig, 0) = 1) then
    secondInputInUse = true;
  else
    secondInputInUse = false;
  end if;
  
  if (get_bit(bitConfig, 1) = 1) then
    secondOutputInUse = true;
  else
    secondOutputInUse = false;
  end if;
 
  select
  --   addressId: numeric
    bridgeNum,
  -- bridgeName: text
    bridgeName,
  --   inputAssetIdA: numeric
    bytea2numeric(substring(inputAssetIdA, length(inputAssetIdA) - 7, 8), false)::bigint & (((1::bigint << 30) - 1)),
  --   outputAssetIdA: numeric
    bytea2numeric(substring(outputAssetIdA, length(outputAssetIdA) - 7, 8), false)::bigint & (((1::bigint << 30) - 1)),
  --   inputAssetIdB: numeric,
    bytea2numeric(substring(inputAssetIdB, length(inputAssetIdB) - 7, 8), false)::bigint & (((1::bigint << 30) - 1)),
  --   outputAssetIdB: numeric,
    bytea2numeric(substring(outputAssetIdB, length(outputAssetIdB) - 7, 8), false)::bigint & (((1::bigint << 30) - 1)),
  --   auxData: numeric    
    bytea2numeric(substring(auxData, length(auxData) - 7, 8), false)::bigint & (((1::bigint << 62) - 1)),
  --  secondInputInUse: boolean
    secondInputInUse,
  --  secondOutputInUse: boolean
    secondOutputInUse
  into bridge;

  bridges = array_append(bridges, bridge);
  startIndex = startIndex + 32;
end loop;

for i in 0..NUM_BRIDGE_CALLS_PER_BLOCK - 1 loop defiDepositSums = array_append(
  defiDepositSums,
  bytea2numeric(substring(data, startIndex + 1, 32), false)
);

startIndex = startIndex + 32;

end loop;

for i in 0..NUMBER_OF_ASSETS - 1 
loop 

assetIds = array_append(
  assetIds,
  bytea2numeric(substring(data, startIndex + 28 + 1, 4), false)
);
startIndex = startIndex + 32;

end loop;

for i in 0..NUMBER_OF_ASSETS - 1 
loop

totalTxFees = array_append(
  totalTxFees,
  bytea2numeric(substring(data, startIndex + 1, 32), false)
);
startIndex = startIndex + 32;

end loop;

for i in 0..NUM_BRIDGE_CALLS_PER_BLOCK - 1 
loop 

defiInteractionNotes = array_append(
  defiInteractionNotes,
  substring(data, startIndex + 1, 32)
);
startIndex = startIndex + 32;

end loop;

innerProofStartIndex = LENGTH_ROLLUP_HEADER_INPUTS;

-- skip over numRealtxs
innerProofStartIndex = innerProofStartIndex + 4;
innerProofDataLength = bytea2numeric(substring(data, innerProofStartIndex + 1, 4), false);

innerProofStartIndex = innerProofStartIndex + 4;

while innerProofDataLength > 0 
loop 

innerProofByteData = substring(data, innerProofStartIndex, length(data));
proofId = bytea2numeric(substring(data, innerProofStartIndex + 1, 1), false);

case proofId -- deposit, withdraw
  when 1, 2 then
  
 innerOffset = 2;
  
if proofId = 1 then
  proofType = 'DEPOSIT';
else 
  proofType = 'WITHDRAW';
end if;
  
select
  --   proofType text
  proofType,
  --   noteCommitment1 bytea,
  substring(innerProofByteData, innerOffset + 1, 32),
  --   noteCommitment2 bytea,
  substring(innerProofByteData, innerOffset + 32 + 1, 32),
  --   nullifier1 bytea,
  substring(innerProofByteData, innerOffset + 32 + 32 + 1, 32),
  --   nullifier2 bytea,
  substring(innerProofByteData, innerOffset + 32 + 32 + 32 + 1, 32),
  --   publicValue numeric,
  bytea2numeric(substring(innerProofByteData, innerOffset + 32 + 32 + 32 + 32 + 1, 32), false),
  --   publicOwner bytea,
  EMPTY_BYTES_12 || substr(
    innerProofByteData,
    innerOffset + 32 + 32 + 32 + 32 + 32 + 1,
    20
  ),
  --   assetId numeric
  bytea2numeric(substr(innerProofByteData, innerOffset + 32 + 32 + 32 + 32 + 32 + 20 + 1, 4), false)
into innerProof;
  
  INNER_PROOF_ENCODED_LENGTH = 1 + 5 * 32 + 20 + 4;
  
-- send, account, defi_deposit, defi_claim
when 3,
4,
5,
6 then


if proofId = 3 then
  proofType = 'SEND';
elsif proofId = 4 then
  proofType = 'ACCOUNT';
elsif proofId = 5 then
  proofType = 'DEFI_DEPOSIT';
else
  proofType = 'DEFI_CLAIM';
end if;

innerOffset = 2;
select
  --   proofType text
  proofType,
  --   noteCommitment1 bytea,
  substring(innerProofByteData, innerOffset + 1, 32),
  --   noteCommitment2 bytea,
  substring(innerProofByteData, innerOffset + 32 + 1, 32),
  --   nullifier1 bytea,
  substring(innerProofByteData, innerOffset + 32 + 32 + 1, 32),
  --   nullifier2 bytea,
  substring(innerProofByteData, innerOffset + 32 + 32 + 32 + 1, 32),
  --   publicValue bytea,
  bytea2numeric(EMPTY_BYTES_32, false),
  --   publicOwner bytea,
  EMPTY_BYTES_32,
  --   assetId - NOT VISIBLE in this transaction types
  null 
 into innerProof;
  
  INNER_PROOF_ENCODED_LENGTH = 1 + 4 * 32;

-- padding
else
select
  -- proofType
  'PADDING',
  --   noteCommitment1 bytea,
  EMPTY_BYTES_32,
  --   noteCommitment2 bytea,
  EMPTY_BYTES_32,
  --   nullifier1 bytea,
  EMPTY_BYTES_32,
  --   nullifier2 bytea,
  EMPTY_BYTES_32,
  --   publicValue bytea,
  bytea2numeric(EMPTY_BYTES_32, false),
  --   publicOwner bytea,
  EMPTY_BYTES_32,
  --   assetId - NOT VISIBLE in this transaction types
  null 
into innerProof;
  
  INNER_PROOF_ENCODED_LENGTH = 1;
end case;

innerProofs = array_append(innerProofs, innerProof);
innerProofStartIndex = innerProofStartIndex + INNER_PROOF_ENCODED_LENGTH;
innerProofDataLength = innerProofDataLength - INNER_PROOF_ENCODED_LENGTH;

end loop;

select
  -- rollupId
  bytea2numeric(substring(data, 29, 4), false),
  -- rollupSize
  rollupSize,
  -- dataStartIndex
  bytea2numeric(substring(data, 93, 4), false),
  -- oldDataRoot
  substring(data, 97, 32),
  -- newDataRoot
  substring(data, 129, 32),
  -- oldNullRoot
  substring(data, 161, 32),
  -- newNullRoot
  substring(data, 193, 32),
  -- oldDataRootsRoot
  substring(data, 225, 32),
  -- newDataRootsRoot
  substring(data, 257, 32),
  -- oldDefiRoot
  substring(data, 289, 32),
  -- newDefiRoot
  substring(data, 321, 32),
  -- bridges
  bridges,
  -- defiDepositSums
  defiDepositSums,
  -- assetIds
  assetIds,
  -- totalTxFees
  totalTxFees,
  -- defiInteractionNotes
  defiInteractionNotes,
  -- prevDefiInteractionHash
  substring(data, startIndex + 1, 32),
  -- rollupBeneficiary
  substring(data, startIndex + 32 + 1, 32),
  -- numRollupTxs
  bytea2numeric(substring(data, startIndex + 32 + 32 + 1, 32), false),
  -- innerProofs
  innerProofs 
into proofData;

RETURN proofData;

end;

$$LANGUAGE PLPGSQL;

-- select
--   "call_block_time",
-- --   "_0",
-- (
--     select
--       rollupid
--     from
--       aztec_v2.fn_process_aztec_block("_0")
--   ),
--   (
--     select
--       assetIds
--     from
--       aztec_v2.fn_process_aztec_block("_0")
--   ),
--   (
--     select
--       totalTxFees
--     from
--       aztec_v2.fn_process_aztec_block("_0")
--   )
  
-- from
--   aztec_v2."RollupProcessor_call_processRollup"
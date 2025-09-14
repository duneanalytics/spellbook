{% macro compute_order_hash_with_bridge(blockchain) %}

{% set network_to_chain_id = {
    'ethereum': 1,
    'optimism': 10,
    'base': 8453,    
} %}

-- SELECT
  keccak(
    concat(
      X'1901',

      -- domain separator
      keccak(
        concat(
          keccak(to_utf8('EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)')),
          keccak(to_utf8('Portikus')),
          keccak(to_utf8('2.0.0')),
          lpad(cast(cast({{ network_to_chain_id[blockchain]}} AS uint256) AS varbinary), 32, X'00'),
          lpad(from_hex('0000000000bbf5c5fd284e657f01bd000933c96d'), 32, X'00')
        )
      ),

      -- Order struct hash
      keccak(
        concat(
          keccak(to_utf8(
            'Order(address owner,address beneficiary,address srcToken,address destToken,uint256 srcAmount,uint256 destAmount,uint256 expectedDestAmount,uint256 deadline,uint256 nonce,uint256 partnerAndFee,bytes permit,Bridge bridge)Bridge(uint256 maxRelayerFee,uint256 destinationChainId,address outputToken,address multiCallHandler)'
          )),

          -- lpad(from_hex('6d383975c64eebe1251e50c3ab0e53537342bde9'), 32, X'00'), -- owner
          lpad(owner, 32, X'00'),
          -- lpad(from_hex('6d383975c64eebe1251e50c3ab0e53537342bde9'), 32, X'00'), -- beneficiary
          lpad(beneficiary, 32, X'00'), -- beneficiary
          -- lpad(from_hex('f469fbd2abcd6b9de8e169d128226c0fc90a012e'), 32, X'00'), -- srcToken
          lpad(srcToken, 32, X'00'), -- srcToken
          -- lpad(from_hex('eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'), 32, X'00'), -- destToken
          lpad(destToken, 32, X'00'), -- destToken
          -- lpad(cast(cast('422561' as uint256) AS varbinary), 32, X'00'),
          lpad(cast(srcAmount AS varbinary), 32, X'00'), -- srcAmount
          -- lpad(cast(cast('168106215780811752' as uint256) AS varbinary), 32, X'00'),
          lpad(cast(destAmount AS varbinary), 32, X'00'), -- destAmount
          -- lpad(cast(cast('168950970633981661' as uint256) AS varbinary), 32, X'00'),
          lpad(cast(expectedDestAmount AS varbinary), 32, X'00'), -- expectedDestAmount
          -- lpad(cast(cast('1747024424' as uint256) AS varbinary), 32, X'00'),
          lpad(cast(deadline AS varbinary), 32, X'00'), -- deadline
          -- lpad(cast(cast('1747020844040' as uint256) AS varbinary), 32, X'00'),
          lpad(cast(nonce AS varbinary), 32, X'00'), -- nonce
          -- lpad(cast(cast('90631063861114836560958097440945986548822432573276877133894239693005947666447' as uint256) AS varbinary), 32, X'00'),
          lpad(cast(partnerAndFee AS varbinary), 32, X'00'), -- partnerAndFee

          -- Permit (empty: 0x, not 0x00)
          -- lpad(keccak(from_hex('')), 32, X'00'),
          lpad( 
            keccak(permit),
            32, X'00'
          ),
          -- Bridge struct hash (inlined)          
          keccak(
            concat(
              keccak(to_utf8('Bridge(uint256 maxRelayerFee,uint256 destinationChainId,address outputToken,address multiCallHandler)')),
              lpad(cast(cast(bridgeMaxRelayerFee as uint256) AS varbinary), 32, X'00'),
              lpad(cast(cast(bridgeDestinationChainId as uint256) AS varbinary), 32, X'00'),
              lpad(bridgeOutputToken, 32, X'00'),
              lpad(bridgeMultiCallHandler, 32, X'00')
            )
          )
        )
      )
    )
  )
  -- ) AS eip712_body_hash;


{% endmacro %}
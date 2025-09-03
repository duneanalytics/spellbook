{% macro compute_order_hash(blockchain) %}

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

      -- struct hash
      keccak(
        concat(
          keccak(to_utf8('Order(address owner,address beneficiary,address srcToken,address destToken,uint256 srcAmount,uint256 destAmount,uint256 expectedDestAmount,uint256 deadline,uint256 nonce,uint256 partnerAndFee,bytes permit)')),
        --   lpad(from_hex('12924049e2d21664e35387c69429c98e9891a820'), 32, X'00'), -- owner
        lpad(owner, 32, X'00'), 
        --   lpad(from_hex('12924049e2d21664e35387c69429c98e9891a820'), 32, X'00'), -- beneficiary
          lpad(beneficiary, 32, X'00'), -- beneficiary
        --   lpad(from_hex('04c154b66cb340f3ae24111cc767e0184ed00cc6'), 32, X'00'), -- srcToken
          lpad(srcToken, 32, X'00'), -- srcToken
        --   lpad(from_hex('eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'), 32, X'00'), -- destToken
          lpad(destToken, 32, X'00'), -- destToken
        --   lpad(cast(cast('1202939820354578' AS uint256) AS varbinary), 32, X'00'), -- srcAmount
          lpad(cast(srcAmount AS varbinary), 32, X'00'), -- srcAmount
        --   lpad(cast(cast('868930905657826' AS uint256) AS varbinary), 32, X'00'), -- destAmount
          lpad(cast(destAmount AS varbinary), 32, X'00'), -- destAmount
        --   lpad(cast(cast('873297392620931' AS uint256) AS varbinary), 32, X'00'), -- expectedDestAmount
          lpad(cast(expectedDestAmount AS varbinary), 32, X'00'), -- expectedDestAmount
        --   lpad(cast(cast('1740790593' AS uint256) AS varbinary), 32, X'00'), -- deadline
          lpad(cast(deadline AS varbinary), 32, X'00'), -- deadline
        --   lpad(cast(cast('1740787014424' AS uint256) AS varbinary), 32, X'00'), -- nonce
          lpad(cast(nonce AS varbinary), 32, X'00'), -- nonce
        --   lpad(cast(cast('90631063861114836560958097440945986548822432573276877133894239693005947666432' AS uint256) AS varbinary), 32, X'00'), -- partnerAndFee
          lpad(cast(partnerAndFee AS varbinary), 32, X'00'), -- partnerAndFee
        --   permit:
        --   lpad( 
        --     keccak(from_hex(
        --       '00000000000000000000000012924049e2d21664e35387c69429c98e9891a8200000000000000000000000000000000000bbf5c5fd284e657f01bd000933c96d0000000000000000000000000000000000000000000000000004461140adac120000000000000000000000000000000000000000000000000000000067c641b2000000000000000000000000000000000000000000000000000000000000001bae25978476dcaf13eb21c5140c058bc49fd4d087f0f0d23b1ccede8f9288bb33450886d34539305ede3448c44206e89074d91afcdc8e07cabbe23cf45c67dd7c'
        --     )),
        --     32, X'00'
        --   )
          lpad( 
            keccak(permit),
            32, X'00'
          )
        )
      )
    )
  )
  -- ) AS eip712_final_hash


{% endmacro %}
{{ 
    safe_aggregation_all(
        table_type = 'native_transfers',
        blockchains = ["arbitrum","avalanche_c","base","blast","bnb","celo","ethereum","gnosis","linea","mantle","optimism","polygon","ronin","scroll","unichain","worldchain","zkevm","zksync"]
    )
}}
{% docs beacon_blobs_doc %}

## Table Description

The `beacon.blobs` table contains information about blobs on the Ethereum mainnet beacon chain, introduced with EIP-4844. Each row represents a single blob, including details such as the block it was included in, its index, and associated KZG commitments and proofs.

{% enddocs %}

{% docs beacon_blocks_doc %}

## Table Description

The `beacon.blocks` table contains information about blocks on the Ethereum mainnet beacon chain. Each row represents a single block, including details such as the block time, slot, epoch, proposer, and various root hashes.

{% enddocs %}

{% docs beacon_attestations_doc %}

## Table Description

The `beacon.attestations` table contains information about attestations on the Ethereum mainnet beacon chain. Each row represents a single attestation, including details such as the block it was included in, aggregation bits, and source and target epochs.

{% enddocs %}

{% docs beacon_validators_doc %}

## Table Description

The `beacon.validators` table contains information about validators on the Ethereum mainnet beacon chain. Each row represents a single validator, including details such as their index, public key, balance, and various epoch-related information.

{% enddocs %}

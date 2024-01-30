-- Proposals Available On Agora And Snapshot Platform

{{ config(alias = 'proposals'
    ,materialized = 'incremental'
    ,file_format = 'delta'
    ,schema = 'governance_optimism'
    ,incremental_strategy = 'merge'
    ,unique_key = ['proposal_id']
    ,incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.start_timestamp')]
    ,post_hook='{{ expose_spells(\'["optimism"]\',
                                      "sector",
                                      "governance",
                                    \'["chain_l"]\') }}'
    )
}}

{% set models = [
    ref('governance_optimism_agora_proposals'),
    ref('governance_optimism_snapshot_proposals')
] %}

WITH all_proposals AS (
    SELECT *
    FROM (
        {% for model in models %}
        SELECT
            proposal_id,
            proposal_link,
            proposal_type,
            proposal_description,
            start_block,
            start_timestamp,
            end_block,
            end_timestamp,
            platform,
            highest_weightage_vote,
            highest_weightage_voter,
            highest_weightage_voter_percentage,
            total_for_votingWeightage,
            total_abstain_votingWeightage,
            total_against_votingWeightage,
            unique_for_votes,
            unique_abstain_votes,
            unique_against_votes,
            unique_votes_count,
            total_votes_casted,
            proposal_status
        FROM
            {{ model }}
        {% if is_incremental() %}
        WHERE
            {{ incremental_predicate('start_timestamp') }}
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
    )
)

SELECT *
FROM
    all_proposals
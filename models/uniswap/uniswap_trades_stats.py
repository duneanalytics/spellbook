import pyspark.sql.functions as F
import scipy as sc

def model(dbt, session):
    dbt.config(
        submission_method="all_purpose_cluster",
        create_notebook=True,
        http_path="sql/protocolv1/o/4190327660257548/0808-060955-vqrj1uyc",
        cluster_id="dbc-7ee55e77-eabe",
        materialized = "incremental",
        packages = ["scipy", "scikit-learn"]
    )
    df_ethereum = dbt.ref("uniswap_ethereum_trades")
    df_optimism = dbt.ref("uniswap_optimism_trades")

    if dbt.is_incremental:
        # or only rows from the past 3 days
        df_ethereum = df_ethereum.filter(df_ethereum.updated_at >= F.date_add(F.current_timestamp(), F.lit(-7)))
        df_optimism = df_optimism.filter(df_optimism.updated_at >= F.date_add(F.current_timestamp(), F.lit(-7)))

    return df_ethereum


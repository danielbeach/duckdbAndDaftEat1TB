from datetime import datetime

import polars as pl


def main():
    start_time = datetime.now()

    bucket = "confessions-of-a-data-guy"
    prefix = "transactions"
    s3_glob = f"s3://{bucket}/{prefix}/**/*.parquet"

    output_file = "daily_transactions_summary.csv"

    print(f"Reading data from: {s3_glob}")
    print(f"Start time: {start_time:%Y-%m-%d %H:%M:%S}")
    print("-" * 60)

    try:
        (
            pl.scan_parquet(s3_glob)
            .group_by(pl.col("datetime").cast(pl.Date).alias("date"))
            .agg(
                pl.count("transaction_id", "customer_id").name.replace("_id", "_count"),
                pl.sum("order_amount", "order_qty").name.prefix("total_"),
            )
            .sort("total_order_amount", descending=True)
            .sink_csv(output_file)
        )

        end_time = datetime.now()
        duration = end_time - start_time

        print(f"\nResults written to: {output_file}")
        print(f"End time: {end_time:%Y-%m-%d %H:%M:%S}")
        print(f"Total duration: {duration}")
        print(f"Total seconds: {duration.total_seconds():.2f}")
    except Exception as e:
        print(f"Error processing data: {e}")
        print("\nTips:")
        print("- Verify AWS creds are correct and have S3 read access.")
        print(
            "- If datetime casting fails, the column may be a string with mixed formats."
        )


if __name__ == "__main__":
    main()

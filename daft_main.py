from datetime import datetime

import daft
from daft import col, DataType

import pyarrow as pa
import pyarrow.csv as pacsv


def main():
    start_time = datetime.now()

    bucket = "confessions-of-a-data-guy"
    prefix = "transactions"
    s3_glob = f"s3://{bucket}/{prefix}/**/*.parquet"

    print(f"Reading data from: {s3_glob}")
    print(f"Start time: {start_time:%Y-%m-%d %H:%M:%S}")
    print("-" * 60)

    try:
        df = daft.read_parquet(s3_glob)
        df = df.into_batches(200_000)

        result = (
            df.with_column("date", col("datetime").cast(DataType.date()))
              .groupby("date")
              .agg(
                  col("transaction_id").count().alias("transaction_count"),
                  col("customer_id").count().alias("customer_count"),
                  col("order_amount").sum().alias("total_order_amount"),
                  col("order_qty").sum().alias("total_order_qty"),
              )
              .sort(col("total_order_amount"), desc=True)
        )

        output_file = "daily_transactions_summary.csv"

        arrow_tbl: pa.Table = result.to_arrow()
        pacsv.write_csv(arrow_tbl, output_file)

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
        print("- If datetime casting fails, the column may be a string with mixed formats.")


if __name__ == "__main__":
    main()

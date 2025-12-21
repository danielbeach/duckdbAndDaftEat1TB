import os
import duckdb
from datetime import datetime
from pathlib import Path

def main():
    start_time = datetime.now()

    bucket = "confessions-of-a-data-guy"
    prefix = "transactions"
    s3_glob = f"s3://{bucket}/{prefix}/**/*.parquet"

    spill_dir = Path("./duckdb_spill").resolve()
    spill_dir.mkdir(parents=True, exist_ok=True)

    print(f"Reading data from: {s3_glob}")
    print(f"Spill dir: {spill_dir}")
    try:
        con = duckdb.connect(database=":memory:")
        con.execute("INSTALL httpfs;")
        con.execute("LOAD httpfs;")

        con.execute("SET memory_limit='50GB';")
        con.execute("SET temp_directory=?;", [str(spill_dir)])
        con.execute("SET preserve_insertion_order=false;")

        query = f"""
            SELECT
                CAST(datetime AS DATE) AS date,
                COUNT(transaction_id) AS transaction_count,
                COUNT(customer_id) AS customer_count,
                SUM(order_amount) AS total_order_amount,
                SUM(order_qty) AS total_order_qty
            FROM read_parquet('{s3_glob}')
            GROUP BY 1
            ORDER BY total_order_amount DESC
        """

        output_file = "daily_transactions_summary.csv"

        con.execute(
            "COPY (" + query + ") TO ? (HEADER, DELIMITER ',');",
            [output_file],
        )

        end_time = datetime.now()
        duration = end_time - start_time

        print(f"\nResults written to: {output_file}")
        print(f"End time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Total duration: {duration}")
        print(f"Total seconds: {duration.total_seconds():.2f}")

    except Exception as e:
        print(f"Error processing data: {e}")
        print("\nTips:")
        print("- Ensure AWS creds are available (env vars, ~/.aws, or instance role).")
        print("- Set AWS_REGION/AWS_DEFAULT_REGION if needed.")
        print("- If your DuckDB build can't INSTALL extensions, use a build that can or preinstall httpfs.")

if __name__ == "__main__":
    main()

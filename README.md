# Processing 1TB of Parquet Data on a Single Node

This repository demonstrates how modern data processing engines like **DuckDB** and **Daft** can efficiently process **1TB of Parquet data** stored in S3 on a **single node**, even when the dataset is larger than available memory.

## Dataset

The test dataset is generated using [rustGenerate1TB](https://github.com/danielbeach/rustGenerate1TB), which creates 1TB of banking transaction data as Parquet files and uploads them to AWS S3. The dataset contains:

- **Location**: `s3://confessions-of-a-data-guy/transactions/`
- **Format**: Parquet files (~256MB each)
- **Schema**:
  - `transaction_id` (String)
  - `datetime` (Datetime)
  - `customer_id` (Int64)
  - `order_qty` (Int32)
  - `order_amount` (Float64)

## Why Single-Node Processing Matters

Traditional data processing often requires distributed systems (like Spark clusters) to handle large datasets. However, modern analytical engines can process datasets **larger than available memory** on a single machine by:

1. **Streaming/Chunked Processing**: Reading and processing data in small chunks rather than loading everything into memory
2. **Lazy Evaluation**: Building query plans that optimize data access patterns
3. **Spill-to-Disk**: Automatically spilling intermediate results to disk when memory is constrained
4. **Columnar Format**: Leveraging Parquet's columnar storage for efficient column pruning and predicate pushdown

## DuckDB Implementation

**File**: `duckdb_main.py`

DuckDB uses its `httpfs` extension to read Parquet files directly from S3 and processes them with:

- **Automatic Spill-to-Disk**: Configured with `temp_directory` to spill intermediate results when memory is constrained
- **Memory Management**: Configurable memory limits allow DuckDB to process data larger than RAM
- **SQL Interface**: Standard SQL queries with automatic query optimization
- **Direct S3 Access**: No need to download files locally

### Key Features:
- Reads all Parquet files matching the S3 glob pattern
- Groups transactions by date
- Aggregates: transaction counts, customer counts, order amounts, and quantities
- Writes results directly to CSV without materializing in memory

### Memory Configuration:
```python
con.execute("SET memory_limit='50GB';")
con.execute("SET temp_directory=?;", [str(spill_dir)])
```

Even on a 16GB machine, DuckDB can process 1TB by spilling to disk automatically.

## Daft Implementation

**File**: `daft_main.py`

Daft is a distributed DataFrame library that can run on a single node and efficiently handle larger-than-memory datasets:

- **Batch Processing**: Processes data in configurable batch sizes (200k rows)
- **Lazy Execution**: Builds execution plans that are optimized before materialization
- **Memory Efficient**: Automatically manages memory through streaming execution
- **Native S3 Support**: Direct S3 access with automatic credential handling

### Key Features:
- Reads Parquet files from S3 using glob patterns
- Converts datetime to date for grouping
- Performs aggregations with automatic memory management
- Converts results to Arrow format for efficient CSV writing

### Memory Management:
```python
df = df.into_batches(200_000)  # Process in 200k row chunks
```

## Usage

### Prerequisites

1. **AWS Credentials**: Configure AWS credentials for S3 access:
   ```bash
   aws configure
   # Or set environment variables:
   export AWS_ACCESS_KEY_ID=your_key
   export AWS_SECRET_ACCESS_KEY=your_secret
   export AWS_DEFAULT_REGION=us-east-1
   ```

2. **Python Dependencies**: Install required packages:
   ```bash
   pip install duckdb daft pyarrow boto3
   ```

### Running DuckDB

```bash
python duckdb_main.py
```

This will:
- Read all Parquet files from S3
- Aggregate transactions by date
- Write results to `daily_transactions_summary.csv`
- Display execution time

### Running Daft

```bash
python daft_main.py
```

This will:
- Read Parquet files in batches
- Perform the same aggregations
- Write results to CSV
- Display execution time

## Performance Characteristics

Both engines demonstrate that **single nodes can process larger-than-memory datasets**:

- **Memory Usage**: Stays within available RAM through streaming and spill-to-disk
- **I/O Efficiency**: Leverages Parquet's columnar format for selective column reading
- **Network Optimization**: Reads only necessary data from S3
- **Scalability**: Can handle datasets many times larger than available memory

## Query Example

Both implementations perform the same aggregation:

```sql
SELECT
    CAST(datetime AS DATE) AS date,
    COUNT(transaction_id) AS transaction_count,
    COUNT(customer_id) AS customer_count,
    SUM(order_amount) AS total_order_amount,
    SUM(order_qty) AS total_order_qty
FROM read_parquet('s3://confessions-of-a-data-guy/transactions/**/*.parquet')
GROUP BY 1
ORDER BY total_order_amount DESC
```

## Output

Both scripts generate `daily_transactions_summary.csv` with daily aggregated transaction data, ordered by total order amount (descending).

## Why This Matters

This demonstrates that modern analytical engines have evolved beyond requiring distributed systems for large datasets. Single-node processing offers:

- **Simpler Architecture**: No cluster management overhead
- **Cost Efficiency**: Single machine vs. multi-node clusters
- **Faster Development**: Easier to set up and debug
- **Sufficient Performance**: Often fast enough for analytical workloads

The key is using engines designed for analytical workloads (columnar processing, vectorization, query optimization) rather than row-by-row processing.

## References

- Dataset Generator: [rustGenerate1TB](https://github.com/danielbeach/rustGenerate1TB)
- [DuckDB Documentation](https://duckdb.org/docs/)
- [Daft Documentation](https://www.getdaft.io/)


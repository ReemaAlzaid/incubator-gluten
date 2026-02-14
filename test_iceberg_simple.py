from pyspark.sql import SparkSession
import os
import shutil

# Clean up
for path in ["/tmp/iceberg_warehouse", "/tmp/iceberg_test"]:
    if os.path.exists(path):
        shutil.rmtree(path)

spark = (
    SparkSession.builder
    .appName("iceberg-input-file-bug-test")
    .config("spark.memory.offHeap.enabled", "true")
    .config("spark.memory.offHeap.size", "2g")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", "file:///tmp/iceberg_warehouse")
    .getOrCreate()
)

print("=" * 80)
print("Creating Iceberg table...")
print("=" * 80)

# Create table
spark.sql("""
    CREATE TABLE local.default.test_table (
        id INT,
        name STRING
    ) USING iceberg
""")

# Insert data
spark.sql("""
    INSERT INTO local.default.test_table VALUES 
    (1, 'Alice'),
    (2, 'Bob'),
    (3, 'Charlie')
""")

print("\n" + "=" * 80)
print("Testing input_file_name() on Iceberg table")
print("=" * 80)

# Query with input_file_name()
df = spark.sql("""
    SELECT 
        id, 
        name, 
        input_file_name() as file_path
    FROM local.default.test_table
""")

print("\n=== Results ===")
results = df.collect()
for row in results:
    file_path = row.file_path
    print(f"ID: {row.id}, Name: {row.name}, File: '{file_path}'")
    
# Check for bug
empty_count = sum(1 for row in results if row.file_path == "")
if empty_count > 0:
    print(f"\n❌ BUG CONFIRMED: {empty_count}/{len(results)} rows have EMPTY file paths!")
    print("input_file_name() is broken on Iceberg tables")
else:
    print(f"\n✅ SUCCESS: All {len(results)} rows have valid file paths")

spark.stop()

# Made with Bob

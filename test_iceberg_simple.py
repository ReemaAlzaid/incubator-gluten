from pyspark.sql import SparkSession
import os
import shutil

# Clean up
for path in ["/tmp/iceberg_warehouse", "/tmp/iceberg_test"]:
    if os.path.exists(path):
        shutil.rmtree(path)

spark = (
    SparkSession.builder
    .appName("iceberg-input-file-metadata-test")
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

print("\n=== input_file_name() Results ===")
results = df.collect()
for row in results:
    file_path = row.file_path
    print(f"ID: {row.id}, Name: {row.name}, File: '{file_path}'")
    
# Check for bug
empty_count = sum(1 for row in results if row.file_path == "")
if empty_count > 0:
    print(f"\n❌ BUG: {empty_count}/{len(results)} rows have EMPTY file paths!")
else:
    print(f"\n✅ SUCCESS: All {len(results)} rows have valid file paths")

print("\n" + "=" * 80)
print("Testing input_file_block_start() on Iceberg table")
print("=" * 80)

# Query with input_file_block_start()
df2 = spark.sql("""
    SELECT 
        id, 
        name, 
        input_file_block_start() as block_start
    FROM local.default.test_table
""")

print("\n=== input_file_block_start() Results ===")
results2 = df2.collect()
for row in results2:
    print(f"ID: {row.id}, Name: {row.name}, Block Start: {row.block_start}")

# Check results
if all(row.block_start >= 0 for row in results2):
    print(f"\n✅ SUCCESS: All {len(results2)} rows have valid block start positions")
else:
    print(f"\n❌ BUG: Some rows have invalid block start positions!")

print("\n" + "=" * 80)
print("Testing input_file_block_length() on Iceberg table")
print("=" * 80)

# Query with input_file_block_length()
df3 = spark.sql("""
    SELECT 
        id, 
        name, 
        input_file_block_length() as block_length
    FROM local.default.test_table
""")

print("\n=== input_file_block_length() Results ===")
results3 = df3.collect()
for row in results3:
    print(f"ID: {row.id}, Name: {row.name}, Block Length: {row.block_length}")

# Check results
if all(row.block_length >= 0 for row in results3):
    print(f"\n✅ SUCCESS: All {len(results3)} rows have valid block lengths")
else:
    print(f"\n❌ BUG: Some rows have invalid block lengths!")

print("\n" + "=" * 80)
print("Testing all three metadata functions together")
print("=" * 80)

# Query with all three functions
df_all = spark.sql("""
    SELECT 
        id, 
        name, 
        input_file_name() as file_path,
        input_file_block_start() as block_start,
        input_file_block_length() as block_length
    FROM local.default.test_table
""")

print("\n=== All Metadata Functions Results ===")
results_all = df_all.collect()
for row in results_all:
    print(f"ID: {row.id}, Name: {row.name}")
    print(f"  File: '{row.file_path}'")
    print(f"  Block Start: {row.block_start}")
    print(f"  Block Length: {row.block_length}")
    print()

# Final check
all_valid = all(
    row.file_path != "" and 
    row.block_start >= 0 and 
    row.block_length >= 0 
    for row in results_all
)

if all_valid:
    print("✅ ALL TESTS PASSED: All metadata functions work correctly!")
else:
    print("❌ SOME TESTS FAILED: Check the output above for details")

spark.stop()

# Made with Bob

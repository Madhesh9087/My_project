import sys
from pyspark.sql import SparkSession

def compare_tables(spark, table1, table2):
    # Load tables
    df1 = spark.table(table1)
    df2 = spark.table(table2)

    print(f"Comparing tables: {table1} vs {table2}")

    # 1. Compare schemas
    schema1 = set((f.name, f.dataType) for f in df1.schema.fields)
    schema2 = set((f.name, f.dataType) for f in df2.schema.fields)
    if schema1 != schema2:
        print("Schemas are different!")
        print("Table 1 schema:", schema1)
        print("Table 2 schema:", schema2)
    else:
        print("Schemas are identical.")

    # 2. Compare row counts
    count1 = df1.count()
    count2 = df2.count()
    print(f"Row count - {table1}: {count1}, {table2}: {count2}")
    if count1 != count2:
        print("Row counts are different!")
    else:
        print("Row counts are identical.")

    # 3. Compare data differences (symmetric difference)
    # Assumes both tables have the same schema and columns order
    diff1 = df1.subtract(df2)
    diff2 = df2.subtract(df1)
    diff1_count = diff1.count()
    diff2_count = diff2.count()
    print(f"Rows in {table1} not in {table2}: {diff1_count}")
    print(f"Rows in {table2} not in {table1}: {diff2_count}")

    if diff1_count == 0 and diff2_count == 0:
        print("No data differences found.")
    else:
        print("Data differences found!")
        print(f"Sample rows in {table1} not in {table2}:")
        diff1.show(5)
        print(f"Sample rows in {table2} not in {table1}:")
        diff2.show(5)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: spark-submit compare_hive_tables.py <table1> <table2>")
        sys.exit(1)

    table1 = sys.argv[1]
    table2 = sys.argv[2]

    spark = SparkSession.builder \
        .appName("Hive Table Comparison") \
        .enableHiveSupport() \
        .getOrCreate()

    compare_tables(spark, table1, table2)
    spark.stop()
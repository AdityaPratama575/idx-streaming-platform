#!/usr/bin/env python3
"""Validate that producer, Spark processor, and Avro schema are in sync."""

import json
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def load_avro_schema(path):
    with open(path) as f:
        schema = json.load(f)
    return {f["name"]: f["type"] for f in schema["fields"]}


def parse_spark_schema():
    """Read StructType definition from spark_processor.py (by parsing source)."""
    with open(os.path.join(PROJECT_ROOT, "spark_processor.py")) as f:
        source = f.read()

    import re
    fields = {}
    pattern = re.compile(r'StructField\("(\w+)",\s*(\w+)Type')

    for line in source.splitlines():
        match = pattern.search(line)
        if match:
            name, type_name = match.groups()
            fields[name] = type_name
    return fields


def main():
    errors = 0

    avro = load_avro_schema(os.path.join(PROJECT_ROOT, "schemas", "stock_tick_v1.avsc"))
    spark_fields = parse_spark_schema()

    print("=== Schema Validation Report ===")
    print()

    avro_names = set(avro.keys())
    spark_names = set(spark_fields.keys())

    # Check Spark has all Avro fields
    missing_in_spark = avro_names - spark_names
    if missing_in_spark:
        print(f"❌ Fields in Avro but missing in Spark: {missing_in_spark}")
        errors += 1
    else:
        print("✅ All Avro fields present in Spark processor")

    # Check Avro has all Spark fields
    missing_in_avro = spark_names - avro_names
    if missing_in_avro:
        print(f"⚠️ Fields in Spark but missing in Avro: {missing_in_avro}")
        # Not a fatal error - Spark may have added fields ahead of schema update
    else:
        print("✅ All Spark fields present in Avro schema")

    # Check producer payload keys match
    with open(os.path.join(PROJECT_ROOT, "producer.py")) as f:
        producer_source = f.read()
    expected_keys = avro_names
    if all(k in producer_source for k in expected_keys):
        print("✅ All schema keys referenced in producer.py")
    else:
        print("❌ Some schema keys missing from producer.py")
        errors += 1

    print()
    if errors:
        print(f"❌ {errors} error(s) found!")
        sys.exit(1)
    else:
        print("✅ All schemas are in sync!")
        sys.exit(0)


if __name__ == "__main__":
    main()

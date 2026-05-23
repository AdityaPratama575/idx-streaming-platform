# Schema Registry — IDX Stock Tick

## Rules for Schema Evolution

1. **Backward compatible**: New fields must have `default: null` (nullable)
2. **No field deletion**: Fields can be deprecated but never removed
3. **No type changes**: Field types are immutable after v1
4. **Version increment**: Always create v2, v3, ... for changes

## Versioning Strategy

- Producer adds `schema_name` + `schema_version` in Kafka record headers
- Spark processor uses `from_json` with `PERMISSIVE` mode (unknown fields → NULL)
- Producer deploys AFTER Spark processor for new fields

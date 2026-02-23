# Architecture

## System Flow

1. Start TCP listener on `0.0.0.0:9092`.
2. Read framed Kafka requests (`message_size` + payload).
3. Parse request header (`api_key`, `api_version`, `correlation_id`, client metadata).
4. Route by API key:
   - `75`: `DescribeTopicPartitions`
   - else: `ApiVersions` response path
5. Build response body + response header and write back on the same connection.

## DescribeTopicPartitions Flow

1. Parse body fields:
   - compact `topics` array
   - `response_partition_limit`
   - nullable `cursor`
   - request tagged fields
2. Resolve metadata per requested topic.
3. Sort topics alphabetically for response determinism.
4. Build one topic entry per requested topic.
5. Encode `next_cursor` as null (`-1`).

## Metadata Pipeline

### Startup

1. Read `server.properties` path from process args.
2. Parse `log.dirs`.
3. Discover `__cluster_metadata-0/*.log` segments.
4. Load metadata records into in-memory `topicMap`.

### Reload/Fallback

When requested topic metadata is missing or incomplete:
1. Rediscover metadata segment paths.
2. Reload metadata logs.
3. Use tolerant direct lookup (`lookupTopicMetadataFromLogs`) as last resort.

### In-Memory Model

- `topicMap`: topic name -> `TopicMetadata`.
- `TopicMetadata` holds:
  - topic UUID
  - partition metadata slice
- Partition insertion uses dedupe by `partition_id` to avoid duplicate response entries.

## Key Design Decisions

1. **Alphabetical topic ordering in responses**
   - Required by `DescribeTopicPartitions` stage expectations for stable output.

2. **Compact/flexible parsing in requests and metadata**
   - Kafka protocol uses compact encodings and tagged fields; parser is defensive and tolerant.

3. **Key-based PartitionRecord fallback**
   - Value layouts can vary by metadata record version.
   - Fallback attempts to decode `(topic_id, partition_id)` from record key when value parsing fails.

4. **Default partition fields when only identity is known**
   - If only key-level partition identity is available, defaults can be used for non-critical fields (leader/ISR/replicas) to keep partition enumeration possible.

## Known Limitations

1. Partition value decoding is still variant-sensitive across metadata format changes.
2. Current blocker (WQ2): some topics still resolve UUID correctly but return empty partition arrays.
3. Debug logging is intentionally verbose while stage stabilization is in progress.

## Next Technical Step

Stabilize partition extraction to guarantee per-topic partition IDs are always populated before response encoding, regardless of metadata value schema variation.

This likely means promoting key-derived partition IDs to a first-class source of truth when value decoding cannot confirm partition payload details.

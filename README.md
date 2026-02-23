[![progress-banner](https://backend.codecrafters.io/progress/kafka/60f8d06b-5176-483d-9af7-40d6c8f9dd6a)](https://app.codecrafters.io/users/codecrafters-bot?r=2qF)

This is a Go solution for the
["Build Your Own Kafka" challenge](https://codecrafters.io/challenges/kafka).

## Quick Start

1. Ensure `go (1.25)` is installed.
2. Run locally:

```sh
./your_program.sh /tmp/server.properties
```

3. Submit to CodeCrafters:

```sh
codecrafters submit
```

## Implementation Status (as of 2026-02-23)

### Basic
1. Bind to a port
2. Send correlation ID
3. Parse correlation ID
4. Parse API version
5. Handle ApiVersions requests

### Concurrent Clients
1. Serial requests
2. Concurrent requests

### Listing Partitions Stage Matrix

| Stage | Status |
| --- | --- |
| Include DescribeTopicPartitions in ApiVersions | Done |
| List for an unknown topic | Done |
| List for a single partition | Done |
| List for multiple partitions (KU4) | In progress (regressed) |
| List for multiple topics (WQ2) | In progress |

### Current Known Issue

Current failing pattern for KU4/WQ2:
1. Topic-level metadata resolves (`error_code=0`, `topic_id` is correct).
2. `partitions` arrays are empty.
3. Result: tester fails partition length checks for topic entries.

## What Works Today

1. `ApiVersions` request handling (including correlation ID + supported APIs).
2. `DescribeTopicPartitions` request parsing for compact `topics` array, `response_partition_limit`, and `cursor`.
3. Alphabetical ordering of response topics for multi-topic responses.
4. Metadata source discovery from `/tmp/server.properties` via `log.dirs` and scan of `__cluster_metadata-0` segment files.

## How to Run and Debug

### Run locally

```sh
./your_program.sh /tmp/server.properties
```

### Submit remotely

```sh
codecrafters submit
```

### Where logs appear

Runtime logs are printed to stdout by the broker process (visible in local terminal and in tester output during submit).

### Partition Debug Checklist

1. Confirm request topics are parsed correctly.
2. Confirm topic UUID resolves and is non-zero.
3. Confirm partition IDs are discovered before response encoding.
4. Confirm fallback metadata path (`lookupTopicMetadataFromLogs`) is exercised when needed.
5. Confirm response `partitions` compact array length is greater than `1` when partitions exist.

## Repository Map

1. `app/main.go`: TCP server, request parsing, response encoding.
2. `app/metadata.go`: metadata log parsing, topic/partition extraction, fallback logic.
3. `app/config.go`: `server.properties` parsing and metadata segment discovery.
4. `app/encoding.go`: primitive protocol/metadata decoders and append helpers.

## Architecture Notes

See `docs/ARCHITECTURE.md` for design decisions, metadata pipeline details, and known limitations.

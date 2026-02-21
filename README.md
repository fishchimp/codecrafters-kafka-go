[![progress-banner](https://backend.codecrafters.io/progress/kafka/60f8d06b-5176-483d-9af7-40d6c8f9dd6a)](https://app.codecrafters.io/users/codecrafters-bot?r=2qF)

This is a starting point for Go solutions to the
["Build Your Own Kafka" Challenge](https://codecrafters.io/challenges/kafka).

In this challenge, you'll build a toy Kafka clone that's capable of accepting
and responding to ApiVersions & Fetch API requests. You'll also learn about
encoding and decoding messages using the Kafka wire protocol. You'll also learn
about handling the network protocol, event loops, TCP sockets and more.

**Note**: If you're viewing this repo on GitHub, head over to
[codecrafters.io](https://codecrafters.io) to try the challenge.

# Passing the first stage

The entry point for your Kafka implementation is in `app/main.go`. Study and
uncomment the relevant code, and push your changes to pass the first stage:

```sh
git commit -am "pass 1st stage" # any msg
git push origin master
```

That's all!

# Stage 2 & beyond

Note: This section is for stages 2 and beyond.

1. Ensure you have `go (1.25)` installed locally
1. Run `./your_program.sh` to run your Kafka broker, which is implemented in
   `app/main.go`.
1. Commit your changes and run `git push origin master` to submit your solution
   to CodeCrafters. Test output will be streamed to your terminal.

# Implementation Status (as of 21 Feb 2026)

## Complete Capabilities

### Basic
1. Bind to a port
1. Send Correlation ID
1. Parse Correlation ID
1. Parse API Version
1. Handle ApiVersions requests

### Concurrent Clients
1. Serial requests
1. Concurrent requests

### Listing Partitions
1. Include DescribeTopicPartitions in ApiVersions
1. List for an unknown topic
1. List for a single partition

## Incomplete

### Listing Partitions
1. List for multiple partitions
1. List for multiple topics

### Consuming Messages
1. Include Fetch in ApiVersions
1. Fetch with no topics
1. Fetch with an unknown topic
1. Fetch with an empty topic
1. Fetch single message from disk
1. Fetch multiple messages from disk

### Producing Messages
1. Include Produce in ApiVersions
1. Respond for invalid topic or partition
1. Respond for valid topic and partition
1. Produce a single record
1. Produce multiple records
1. Produce to multiple partitions
1. Produce to multiple partitions of multiple topics

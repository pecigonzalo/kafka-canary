# Kafka Canary

This is a `kafka-go` implementation of the [strimzi-canary](https://github.com/strimzi/strimzi-canary), with the goal of being independant of the Strimzi platform while taking a lot of inspiration from it.
The canary tool acts as an indicator of whether Apache Kafka® clusters are operating correctly. This is achieved by creating a canary topic and periodically producing and consuming events on the topic and getting metrics out of these exchanges.

## Motivation

The `kafka-go` library provides a better interface than `sarama` which is used by `strimzi-canary` and its easier to maintain. Furthermore, `strimzi-canary` goals are aligned with those of the Strimzi platform. The goal of this project is to provide a general canary implementation, to be used with Strimzi, DYI Kafka, AWS MSK, etc.

## State

⚠️ In Progress⚠️

- [ ] Behaviour tests
- [ ] Integration tests
- [x] Implement core monitors
  - [x] Topic activities
  - [x] Producer
  - [x] Consumer
- [x] Configuration
  - [x] Environment variables
  - [x] Configuration File
  - [x] Arguments
- [x] Support files
  - [x] Kuberentes deployment example
  - [x] Helm chart
- [ ] Usage docs

## Requirements

- [Go](https://golang.org/doc/install) >= 1.20

### Optional

- [direnv](https://direnv.net/)
- [Nix](https://nixos.org/) with [Flakes](https://nixos.wiki/wiki/Flakes)

## Thanks

- [strimzi-canary](https://github.com/strimzi/strimzi-canary) - For the original idea and implementation
- [topicctl](https://github.com/segmentio/topicctl) - For the client implementation

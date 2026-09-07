# Chitragupta

[![CI](https://github.com/waliaabhishek/chitragupta/actions/workflows/ci.yml/badge.svg)](https://github.com/waliaabhishek/chitragupta/actions/workflows/ci.yml)
[![codecov](https://img.shields.io/codecov/c/github/waliaabhishek/chitragupta)](https://codecov.io/gh/waliaabhishek/chitragupta)
[![Python 3.14+](https://img.shields.io/badge/python-3.14%2B-blue)](https://www.python.org/downloads/)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![mypy](https://img.shields.io/badge/type--checked-mypy-blue)](https://mypy-lang.org/)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)

> In Hindu tradition, [Chitragupta](https://en.wikipedia.org/wiki/Chitragupta) is the deity who maintains a complete record of every being's actions; the divine accountant himself. Fitting name for a system that tracks exactly who used what and how much it cost.

Understand where your infrastructure spend goes and allocate it to the teams and service accounts using it. Chitragupta brings together billing, resource inventory, and usage data across Confluent Cloud, self-managed Kafka, and Prometheus-instrumented systems.


> [!IMPORTANT]
> The v2 version is a complete rewrite from ground up for a full plugin architecture, multi-tenancy, FastAPI, proper storage layer with mitigations, emitter framework, docs site.
> Essentially an entirely new system with a lot more features and a much better performance profile.
> The goal is to keep adding more features and improvements as I go along and as more requests come in.

## See it working

Start with total spend, explore the resources behind it, and inspect how Kafka
cluster costs are distributed across topics. These examples use synthetic data.

### Watch a cost investigation

Follow a cost increase from the dashboard to a Kafka topic, compare daily
amounts, and inspect processing status and reporting options.

https://github.com/user-attachments/assets/2a67c100-83c8-4bc0-9f46-4e386959c30b

### Explore the resources behind the bill

Navigate from a tenant to its environments and resources in Cost Explorer.

![Cost Explorer showing a synthetic tenant and its Analytics, Commerce, Fulfillment, and Logistics environments](https://github.com/waliaabhishek/chitragupta/releases/download/demo-media/chitragupta-demo-cost-explorer.png)

### Find the topics contributing most to cost

Compare topic costs in a treemap, then filter by cluster, topic, or date to
investigate further.

![Topic Attribution treemap showing the relative costs of synthetic Kafka topics](https://github.com/waliaabhishek/chitragupta/releases/download/demo-media/chitragupta-demo-topic-attribution.png)

## Try it without credentials

Use the demo for a local evaluation or a presentation. You need Git and Docker
with Compose; no provider API keys or secrets are required.

```bash
git clone https://github.com/waliaabhishek/chitragupta.git
cd chitragupta
./demo --showcase
```

Open **<http://127.0.0.1:8081>** once startup completes. Showcase includes cost
changes to investigate; run `./demo` for the Clean profile with healthy,
fully allocated data.

The [Demo guide](https://waliaabhishek.github.io/chitragupta/latest/getting-started/demo/) walks through a cost investigation
and explains how to stop, reset, or customize the demo.

## Connect your own environment

| Ecosystem | Cost source |
|---|---|
| Confluent Cloud | Confluent Cloud billing API |
| Self-managed Kafka | Your configured cost model and Prometheus usage metrics |
| Generic metrics | Your configured cost model and Prometheus usage metrics |

Follow the [Quickstart](https://waliaabhishek.github.io/chitragupta/latest/getting-started/quickstart/) to configure credentials
and start your deployment. The [configuration reference](https://waliaabhishek.github.io/chitragupta/latest/configuration/)
covers each ecosystem.

You can filter and group allocated costs with tags, query results through the
REST API, and export data for reporting. Confluent Cloud also offers a
[FOCUS Mapping Preview](https://waliaabhishek.github.io/chitragupta/latest/focus-mapping-preview/), with documented conformance
limitations.

## Documentation

- [Documentation website](https://waliaabhishek.github.io/chitragupta/latest/)
- [Demo guide](https://waliaabhishek.github.io/chitragupta/latest/getting-started/demo/)
- [Configuration](https://waliaabhishek.github.io/chitragupta/latest/configuration/)
- [Deployment and operations](https://waliaabhishek.github.io/chitragupta/latest/operations/)
- [API reference](https://waliaabhishek.github.io/chitragupta/latest/api-reference/)
- [Upgrading](https://waliaabhishek.github.io/chitragupta/latest/operations/upgrading/)

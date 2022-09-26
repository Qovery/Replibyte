<p align="center"> <img src="assets/RepliByte%20Logo.png" alt="replibyte logo"/> </p>

<h3 align="center">Seed Your Development Database With Real Data ⚡️</h3>
<p align="center">Replibyte is a blazingly fast tool to seed your databases with your production data while keeping sensitive data safe 🔥</p>

<p align="center">
<a href="https://opensource.org/licenses/MIT"> <img alt="MIT License" src="https://img.shields.io/badge/License-MIT-yellow.svg"> </a>
<img src="https://img.shields.io/badge/stability-stable-green.svg?style=flat-square" alt="stable badge">
<img src="https://img.shields.io/badge/stability-stable-green.svg?style=flat-square" alt="stable badge">
<img src="https://github.com/Qovery/replibyte/actions/workflows/build-and-test.yml/badge.svg?style=flat-square" alt="Build and Tests">
<a href="https://discord.qovery.com"> <img alt="Discord" src="https://img.shields.io/discord/688766934917185556?label=discord&style=flat-square"> </a>
</p>

## Prerequisites

- MacOSX / Linux / Windows
- Nothing more! Replibyte is stateless and does not require anything special.

## Usage

Create a dump

```shell
replibyte -c conf.yaml dump create
```

List all dumps

```shell
replibyte -c conf.yaml dump list

type          name                  size    when                    compressed  encrypted
PostgreSQL    dump-1647706359405    154MB   Yesterday at 03:00 am   true        true
PostgreSQL    dump-1647731334517    152MB   2 days ago at 03:00 am  true        true
PostgreSQL    dump-1647734369306    149MB   3 days ago at 03:00 am  true        true
```

Restore the latest dump in a local container

```shell
replibyte -c conf.yaml dump restore local -v latest -i postgres -p 5432
```

Restore the latest dump in a remote database

```shell
replibyte -c conf.yaml dump restore remote -v latest
```

## Features

- [x] Support data dump and restore for PostgreSQL, MySQL and MongoDB
- [x] Analyze your data schema 🔎
- [x] Replace sensitive data with fake data
- [x] Works on large database (> 10GB)
- [x] Database Subsetting: Scale down a production database to a more reasonable size 🔥
- [x] Start a local database with the production data in a single command 🔥
- [x] On-the-fly data (de)compression (Zlib)
- [x] On-the-fly data de/encryption (AES-256)
- [x] Fully stateless (no server, no daemon) and lightweight binary 🍃
- [x] Use [custom transformers](examples/wasm)

Here are the features we plan to support

- [ ] Auto-detect and version database schema change
- [ ] Auto-detect sensitive fields
- [ ] Auto-clean backed up data

## Getting Started

1. [How Replibyte works](https://www.replibyte.com/docs/how-replibyte-works)
2. Initial setup: 
   1. [Install](https://www.replibyte.com/docs/getting-started/installation)
   2. [Configure](https://www.replibyte.com/docs/getting-started/configuration)
3. Step-by-step guides:
   1. [Create a dump](https://www.replibyte.com/docs/guides/create-a-dump)
   2. [Restore a dump](https://www.replibyte.com/docs/guides/restore-a-dump)
   3. [Subset a dump](https://www.replibyte.com/docs/guides/subset-a-dump)
   4. [Delete a dump](https://www.replibyte.com/docs/guides/delete-a-dump)
   5. Deploy Replibyte
      1. [Container](https://www.replibyte.com/docs/guides/deploy-replibyte/container)
      2. [Qovery](https://www.replibyte.com/docs/guides/deploy-replibyte/qovery)

## Demo

[![What is RepliByte](assets/video_.png)](https://www.youtube.com/watch?v=IKeLnZvECQw)

## Contributing

Check [here](https://www.replibyte.com/docs/contributing).

## Thanks

Thanks to all people sharing their ideas to make Replibyte better. We do appreciate it. I would also thank [AirByte](https://airbyte.com/),
a great product and a trustworthy source of inspiration for this project.

---

Replibyte is initiated and maintained by [Qovery](https://www.qovery.com?ref=replibyte-readme). 

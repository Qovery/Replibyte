<p align="center"> <img src="assets/RepliByte_.png" alt="replibyte logo"/> </p>

<h3 align="center">The Simplest Way To Synchronize Your Cloud Databases</h3>
<p align="center">Replibyte is an application to replicate your cloud databases </br>from one place to the other while hiding sensitive data 🕵️‍♂️</p>

<p align="center">
<img src="https://img.shields.io/badge/stability-work_in_progress-lightgrey.svg?style=flat-square" alt="work in progress badge">
<img src="https://github.com/Qovery/replibyte/actions/workflows/build-and-test.yml/badge.svg?style=flat-square" alt="Build and Tests">
<a href="https://discord.qovery.com"> <img alt="Discord" src="https://img.shields.io/discord/688766934917185556?label=discord&style=flat-square"> </a>
</p>

---

**⚠️ DEVELOPMENT IN PROGRESS - CONTRIBUTORS WANTED!! [JOIN DISCORD](https://discord.qovery.com)**

---

## Install

*The installation from the package managers is coming soon*

### Requirements for Postgres

You need to have **pg_dump** and **psql** binaries installed on your machine. [Download Postgres](https://www.postgresql.org/download/).

```shell
git clone https://github.com/Qovery/replibyte.git

# you need to install rust compiler before
cargo build --release

# feel free to move the binary elsewhere
./target/release/replibyte
```

[//]: # (For MacOS)

[//]: # (```)

[//]: # (# Add Replibyte brew repository)

[//]: # (brew tap Qovery/replibyte)

[//]: # ()

[//]: # (# Install the CLI)

[//]: # (brew install replibyte)

[//]: # (```)

[//]: # ()

[//]: # (For Linux)

[//]: # (```)

[//]: # (bash)

[//]: # (```)

## Usage

Example with Postgres as a *Source* and *Destination* database **AND** S3 as a *Bridge* (cf [configuration file](#Configuration))

Backup your Postgres databases into S3

```shell
replibyte backup -c prod-conf.yaml
```

Restore your Postgres databases from S3

```shell
replibyte backup list -c prod-conf.yaml

type          name                    size    when
PostgreSQL    backup-1647706359405    154MB   Yesterday at 03:00 am
PostgreSQL    backup-1647731334517    152MB   2 days ago at 03:00 am
PostgreSQL    backup-1647734369306    149MB   3 days ago at 03:00 am
```

```shell
replibyte restore latest -c prod-conf.yaml

OR 

replibyte restore backup-1647706359405 -c prod-conf.yaml
```

### Configuration

Create your `prod-conf.yaml` configuration file to source your production database.

```yaml
source:
  connection_uri: $DATABASE_URL
  transformers:
    - database: public
      table: employees
      columns:
        - name: last_name
          transformer: random
        - name: birth_date
          transformer: random-date
        - name: first_name
          transformer: first-name
bridge:
  bucket: $BUCKET_NAME
  access_key_id: $ACCESS_KEY_ID
  secret_access_key: $AWS_SECRET_ACCESS_KEY
```

Run the app for the source

```shell
replibyte -c prod-conf.yaml
```

### Destination

Create your `staging-conf.yaml` configuration file to sync your production database with your staging database.

```yaml
bridge:
  bucket: $BUCKET_NAME
  access_key_id: $ACCESS_KEY_ID
  secret_access_key: $AWS_SECRET_ACCESS_KEY
destination:
  connection_uri: $DATABASE_URL
```

Run the app for the destination

```shell
replibyte -c staging-conf.yaml
```

## How RepliByte works

RepliByte is built to replicate small and very large databases from one place (source) to the other (destination) with a bridge as
intermediary (bridge). Here is an example of what happens while replicating a Postgres database.

```mermaid
sequenceDiagram
    participant RepliByte
    participant Postgres (Source)
    participant AWS S3 (Bridge)
    Postgres (Source)->>RepliByte: 1. Dump data
    loop Transformer
        RepliByte->>RepliByte: 2. Obfuscate sensitive data
    end
    RepliByte->>AWS S3 (Bridge): 3. Upload obfuscated dump data
    RepliByte->>AWS S3 (Bridge): 4. Write index file
```

1. RepliByte connects to the _Postgres Source_ database and makes a full SQL dump of it.
2. RepliByte receives the SQL dump, parse it, and generates random/fake information in real-time.
3. RepliByte streams and uploads the modified SQL dump in real-time on AWS S3.
4. RepliByte keeps track of the uploaded SQL dump by writing it into an index file.

---

Once at least a replica from the source Postgres database is available in the S3 bucket, RepliByte can use and inject it into the
destination PostgresSQL database.

```mermaid
sequenceDiagram
    participant RepliByte
    participant Postgres (Destination)
    participant AWS S3 (Bridge)
    AWS S3 (Bridge)->>RepliByte: 1. Read index file
    AWS S3 (Bridge)->>RepliByte: 2. Download dump SQL file
    RepliByte->>Postgres (Destination): 1. Restore dump SQL
```

1. RepliByte connects to the S3 bucket and reads the index file to retrieve the latest SQL to download.
2. RepliByte downloads the SQL dump in a stream bytes.
3. RepliByte restores the SQL dump in the destination Postgres database in real-time.

## Features

- [x] Complete data synchronization
- [x] Backup TB of data (read [Design](#design))
- [x] Work on different VPC/network
- [x] Generate random/fake information

Here are the features we plan to support

- [ ] Incremental data synchronization
- [ ] Auto-clean up bridge data

## Connectors

### Supported Source connectors

- [x] PostgreSQL
- [ ] MySQL (Coming Soon)
- [ ] MongoDB (Coming Soon)

### Supported Transformers

A transformer is useful to change / hide the value of a column. RepliByte provides pre-made transformers.

Check out the [list of our available Transformers](TRANSFORMERS.md)

### RepliByte Bridge

The S3 wire protocol, used by RepliByte bridge, is supported by most cloud providers. Here is a non-exhaustive list of S3 compatible
services.

| Cloud Service Provider | S3 service name                                                           | S3 compatible  |
|------------------------|---------------------------------------------------------------------------|----------------|
| Amazon Web Services    | [S3](https://aws.amazon.com/s3/)                                          | Yes (Original) |
| Google Cloud Platform  | [Cloud Storage](https://cloud.google.com/storage)                         | Yes            |
| Microsoft Azure        | [Blob Storage](https://azure.microsoft.com/en-us/services/storage/blobs/) | Yes            |
| Digital Ocean          | [Spaces](https://www.digitalocean.com/products/spaces)                    | Yes            |
| Scaleway               | [Object Storage](https://www.scaleway.com/en/object-storage/)             | Yes            |

> Feel free to drop a PR to include another S3 compatible solution.

### Supported Destination connectors

- [x] PostgreSQL
- [ ] MySQL (Coming Soon)
- [ ] MongoDB (Coming Soon)

## Design

### Low Memory and CPU footprint

Written in Rust, RepliByte can run with 512 MB of RAM and 1 CPU to replicate 1 TB of data (we are working on a benchmark). RepliByte
replicate the data in a stream of bytes and does not store anything on a local disk.

### Limitations

- Tested with Postgres 13 and 14. It should work with prior versions.

### Index file structure

An index file describe the structure of your backups and all of them.

Here is the manifest file that you can find at the root of your target `Bridge` (E.g: S3).

```json
{
  "backups": [
    {
      "size": 1024000,
      "directory_name": "timestamp",
      "created_at": "iso8601 date format"
    }
  ]
}
```

## Motivation

At [Qovery](https://www.qovery.com) (the company behind RepliByte), developers can clone their applications and databases just with one
click. However, the cloning process can be tedious and time-consuming, and we end up copying the information multiple times. With RepliByte,
the Qovery team wants to provide a comprehensive way to seed cloud databases from one place to another.

The long-term motivation behind RepliByte is to provide a way to clone any database in real-time. This project starts small, but has big
ambition!

## Use cases

| Scenario                                                                          | Supported |
|-----------------------------------------------------------------------------------|-----------|
| Synchronize the whole Postgres instance                                           | Yes       |
| Synchronize the whole Postgres instance and replace sensitive data with fake data | Yes       |
| Synchronize specific Postgres tables and replace sensitive data with fake data    | WIP       |
| Synchronize specific Postgres databases and replace sensitive data with fake data | WIP       |
| Migrate from one database hosting platform to the other                           | Yes       |

> Do you want to support an additional use-case? Feel free to [contribute](#contributing) by opening an issue or submitting a PR.

## What is not RepliByte

### RepliByte is not an ETL

RepliByte is not an ETL like [AirByte](https://github.com/airbytehq/airbyte), [AirFlow](https://airflow.apache.org/), Talend, and it will
never be. If you need to synchronize versatile data sources, you are better choosing a classic ETL. RepliByte is a tool for software
engineers to help them to synchronize data from the same databases. With RepliByte, you can only replicate data from the same type of
databases. As mentioned above, the primary purpose of RepliByte is to duplicate into different environments. You can see RepliByte as a
specific use case of an ETL, where an ETL is more generic.

### RepliByte is not a database backup tool

Even if you can use RepliByte as a database backup tool, we have not designed it this way. It might change in the future, but at the moment
RepliByte has not been designed to make all the checks needed to guarantee that the backup is consistent.

## FAQ

⬆️ _Open an issue if you have any question - I'll pick the most common questions and put them here with the answer_

# Contributing

## Local development

For local development, you will need to install [Docker](https://www.docker.com/) and run `docker-compose up` to start the local databases.
At the moment, `docker-compose` includes 2 Postgres database instances. One source and one destination database. In the future, we will
provide more options.

Once your Docker instances are running, you can run the RepliByte tests, to check if everything is configured correctly:

```shell
cargo test
```

## How to contribute

RepliByte is in its early stage of development and need some time to be usable in production. We need some help, and you are welcome to
contribute. To better synchronize consider joining our #replibyte channel on our [Discord](https://discord.qovery.com). Otherwise, you can
pick any open issues and contribute.

### Where should I start?

Check the open [issues](https://github.com/Qovery/replibyte/issues) and their priority.

### How can I contact you?

3 options:

1. Open an [issue](https://github.com/Qovery/replibyte/issues).
2. Join our #replibyte channel on [our discord](https://discord.qovery.com).
3. Drop us an email to `github+replibyte {at} qovery {dot} com`.

## Live Coding Session

Romaric, main contributor to RepliByte does some [live coding session on Twitch](https://www.twitch.tv/codewithromaric) to learn more about
RepliByte and explain how to develop in Rust. Feel free to [join the sessions](https://www.twitch.tv/codewithromaric).

## Thanks

Thanks to all people sharing their ideas to make RepliByte better. We do appreciate it. I would also thank [AirByte](https://airbyte.com/),
a great product and a trustworthy source of inspiration for this project.

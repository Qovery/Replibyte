<p align="center"> <img src="assets/RepliByte%20Logo.png" alt="replibyte logo"/> </p>

<h3 align="center">Seed Your Development Database With Real Data ⚡️</h3>
<p align="center">Replibyte is a powerful tool to seed your databases </br>with real data and other cool features 🔥</p>

<p align="center">
<img src="https://img.shields.io/badge/stability-stable-green.svg?style=flat-square" alt="stable badge">
<img src="https://github.com/Qovery/replibyte/actions/workflows/build-and-test.yml/badge.svg?style=flat-square" alt="Build and Tests">
<a href="https://discord.qovery.com"> <img alt="Discord" src="https://img.shields.io/discord/688766934917185556?label=discord&style=flat-square"> </a>
</p>

## Features

- [x] Support data backup and restore for PostgreSQL, MySQL and MongoDB
- [x] Replace sensitive data with fake data
- [x] Works on large database (> 10GB) (read [Design](../docs/DESIGN.md))
- [x] Database Subsetting: Scale down a production database to a more reasonable size 🔥
- [x] Start a local database with the prod data in a single command 🔥
- [x] On-the-fly data (de)compression (Zlib)
- [x] On-the-fly data de/encryption (AES-256)
- [x] Fully stateless (no server, no daemon) and lightweight binary 🍃
- [x] Use [custom transformers](examples/wasm)

Here are the features we plan to support

- [ ] Auto-detect and version database schema change
- [ ] Auto-detect sensitive fields
- [ ] Auto-clean backed up data

## Install

<details>

<summary>Install on MacOSX</summary>

⚠️ RepliByte homebrew auto release is in maintenance. Consider using Docker or building from source in the meantime ⚠️

```shell
brew tap Qovery/replibyte
brew install replibyte
```

Or [manually](https://github.com/Qovery/replibyte/releases).

</details>

<details>

<summary>Install on Linux</summary>

```shell
# download latest replibyte archive for Linux
curl -s https://api.github.com/repos/Qovery/replibyte/releases/latest | \
    jq -r '.assets[].browser_download_url' | \
    grep -i 'linux-musl.tar.gz$' | wget -qi - && \

# unarchive
tar zxf *.tar.gz

# make replibyte executable
chmod +x replibyte

# make it accessible from everywhere
mv replibyte /usr/local/bin/
```

</details>

<details>

<summary>Install on Windows</summary>

Download [the latest Windows release](https://github.com/Qovery/replibyte/releases) and install it.

</details>

<details>

<summary>Install from source</summary>

```shell
git clone https://github.com/Qovery/replibyte.git && cd replibyte 

# Install cargo
# visit: https://doc.rust-lang.org/cargo/getting-started/installation.html

# Build with cargo
cargo build --release

# Run RepliByte
./target/release/replibyte -h
```

</details>

<details>

<summary>Run replibyte with Docker</summary>

```shell
git clone https://github.com/Qovery/replibyte.git

# Build image with Docker
docker build -t replibyte -f Dockerfile .

# Run RepliByte
docker run -v $(pwd)/examples:/examples/ replibyte -c /examples/replibyte.yaml transformer list
```

Feel free to edit `./examples/replibyte.yaml` with your configuration.

</details>

## Usage

[![What is RepliByte](assets/video_.png)](https://www.youtube.com/watch?v=IKeLnZvECQw)

Example with PostgreSQL as a _Source_ and _Destination_ database **AND** S3 as a _Bridge_ (cf [configuration file](#Configuration))

### Create a dev database dataset from your production database

<details>

<summary>Show me</summary>

```shell
replibyte -c prod-conf.yaml backup run
```

*The backup is compressed and stored on your S3 bucket (cf [configuration](#configuration)).*

</details>

### Create a dev database dataset from a dump file

<details>

<summary>Show me</summary>

```shell
cat dump.sql | replibyte -c prod-conf.yaml backup run -s postgres -i
```

*The backup is compressed and stored on your S3 bucket (cf [configuration](#configuration)).*

</details>

### Seed my local database (Docker required)

<details>

<summary>Show me</summary>

List all your backups to choose one:

```shell
replibyte -c prod-conf.yaml backup list

type          name                    size    when                    compressed  encrypted
PostgreSQL    backup-1647706359405    154MB   Yesterday at 03:00 am   true        true
PostgreSQL    backup-1647731334517    152MB   2 days ago at 03:00 am  true        true
PostgreSQL    backup-1647734369306    149MB   3 days ago at 03:00 am  true        true
```

Restore the latest one into a Postgres container bound on 5433 (default: 5432) port:

```shell
replibyte -c prod-conf.yaml restore local -v latest --image postgres --port 5433

To connect to your Postgres database, use the following connection string:
> postgres://postgres:password@localhost:5433/postgres
Waiting for Ctrl-C to stop the container
```

OR restore a specific one:

```
replibyte -c prod-conf.yaml restore local -v backup-1647706359405 --image postgres --port 5433
```

</details>

*The seed comes from your S3 bucket (cf [configuration](#configuration))*

### Seed a remote database

<details>

<summary>Show me</summary>

Show your backups:

```shell
replibyte -c prod-conf.yaml backup list

type          name                    size    when                    compressed  encrypted
PostgreSQL    backup-1647706359405    154MB   Yesterday at 03:00 am   true        true
PostgreSQL    backup-1647731334517    152MB   2 days ago at 03:00 am  true        true
PostgreSQL    backup-1647734369306    149MB   3 days ago at 03:00 am  true        true
```

Restore the latest one:

```shell
replibyte -c prod-conf.yaml restore remote -v latest
```

OR restore a specific one:

```
replibyte -c prod-conf.yaml restore remote -v backup-1647706359405
```

*The seed comes from your S3 bucket (cf [configuration](#configuration))*

</details>

### Configuration

Create your `prod-conf.yaml` configuration file to source your production database.

```yaml
encryption_key: $MY_PRIVATE_ENC_KEY # optional - encrypt data on bridge
source:
  connection_uri: $DATABASE_URL
  database_subset: # optional - downscale database while keeping it consistent
    database: public
    table: orders
    strategy_name: random
    strategy_options:
      percent: 50
    passthrough_tables:
      - us_states
  transformers: # optional - hide sensitive data
    - database: public
      table: employees
      columns:
        - name: last_name
          transformer_name: random
        - name: birth_date
          transformer_name: random-date
        - name: first_name
          transformer_name: first-name
        - name: email
          transformer_name: email
        - name: username
          transformer_name: keep-first-char
    - database: public
      table: customers
      columns:
        - name: phone
          transformer_name: phone-number
bridge:
  bucket: $BUCKET_NAME
  region: $S3_REGION
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
  region: $S3_REGION
  access_key_id: $ACCESS_KEY_ID
  secret_access_key: $AWS_SECRET_ACCESS_KEY
destination:
  connection_uri: $DATABASE_URL
encryption_key: $MY_PRIVATE_ENC_KEY # optional - needed to decrypt data on bridge if there was an encryption_key defined when running the source backup
```

Run the app for the destination

```shell
replibyte -c staging-conf.yaml
```

## How RepliByte works

<details>

<summary>Show me how RepliByte works</summary>

Check out our [Design page](docs/DESIGN.md)

</details>

## Connectors

### Supported Source connectors

- [x] PostgreSQL
- [x] MongoDB
- [x] Local dump file
- [x] MySQL

### Supported Transformers

A transformer is useful to change / hide the value of a column. RepliByte provides pre-made transformers.

Check out the [list of our available Transformers](docs/TRANSFORMERS.md)

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
| Minio                  | [Object Storage](https://min.io/)                                         | Yes            |

> Feel free to drop a PR to include another S3 compatible solution.

### Supported Destination connectors

- [x] PostgreSQL
- [x] MongoDB
- [x] Local dump file
- [x] MySQL

## Motivation

At [Qovery](https://www.qovery.com) (the company behind RepliByte), developers can clone their applications and databases just with one
click. However, the cloning process can be tedious and time-consuming, and we end up copying the information multiple times. With RepliByte,
the Qovery team wants to provide a comprehensive way to seed cloud databases from one place to another.

The long-term motivation behind RepliByte is to provide a way to clone any database in real-time. This project starts small, but has big
ambition!

## FAQ

### Q: Does RepliByte is an ETL?

<details>

<summary>Answer</summary>

RepliByte is not an ETL like [AirByte](https://github.com/airbytehq/airbyte), [AirFlow](https://airflow.apache.org/), Talend, and it will
never be. If you need to synchronize versatile data sources, you are better choosing a classic ETL. RepliByte is a tool for software
engineers to help them to synchronize data from the same databases. With RepliByte, you can only replicate data from the same type of
databases. As mentioned above, the primary purpose of RepliByte is to duplicate into different environments. You can see RepliByte as a
specific use case of an ETL, where an ETL is more generic.

</details>

### Q: Do you support backup from a dump file?

<details>

<summary>Answer</summary>

absolutely,

```shell
cat dump.sql | replibyte -c prod-conf.yaml backup run -s postgres -i
```

and

```shell
replibyte -c prod-conf.yaml backup run -s postgres -f dump.sql
```

</details>

### How RepliByte can list the backups? Is there an API?

<details>

<summary>Answer</summary>

There is no API, RepliByte is fully stateless and store the backup list into the bridge (E.g. S3) via an [index_file](#index-file-structure)
.

---

⬆️ _Open an issue if you have any question - I'll pick the most common questions and put them here with the answer_

</details>

# Contributing

<details>

<summary>Show me how to contribute</summary>

## Local development

For local development, you will need to install [Docker](https://www.docker.com/) and run `docker compose -f ./docker-compose-dev.yml` to
start the local databases. At the moment, `docker-compose` includes 2 PostgreSQL database instances, 2 MySQL instances, 2 MongoDB instances
and a [MinIO](https://min.io/) bridge. One source, one destination by database and one bridge. In the future, we will provide more options.

The Minio console is accessible at http://localhost:9001.

Once your Docker instances are running, you can run the RepliByte tests, to check if everything is configured correctly:

```shell
AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin cargo test
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

</details>

## Telemetry

<details>

<summary>Show me</summary>

RepliByte collects anonymized data from users in order to improve our product. Feel free to inspect the
code [here](replibyte/src/telemetry.rs). This can be deactivated at any time, and any data that has already been collected can be deleted on
request (hello+replibyte {at} qovery {dot} com).

### Collected data

- Command line parameters
- Options used (subset, transformer, compression) in the configuration file.

</details>

## Thanks

Thanks to all people sharing their ideas to make RepliByte better. We do appreciate it. I would also thank [AirByte](https://airbyte.com/),
a great product and a trustworthy source of inspiration for this project.

## Additional resources

- [RepliByte Design](docs/DESIGN.md)
- [Transformers](docs/TRANSFORMERS.md)

---
sidebar_position: 4
---

# Configuration

Create your `conf.yaml` configuration file to source your production database.

```yaml
encryption_key: $MY_PRIVATE_ENC_KEY # optional - encrypt data on datastore
source:
  connection_uri: postgres://user:password@host:port/db # you can use $DATABASE_URL
datastore:
  aws:
    bucket: $BUCKET_NAME
    region: $S3_REGION
    credentials:
      access_key_id: $ACCESS_KEY_ID
      secret_access_key: $AWS_SECRET_ACCESS_KEY
destination:
  connection_uri: postgres://user:password@host:port/db # you can use $DATABASE_URL
```

:::info

Environment variables are substituted by their value at runtime. An error is thrown if the environment variable does not exist.

:::

Run the app for the source:

```shell
replibyte -c conf.yaml
```

## Source and Destination

Replibyte supports multiple databases.

- [PostgreSQL](/docs/databases#postgresql)
- [MySQL](/docs/databases#mysql)
- [MongoDB](/docs/databases#mongodb)

## Transformer

A transformer is useful to change/hide the value of a specified column. Replibyte provides pre-made transformers. You can
also [build your own Transformer in web assembly](/docs/transformers#wasm).

Here is a list of all the [transformers available](/docs/transformers).

| id              | description                                                                                        | doc                                             |
|-----------------|----------------------------------------------------------------------------------------------------|-------------------------------------------------|
| transient       | Does not modify the value                                                                          | [link](/docs/transformers#transient)            |
| random          | Randomize value but keep the same length (string only). [AAA]->[BBB]                               | [link](/docs/transformers#random)               |
| first-name      | Replace the string value by a first name                                                           | [link](/docs/transformers#first-name)           |
| email           | Replace the string value by an email address                                                       | [link](/docs/transformers#email)                |
| keep-first-char | Keep only the first char for strings and digit for numbers                                         | [link](/docs/transformers#keep-first-character) |
| phone-number    | Replace the string value by a phone number                                                         | [link](/docs/transformers#phone-number)         |
| credit-card     | Replace the string value by a credit card number                                                   | [link](/docs/transformers#credit-card)          |
| redacted        | Obfuscate your sensitive data (>3 characters strings only). [4242 4242 4242 4242]->[424**********] | [link](/docs/transformers#redacted)             |

## Datastore

A Datastore is where Replibyte store the created dump to make them accessible from the destination databases.

| Cloud Service Provider | S3 service name                                                           | S3 compatible  |
|------------------------|---------------------------------------------------------------------------|----------------|
| Amazon Web Services    | [S3](https://aws.amazon.com/s3/)                                          | Yes (Original) |
| Google Cloud Platform  | [Cloud Storage](https://cloud.google.com/storage)                         | Yes            |
| Microsoft Azure        | [Blob Storage](https://azure.microsoft.com/en-us/services/storage/blobs/) | Yes            |
| Digital Ocean          | [Spaces](https://www.digitalocean.com/products/spaces)                    | Yes            |
| Scaleway               | [Object Storage](https://www.scaleway.com/en/object-storage/)             | Yes            |
| Minio                  | [Object Storage](https://min.io/)                                         | Yes            |

:::info

Any datastore compatible with the S3 protocol is a valid datastore.

:::

## Select tables from the source

You have the possibility to select which tables you want in the dump.

There are two options to achieve this:
 1. By specifying a list of tables you want (include strategy)
 2. By skipping a list of tables you don't want (exclude strategy)

### Include strategy (`only_tables`)

Add a key named `only_tables` under the `source`:

```yaml
source:
  connection_uri: postgres://root:password@localhost:5432/root
  only_tables: # optional - dumps only specified tables.
    - database: public
      table: orders
    - database: public
      table: customers
```

### Exclude strategy (`skip_tables`)

Add a key named `skip` under the `source`:

```yaml
source:
  connection_uri: postgres://root:password@localhost:5432/root
  skip: # optional - exclude from the dump the specified tables.
    - database: public
      table: us_states
    - database: public
      table: order_details
```

This will exclude from the dump the tables `us_states` and `order_details`.

:::warning

This will exclude the table schema AND the table data.
:::

:::warning

Currently only PostgreSQL and MySQL sources support this feature.
:::

## Example

Here is a configuration file including some transformations and different options like the database subset.

```yaml
encryption_key: $MY_PRIVATE_ENC_KEY # optional - encrypt data on datastore
source:
  connection_uri: postgres://user:password@host:port/db # you can use $DATABASE_URL
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
  only_tables: # optional - dumps only specified tables.
    - database: public
      table: orders
    - database: public
      table: customers
datastore:
  aws:
    bucket: $BUCKET_NAME
    region: $S3_REGION
    credentials:
      access_key_id: $ACCESS_KEY_ID
      secret_access_key: $AWS_SECRET_ACCESS_KEY
destination:
  connection_uri: postgres://user:password@host:port/db # you can use $DATABASE_URL
```

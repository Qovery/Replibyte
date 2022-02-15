# DB Replicator

DB Replicator is a Golang application to load data from a production database to a staging or development one.

---------

**⚠️ THIS PROJECT IS NOT PRODUCTION READY YET!!**

---------

## Motivation

As a developer, synchronizing production database with a staging and development database is tedious. The goal of this application is to make database synchronization easy, safe and GDPR-compliant.

## Features

Here are some key features:

- [ ] Sync data when the database is not under load
- [ ] Version synced data
- [ ] Obfuscate sensitive data (GDPR and SOC friendly)

## Connectors

![connection diagram](assets/diagram.svg)

### Sources

Connector where to read the data.

- [ ] Postgres

### Bridge

Connector to make the bridge between the source and the destination.

- [ ] S3

### Destinations

Connector where to write the source data.

- [ ] Postgres

## Usage example

### Source
Create your `prod-conf.yaml` configuration file to source your production database.

```yaml
bind: 127.0.0.1
port: 1337 
source:
- type: postgres
  connection_uri: $DATABASE_URL
  cron: 0 3 * * * # every day at 3 am 
bridge:
- type: s3
  bucket: $BUCKET_NAME
  access_key_id: $ACCESS_KEY_ID
  secret_access_key: $AWS_SECRET_ACCESS_KEY
```

Run the app for the source
```shell
dbreplicator -c prod-conf.yaml
```

### Destination

Create your `staging-conf.yaml` configuration file to sync your production database with your staging database.

```yaml
bind: 127.0.0.1
port: 1338
bridge:
- type: s3
  bucket: $BUCKET_NAME
  access_key_id: $ACCESS_KEY_ID
  secret_access_key: $AWS_SECRET_ACCESS_KEY
destination:
- type: postgres
  connection_uri: $DATABASE_URL
  cron: 0 5 * * * # every day at 5 am
```

Run the app for the destination
```shell
dbreplicator -c staging-conf.yaml
```

## API

### Last sync status
```shell
curl -X GET http://localhost:1337/lastSyncStatus
```

Response:
```json
{
  "index": 1,
  "created_at": "2022-02-14T20:39:48:000Z",
  "status": "OK"
}
```

## Design

TODO

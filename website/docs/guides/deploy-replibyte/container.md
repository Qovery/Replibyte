---
title: Container
sidebar_position: 1
---

# Deploy Replibyte as a container

You are using Replibyte on your local machine to [create](/docs/guides/create-a-dump) and [restore dumps](/docs/guides/restore-a-dump), it's great, but now you might want to deploy it close to your production and development environments to automate the process. This step-by-step guide explains how to do it and share you best practices.

:::note for qovery users

To deploy with [Qovery](https://www.qovery.com), follow [this guide](/docs/guides/deploy-replibyte/qovery)

:::

Here is a schema of what we are going to put in place.

![schema Replibyte](/img/replibyte_dump_and_restore.jpg)

1. In production:
   1. Replibyte periodically dump the production database and..
   2. upload a dump **without the sensitive data** on a S3 bucket.
2. In development:
   1. Replibyte periodically restore the development database with the latest dump.

Let's go!

## Run Replibyte container locally

### Download the official Replibyte image

```sh
docker pull ghcr.io/qovery/replibyte
```

Check the [Github package](https://github.com/qovery/replibyte/pkgs/container/replibyte) for available tags (currently `latest` and per git release tag).

:::

### Create Replibyte configuration file

I will take our final `conf.yaml` file from the ["create a dump"](/docs/guides/create-a-dump) guide and rename it into `replibyte.yaml`.

:::caution

You must name your replibyte configuration file `replibyte.yaml`. Otherwise, it will not work.

:::

```yaml title="replibyte.yaml"
encryption_key: $ENCRYPTION_SECRET # put a secure secret here
source:
  connection_uri: $SOURCE_CONNECTION_URI
  transformers:
    - database: public
      table: customers
      columns:
        - name: first_name
          transformer_name: first-name
        - name: last_name
          transformer_name: random
        - name: contact_phone
          transformer_name: phone-number
        - name: contact_email
          transformer_name: email
datastore:
  aws:
    bucket: $S3_BUCKET
    region: $S3_REGION
    access_key_id: $S3_ACCESS_KEY_ID
    secret_access_key: $S3_SECRET_ACCESS_KEY
destination:
  connection_uri: $DESTINATION_CONNECTION_URI
```

And set your environment variables in a file. You can leave secure environment variables empty so that they read from the shell environment.

```sh
$ cat env.txt
S3_ACCESS_KEY_ID
S3_SECRET_ACCESS_KEY
S3_REGION=us-east-2
S3_BUCKET=my-test-bucket
SOURCE_CONNECTION_URI=postgres://...
DESTINATION_CONNECTION_URI=postgres://...
ENCRYPTION_SECRET
```

### Start the container

```sh
docker run -it --name replibyte \
    --env-file env.txt \
    -v "$(pwd)/replibyte.yaml":/replibyte.yaml:ro \
    ghcr.io/qovery/replibyte \
```

## Running in a cloud environment

### Deploy with Qovery

---

:::info

[Qovery](https://www.qovery.com) (the company behind Replibyte) is a platform used by more than 20 000 developers to deploy their apps on AWS in just a few seconds. Replibyte will be natively supported by Qovery in Q4 2022.

:::

To deploy Replibyte with Qovery - [here are the instructions](/docs/guides/deploy-replibyte/qovery).

---

### Self-hosted Deployment

This part depends on the platform (E.g Kubernetes, Docker Swarm, Nomad...) you use to deploy your containers. Basically, you just need to pull the container and run it with the right parameters.

### Parameters for production

Here is the command line to dump the production

```bash
docker run -e S3_ACCESS_KEY_ID=XXX \
           -e S3_SECRET_ACCESS_KEY=YYY \
           -e S3_REGION=us-east-2 \
           -e S3_BUCKET=my-test-bucket \
           -e SOURCE_CONNECTION_URI=postgres://... \
           -e DESTINATION_CONNECTION_URI=postgres://... \
           -e ENCRYPTION_SECRET=itIsASecret \
           ghcr.io/qovery/replibyte replibyte dump create
```

### Parameters to seed development databases

Here is the command line to seed your development database with the latest production dump

```bash
docker run -e S3_ACCESS_KEY_ID=XXX \
           -e S3_SECRET_ACCESS_KEY=YYY \
           -e S3_REGION=us-east-2 \
           -e S3_BUCKET=my-test-bucket \
           -e SOURCE_CONNECTION_URI=postgres://... \
           -e DESTINATION_CONNECTION_URI=postgres://... \
           -e ENCRYPTION_SECRET=itIsASecret \
           ghcr.io/qovery/replibyte replibyte dump restore remote -v latest
```

---

Do you have any questions? Feel free to join the channel #replibyte on [our Discord server](https://discord.qovery.com).

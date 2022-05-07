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

## Create a container with Replibyte

:::note

I assume Docker is already installed and running.

:::

To deploy Replibyte, I will put the Replibyte binary into a linux Docker container image. It is an easy way to make Replibyte working everywhere.

Here are the steps:

### Create an empty directory `replibyte-container`

```shell
mkdir replibyte-container && cd replibyt-container
```

### Create Replibyte configuration file

I will take our final `conf.yaml` file from the ["create a dump"](/docs/guides/create-a-dump) guide and rename it into `replibyte.yaml`.

:::caution

You must name your replibyte configuration file `replibyte.conf`. Otherwise, it will not work.

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
  bucket: $S3_BUCKET
  region: $S3_REGION
  access_key_id: $S3_ACCESS_KEY_ID
  secret_access_key: $S3_SECRET_ACCESS_KEY
destination:
  connection_uri: $DESTINATION_CONNECTION_URI
```

Save it into your `replibyte-container` directory.

### Create container

Copy/paste the following Dockerfile in your `replibyte-container` directory

```dockerfile title="Dockerfile"
FROM debian:buster as replibyte

RUN apt clean && apt update && apt install -y jq wget curl

WORKDIR replibyte

# Download latest Replibyte binary
RUN curl -s https://api.github.com/repos/Qovery/replibyte/releases/latest | jq -r '.assets[].browser_download_url' | grep -i 'tar.gz$' | wget -qi - && \
    tar zxf *.tar.gz && \
    mv `ls replibyte*-linux-musl` replibyte && \
    chmod +x replibyte

ENV PATH="/replibyte:${PATH}"

ARG S3_ACCESS_KEY_ID
ENV S3_ACCESS_KEY_ID $S3_ACCESS_KEY_ID

ARG S3_SECRET_ACCESS_KEY
ENV S3_SECRET_ACCESS_KEY $S3_SECRET_ACCESS_KEY

ARG S3_REGION
ENV S3_REGION $S3_REGION

ARG S3_BUCKET
ENV S3_BUCKET $S3_BUCKET

ARG SOURCE_CONNECTION_URI
ENV SOURCE_CONNECTION_URI $SOURCE_CONNECTION_URI

ARG DESTINATION_CONNECTION_URI
ENV DESTINATION_CONNECTION_URI $DESTINATION_CONNECTION_URI

ARG ENCRYPTION_SECRET
ENV ENCRYPTION_SECRET $ENCRYPTION_SECRET

COPY replibyte.yaml .
```

:::caution

Never hard code credentials in a Dockerfile

:::

If you run `ls -lh` you must see 2 files.

```shell
ls -lh

.rw-r--r--   890 xxx  6 May 14:00  Dockerfile
.rw-r--r--   588 xxx  6 May 14:00  replibyte.yaml
```

And now you can check that you successfully build your container with `docker build -f Dockerfile -t replibyte:latest .`

```shell
docker build -f Dockerfile -t replibyte:latest .


[+] Building 1.2s (10/10) FINISHED
 => [internal] load build definition from Dockerfile                                                                                                                                                                                                                                                                                                                         0.0s
 => => transferring dockerfile: 47B                                                                                                                                                                                                                                                                                                                                                    0.0s
 => [internal] load .dockerignore                                                                                                                                                                                                                                                                                                                                                      0.0s
 => => transferring context: 2B                                                                                                                                                                                                                                                                                                                                                        0.0s
 => [internal] load metadata for docker.io/library/debian:buster                                                                                                                                                                                                                                                                                                                       1.1s
 => [1/5] FROM docker.io/library/debian:buster@sha256:ebe4b9831fb22dfa778de4ffcb8ea0ad69b5d782d4e86cab14cc1fded5d8e761                                                                                                                                                                                                                                                                 0.0s
 => [internal] load build context                                                                                                                                                                                                                                                                                                                                                      0.0s
 => => transferring context: 36B                                                                                                                                                                                                                                                                                                                                                       0.0s
 => CACHED [2/5] RUN apt clean && apt update && apt install -y jq wget curl                                                                                                                                                                                                                                                                                                            0.0s
 => CACHED [3/5] WORKDIR replibyte                                                                                                                                                                                                                                                                                                                                                     0.0s
 => CACHED [4/5] RUN curl -s https://api.github.com/repos/Qovery/replibyte/releases/latest | jq -r '.assets[].browser_download_url' | grep -i 'tar.gz$' | wget -qi - &&     tar zxf *.tar.gz &&     mv `ls replibyte*-linux-musl` replibyte &&     chmod +x replibyte                                                                                                                  0.0s
 => CACHED [5/5] COPY replibyte.yaml .                                                                                                                                                                                                                                                                                                                                                 0.0s
 => exporting to image                                                                                                                                                                                                                                                                                                                                                                 0.0s
 => => exporting layers                                                                                                                                                                                                                                                                                                                                                                0.0s
 => => writing image sha256:1781d25593a71df493d686eb9db889a705949e6881be38b0d28cc436da8810f6
```

And you can try your container with `docker run replibyte:latest replibyte`

```shell
docker run replibyte:latest replibyte


replibyte 0.1.0
Qovery
RepliByte is a tool to synchronize cloud databases and fake sensitive data, just pass `-h`

USAGE:
    replibyte [OPTIONS] --config <configuration file> <SUBCOMMAND>

OPTIONS:
    -c, --config <configuration file>    replibyte configuration file
    -h, --help                           Print help information
    -n, --no-telemetry                   disable telemetry
    -V, --version                        Print version information

SUBCOMMANDS:
    dump           all backup commands
    help           Print this message or the help of the given subcommand(s)
    transformer    all transformers command
```

If you want to run your Replibyte command properly, you will need to pass all the arguments with the `docker run -e` parameter. E.g:

```shell
docker run -e S3_ACCESS_KEY_ID=XXX \
           -e S3_SECRET_ACCESS_KEY=YYY \
           -e S3_REGION=us-east-2 \
           -e S3_BUCKET=my-test-bucket \
           -e SOURCE_CONNECTION_URI=postgres://... \
           -e DESTINATION_CONNECTION_URI=postgres://... \
           -e ENCRYPTION_SECRET=itIsASecret \
           replibyte:latest replibyte dump create
```

## Deploy container

---

:::info

[Qovery](https://www.qovery.com) (the company behind Replibyte) is a platform used by more than 20 000 developers to deploy their apps on AWS in just a few seconds. Replibyte will be natively supported by Qovery in Q4 2022.

:::

To deploy Replibyte with Qovery - [here are the instructions](/docs/guides/deploy-replibyte/qovery).

---

Once you have built and tried your container, you need to push it into a Container Registry. The most popular one is [Docker Hub](https://hub.docker.com). But you can use any other Container Registry like [AWS ECR](https://aws.amazon.com/ecr/), [Google GCR](https://cloud.google.com/container-registry), [Quay](https://quay.io/)... Here, I will use Docker Hub which is free and easy to use.

To push your container image in Docker Hub you need to run the following commands:

:::tip

You need to sign up on [Docker Hub](https://hub.docker.com).

:::

```shell title="Auth yourself"
docker login --username=yourhubusername --email=youremail@company.com

WARNING: login credentials saved in /home/username/.docker/config.json
Login Succeeded
```

```shell
docker push your_docker_hub_username/replibyte:latest
```

That's it! You are ready to pull and run your replibyte image from anywhere.

## Deployment

This part depends on the platform (E.g Kubernetes, Docker Swarm, Nomad...) you use to deploy your containers. Basically, you just need to pull the container you pushed, and run it with the good parameters.

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
           replibyte:latest replibyte dump create
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
           replibyte:latest replibyte dump restore remote -v latest
```

---

Do you have any questions? Feel free to join the channel #replibyte on [our Discord server](https://discord.qovery.com).

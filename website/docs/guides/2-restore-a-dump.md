---
title: II. Restore a dump
sidebar_position: 2
---

# Restore a dump

:::note

I assume you did the previous guide, and you have your final `conf.yaml` file

:::

On the last step, we have created and uploaded our transformed dump in our S3 Datastore. Now, we are ready to restore it in a development database. 

:::note

The database where you restore must be the same type of the source where you dump. If you created a PostgreSQL dump, then you must restore on a PostgreSQL database.

:::

Replibyte provides you two options to restore a dump:

* **Option 1**: Locally - which is convenient for local development
  * Example use cases:
    * You develop an app locally and wants to work with real data.
    * You want to inspect what the transformed dump looks like.
* **Option 2**: Remote - which is convenient to restore a remote database.
  * Example use cases:
    * You have a dump on your local machine, and you want to restore a database only accessible from a specific network.
    * You have no access to the dumps, only an admin can restore them.

## Option 1: Locally

### With Docker

:::caution

[Docker](https://www.docker.com/) must be installed and running

:::

It's the best option to develop locally with a consistent transformed dump coming from your production data. Execute the following command to restore in a local Docker instance the latest dump:

```shell
replibyte -c conf.yaml dump restore local -d postgresql -v latest
```

`-d` parameter accepts `mongodb`, `mysql` and other databases supported by Replibyte.

You can also list the available dumps with:

```shell
replibyte -c conf.yaml dump list

type          name                  size    when                    compressed  encrypted
PostgreSQL    dump-1647706359405    154MB   Yesterday at 03:00 am   true        true
PostgreSQL    dump-1647731334517    152MB   2 days ago at 03:00 am  true        true
PostgreSQL    dump-1647734369306    149MB   3 days ago at 03:00 am  true        true
```

And restore the dump you want with:

```shell
replibyte -c conf.yaml dump restore local -d postgres -v dump-1647731334517
```

### In a file

You might want to inspect what you have in your dump, and restore it manually, you can execute the same restore command but with the `-o` parameter:

```shell
replibyte -c conf.yaml dump restore local -i postgres -v latest -o > dump.sql
```

## Option 2: Remote

To restore on a remote database, you need to specify the destination connection URI in your `conf.yaml`:

```yaml title="conf.yaml"
destination:
  connection_uri: postgres://user:password@host:port/db
  # Disable public's schema wipe
  # wipe_database: false (default: true)
```

and run the following command:

```shell
replibyte -c conf.yaml dump restore remote -v latest
```

---

You know now how to restore your transformed dump via multiple options, and even choose which version you want to restore. 

But now, **what happen if your database is very large?** In the next guide, you will learn how to downscale your database from a large size to a more reasonable one, while keeping it consistent. ➡️

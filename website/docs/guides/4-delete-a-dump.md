---
title: IV. Delete a dump
sidebar_position: 4
---

# Delete a dump

The `backup delete` command comes with 3 different deleting strategies.

1. Delete a backup by its name
2. Delete backups older than a specified number of days
3. Keep only a maximum number of backups

## Delete by backup name

```shell
replibyte -c prod-conf.yaml backup delete <BACKUP_NAME>
```

This is the simplest strategy you can find.

The list of available backups can be retrieved by running the following command:

```shell
replibyte -c prod-conf.yaml backup list

type          name                    size    when                    compressed  encrypted
PostgreSQL    backup-1647706359405    154MB   Yesterday at 03:00 am   true        true
PostgreSQL    backup-1647731334517    152MB   2 days ago at 03:00 am  true        true
PostgreSQL    backup-1647734369306    149MB   3 days ago at 03:00 am  true        true
```

## Delete backups older than 2 days

```shell
replibyte -c prod-conf.yaml backup delete --older-than=2d
```

Only the day unit is supported for now, other units could come in the future.

## Keep only the last `10` backups

```shell
replibyte -c prod-conf.yaml backup delete --keep-last=10
```

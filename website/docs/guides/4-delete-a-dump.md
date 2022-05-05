---
title: IV. Delete a dump
sidebar_position: 4
---

# Delete a dump

The `dump delete` command comes with 3 different deleting strategies.

1. Delete a dump by its name
2. Delete dumps older than a specified number of days
3. Keep only a maximum number of dumps

### Delete by dump name

```shell
replibyte -c conf.yaml dump delete <DUMP_NAME>
```

This is the simplest strategy you can find.

The list of available dumps can be retrieved by running the following command:

```shell
replibyte -c conf.yaml dump list

type          name                  size    when                    compressed  encrypted
PostgreSQL    dump-1647706359405    154MB   Yesterday at 03:00 am   true        true
PostgreSQL    dump-1647731334517    152MB   2 days ago at 03:00 am  true        true
PostgreSQL    dump-1647734369306    149MB   3 days ago at 03:00 am  true        true
```

### Delete dumps older than 2 days

```shell
replibyte -c conf.yaml dump delete --older-than=2d
```

Only the day unit is supported for now, other units could come in the future.

### Keep only the last 10 dumps

```shell
replibyte -c conf.yaml dump delete --keep-last=10
```

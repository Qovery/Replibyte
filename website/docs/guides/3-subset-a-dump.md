---
title: III. Subset a dump
sidebar_position: 3
---

# Subset a dump

:::caution

Only PostgreSQL supports *Subsetting* at the moment. Feel free to [contribute](/docs/contributing) to accelerate the support of MySQL and MongoDB 

:::

Subsetting is a powerful feature to only import a smaller consistent part from your production database. 

## How Subsetting works

Check out how subsetting works under the hood [here](/docs/design/how-database-subset-works).

## Configuration

Using Subsetting feature is as simple as adding new parameters in your `conf.yaml`

```yaml title="add database_subset object"
source:
  connection_uri: postgres://user:password@host:port/db
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
  database_subset:
    database: public
    table: customers
    strategy_name: random
    strategy_options:
      percent: 10
    passthrough_tables:
      - product_catalog
```

By applying this configuration, Replibyte will:

* Keep around 10% of the full database
* Go down the whole tables linked to `public.customers`
* Keep the whole rows from product_catalog

## Subset Strategy

TODO

## Considerations

This feature is still under active improvement. Feel free to [open an issue](https://github.com/Qovery/Replibyte/issues/new) if you face any trouble.

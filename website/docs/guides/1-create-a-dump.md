---
title: I. Create a dump
sidebar_position: 1
---

# Create a dump

:::note hello 👋🏼

I assume you have [Installed Replibyte](/docs/getting-started/installation) and you Read the [concepts](/docs/getting-started/concepts).

:::

## Configuration

To use Replibyte, you need to use to a dump from your production database. Here are three options:

* **Option 1 (easiest)**: You don't, and you want Replibyte to make a dump.
* **Option 2**: You don't, and you want to make a dump manually.
* **Option 3**: You already have a dump from your database.

### Option 1: make a dump with Replibyte

To let Replibyte creating a dump from your database for you, you need to update your `conf.yaml` file with the source connection URI from your production database as a property.

Pick the example that fit the database you are using.

<details>

<summary>PostgreSQL</summary>

```yaml
source:
  connection_uri: postgres://[user]:[password]@[host]:[port]/[database]
```

</details>

<details>

<summary>MySQL</summary>

```yaml
source:
  connection_uri: mysql://[user]:[password]@[host]:[port]/[database]
```

</details>

<details>

<summary>MongoDB</summary>

```yaml
source:
  connection_uri: mongo://[user]:[password]@[host]:[port]/[database]
```

</details>

or you can also use an environment variable

```yaml title="With an environment variable"
source:
  connection_uri: $DATABASE_URL
```

### Option 2: Make a dump manually

Here are the commands to dump your database yourself

<details>

<summary>PostgreSQL</summary>

```yaml
pg_dump --column-inserts --no-owner -h [host] -p [port] -U [username] [database]
```

</details>

<details>

<summary>MySQL</summary>

```yaml
mysqldump -h [host] -P [port] -u [username] -p --add-drop-database --add-drop-table --skip-extended-insert --complete-insert --single-transaction --quick --databases
```

</details>

<details>

<summary>MongoDB</summary>

```yaml
mongodump -h [host] --port [port] --authenticationDatabase [auth_db|default: admin] --db [database] -u [username] -p [password] --archive
```

</details>

### Option 3: You already have a dump

You have nothing to do, but it is possible that some options are missing from your dump, then you'll need to use the [option 2](#option-2-make-a-dump-manually)

## Hide sensitive data with Transformers

:::note

A MongoDB `Collection` can be associated to a SQL `table`, and a `Document` to a `table row`. 

:::

By using [Transformers](/docs/transformers), you can change on the fly your database data. Let's say we have the following structure for a table `employees`

```sql
CREATE TABLE public.customers (
    id bpchar NOT NULL,
    first_name character varying(30) NOT NULL,
    last_name character varying(30) NOT NULL,
    contact_email character varying(2048) NOT NULL,
    contact_phone character varying(24)
);
```

with the following entries:

```sql
INSERT INTO public.customers (id, first_name, last_name, contact_email, contact_phone) VALUES ('ALFKI', 'Maria', 'Anders', 'maria.anders@gmail.com', '030-0074321');
INSERT INTO public.customers (id, first_name, last_name, contact_email, contact_phone) VALUES ('ANATR', 'Ana', 'Trujillo', 'ana@factchecker.com', '(5) 555-4729');
INSERT INTO public.customers (id, first_name, last_name, contact_email, contact_phone) VALUES ('ANTON', 'Antonio', 'Moreno', 'anto.moreno@gmail.com', NULL);
```

and you want to hide the `first_name`, `last_name`, `contact_email` and the `contact_phone` fields. You can use the following configuration in your `conf.yaml` file.

```yaml title="source and transformers in your conf.yaml"
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
```

By using [Transformers](/docs/transformers), you keep your sensitive data safe of being leaked. 

## Run

It's the big day! Let's **run and upload** our transformed dump. But wait, something is missing. If you read about the [concepts](/docs/getting-started/concepts), and [how Replibyte works](/docs/how-replibyte-works), you know that a [Datastore](/docs/getting-started/concepts#datastore) is required to upload the transformed dump. Here is the lines you need to add in your `conf.yaml`

```yaml title="Add your datastore in your conf.yaml"
datastore:
  aws:
    bucket: my-replibyte-dumps
    region: us-east-2
    credentials:
      access_key_id: $ACCESS_KEY_ID
      secret_access_key: $AWS_SECRET_ACCESS_KEY
      session_token: XXX # optional
```

Here the datastore is a S3 bucket where the dump will be stored and accessible for future restore (next guide).

The final `conf.yaml` to create a final transformed dump looks like this:

:::caution

Do not forget to change your bucket name!

:::

```yaml title="conf.yaml"
source:
  connection_uri: postgres://user:password@host:port/db # optional - use only for option #1
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
    bucket: my-replibyte-dumps
    region: us-east-2
    credentials:
      access_key_id: $ACCESS_KEY_ID
      secret_access_key: $AWS_SECRET_ACCESS_KEY
      session_token: XXX # optional
```

:::note

Check out [all the Datastore available](/docs/datastores).

:::

Finally, you can run the following command according to you chosen option above:

<details>

<summary>Option 1: Make a dump with Replibyte</summary>

```shell
replibyte -c conf.yaml dump create
```

</details>

<details>

<summary>Option 2 and 3: Create a transformed dump from a dump file</summary>

```shell
cat your_dump.sql | replibyte -c conf.yaml dump create -i -s postgresql
```

`-i` parameter is required to read the data from the input.

`-s` parameter is required if you don't have a `source.connection_uri` in the configuration file. (Valid values are `postgresql`, `postgres`, `mysql`)


</details>

---
Now, it's time to look at how to restore your transformed dump ➡️

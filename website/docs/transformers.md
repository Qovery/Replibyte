---
sidebar_position: 7
---

# Transformers

A transformer is useful to change/hide the value of a specified column. Replibyte provides pre-made transformers. You can also [build your own Transformer in web assembly](/docs/transformers#custom-with-web-assembly-wasm).

:::note

Examples are with SQL input and output to reflect the change made by the transformer.

:::

To list the transformers available use

```shell
replibyte -c conf.yaml transformer list
 
 name            | description
-----------------+--------------------------------------------------------------------------------------------
 email           | Generate an email address (string only). [john.doe@company.com]->[tony.stark@avengers.com]
 first-name      | Generate a first name (string only). [Lucas]->[Georges]
 phone-number    | Generate a phone number (string only).
 random          | Randomize value but keep the same length (string only). [AAA]->[BBB]
 keep-first-char | Keep only the first character of the column.
 transient       | Does not modify the value.
 credit-card     | Generate a credit card number (string only).
 redacted        | Obfuscate your sensitive data (string only). [4242 4242 4242 4242]->[424****************]
 ...
```

## Random

Randomize value but keep the same length.

### Examples

```yaml
source:
  connection_uri: $DATABASE_URL
  transformers:
    - database: public
      table: my_table
      columns:
        - name: description
          transformer_name: random
# ...
```

SQL input:

```sql
INSERT INTO public.my_table (description) VALUE ('Hello World');
```

SQL output:

Random string value of the same length.

```sql
INSERT INTO public.my_table (description) VALUE ('Awdka Qdkqd');
```

## First name

Generate a fake first name.

### Examples

```yaml 
source:
  connection_uri: $DATABASE_URL
  transformers:
    - database: public
      table: my_table
      columns:
        - name: first_name
          transformer_name: first-name
# ...
```

SQL input:

```sql
INSERT INTO public.my_table (first_name) VALUE ('Lucas');
```

SQL output:

Fake name from a dictionary of names.

```sql
INSERT INTO public.my_table (first_name) VALUE ('Georges');
```


## Email

Replace the string value by a fake email address.

### Examples

```yaml
source:
  connection_uri: $DATABASE_URL
  transformers:
    - database: public
      table: my_table
      columns:
        - name: contact_email
          transformer_name: email
# ...
```

SQL input:

```sql
INSERT INTO public.my_table (contact_email) VALUE ('tony.stark@random.com');
```

SQL output:

Fake name from a dictionary of names.

```sql
INSERT INTO public.my_table (contact_email) VALUE ('toto@domain.tld');
```


## Keep first character

Keep only the first character of the column.

### Examples

```yaml
source:
  connection_uri: $DATABASE_URL
  transformers:
    - database: public
      table: my_table
      columns:
        - name: first_name
          transformer_name: keep-first-char
# ...
```

SQL input:

```sql
INSERT INTO public.my_table (first_name) VALUE ('Lucas');
```

SQL output:

```sql
INSERT INTO public.my_table (first_name) VALUE ('L');
```

## Phone number

Generate a phone number. (US only at the moment)

### Examples

```yaml
source:
  connection_uri: $DATABASE_URL
  transformers:
    - database: public
      table: my_table
      columns:
        - name: contact_phone
          transformer_name: phone-number
# ...
```

SQL input:

```sql
INSERT INTO public.my_table (contact_phone) VALUE ('+123456789');
```

SQL output:

```sql
INSERT INTO public.my_table (contact_phone) VALUE ('+356433821');
```

## Credit-card

Generate a credit card number

### Examples

```yaml
source:
  connection_uri: $DATABASE_URL
  transformers:
    - database: public
      table: my_table
      columns:
        - name: payment_card
          transformer_name: card-number
# ...
```

SQL input:

```sql
INSERT INTO public.my_table (payment_card) VALUE ('1234123412341234');
```

SQL output:

```sql
INSERT INTO public.my_table (payment_card) VALUE ('5678567856785678');
```

## Redacted

Obfuscate your sensitive data.

### Examples

```yaml
source:
  connection_uri: $DATABASE_URL
  transformers:
    - database: public
      table: my_table
      columns:
        - name: payment_card
          transformer_name: redacted
# ...
```

SQL input:

```sql
INSERT INTO public.my_table (payment_card) VALUE ('1234 1234 1234 1234');
```

SQL output:

```sql
INSERT INTO public.my_table (payment_card) VALUE ('1234***************');
```

Redacted transformer has more options, like the `width` and the `character` to use

```yaml
source:
  connection_uri: $DATABASE_URL
  transformers:
    - database: public
      table: employees
      columns:
        - name: last_name
          transformer_name: redacted
          transformer_options:
            character: '#'
            width: 20
# ...
```

SQL input:

```sql
INSERT INTO public.my_table (payment_card) VALUE ('1234 1234 1234 1234');
```

SQL output:

```sql
INSERT INTO public.my_table (payment_card) VALUE ('123####################');
```

## Transient

Does not change anything (good for testing purpose)

## Custom with Web Assembly (wasm)

Are you ready to get into the matrix? Take a look [here](/docs/advanced-guides/web-assembly-transformer) 👀

## Nested fields

:::note

Support MongoDB only.

:::

### Embedded sub-document (object)

For a document looking like this for which you want to transform the `email` and `phone_number` fields

```json
{
  "contact": {
      "email": "john.doe@example.com",
      "phone_number": "123456"
  }
}
```

The configuration file to use is:

```yaml
source:
  connection_uri: $DATABASE_URL
  transformers:
    - database: my_database
      table: my_collection
      columns:
        - name: contact.email
          transformer_name: email
        - name: contact.phone_number
          transformer_name: phone-number
```


### Embedded array of documents (array of objects)

For a document looking like this for which you want to transform the `email` and `phone_number` fields

```json
{
  "contacts": [
    {
      "email": "john.doe@example.com",
      "phone_number": "123456"
    },
    {
      "email": "jane.doe@example.com",
      "phone_number": "123456"
    }
  ]
}
```

The configuration file to use is:

```yaml
source:
  connection_uri: $DATABASE_URL
  transformers:
    - database: my_database
      table: my_collection
      columns:
        - name: contacts.$[].email
          transformer_name: email
        - name: contacts.$[].phone_number
          transformer_name: phone-number
```

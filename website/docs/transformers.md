---
sidebar_position: 6
---

# Transformers

A transformer is useful to change/hide the value of a specified column. Replibyte provides pre-made transformers. You can also [build your own Transformer in web assembly](#wasm).

:::note

Examples are with SQL input and output to reflect the change made by the transformer.

:::

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

## Transient

Does not change anything (good for testing purpose)

## Custom with Web Assembly (wasm)

Are you ready to get into the matrix? Take a look [here](/docs/advanced-guides/web-assembly-transformer) 👀

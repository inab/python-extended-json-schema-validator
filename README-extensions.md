# Implemented JSON Schema validation extensions

The extensions implemented are focused on features which involve more than one JSON document, like unique or primary key checks, as well as foreign key ones. Examples of these extensions are available at [test-data](test-data) folder.

* __Unique values check__: When the `unique` attribute is declared, the values assigned in that part of the schema on a set of JSON contents must be unique. The check includes all the loaded JSON contents. There are several examples inside [test-data](test-data). Its behaviour is the next:

  + If `unique` is a _`true`_ JSON value, the whole value in that position is used for the uniqueness check.
  
  + If `unique` is an array of strings, and the whole value is an object, those strings are the names of the keys whose values form the tuple to be validated.
  
  + If `unique` is an object:
  
    + It must have `members` key, which plays the role to describe keys to be included, as it is explained above with _`true`_ and array of strings.
    
    + It could have `name` key, which gives a labelling name.
    
    + It could have a `limit_scope` key, which is a boolean and can impose a limitation of the scope of this unique key.

* __Primary key values check__: When the `primary_key` attribute is declared, the values assigned in that part of the schema on a set of JSON contents must be unique, and can be referenced by _foreign keys_. The check includes all the loaded JSON contents. Its behaviour is similar to `unique` extension (there are several examples inside [test-data](test-data)):

  + If `primary_key` is a _`true`_ JSON value, the whole value in that position is used for the uniqueness check.
  
  + If `primary_key` is an array of strings, and the whole value is an object, those strings are the names of the keys whose values form the tuple to be validated.
  
  + If `primary_key` is an object:
  
    + It must have `members` key, which plays the role to describe keys to be included, as it is explained above with _`true`_ and array of strings.
    
    + It could have `name` key, which gives a referencing name. This is useful for complex documents where several parts define their own primary keys, so foreign keys scope can be narrowed to an specific primary key.
    
    + It could have a `limit_scope` key, which is a boolean and can impose a limitation of the scope of this unique key.
  
  + You can pre-populate the list of primary key values from an inline list embedded in the YAML configuration file. It should be something like:
    
```yaml
primary_key:
  inline_provider:
    'https://www.elixir-europe.org/excelerate/WP2/json-schemas/1.0/Dataset':
    - OEBD9990000001
    - OEBD9990000002
    - 'Custom:Community'
    'https://www.elixir-europe.org/excelerate/WP2/json-schemas/1.0/TestAction':
    - OEBA9990000001
    - OEBA9990000002
    - 'Custom:Action'
```

  + You can even pre-populate the list of primary key values from an external source just telling it in the YAML configuration file. It should have something like:
    
```yaml
primary_key:
  provider:
    - 'https://openebench.bsc.es/api/scientific/public/'
  allow_provider_duplicates: false
  schema_prefix: 'https://www.elixir-europe.org/excelerate/WP2/json-schemas/1.0/'
  accept: 'text/uri-list'
```
    
    to fetch keys in CSV format from several sources, using as request prefix the different providers, using the suffix of the schema IRI on the composition.
    
    If you want the keys retrieved from the providers to be used only for foreign key checks, then you have to set up the key `allow_provider_duplicates` to **`true`**. This option is also useful when you want to validate data to be updated in the server, the data is using foreign keys, but you don't want to receive duplicate primary key errors due the entries being validated.
    
  + And of course, a mix of the previous two styles!

* __Index values check__: When the `index` attribute is declared, the values assigned in that part of the schema on a set of JSON contents can be repeated, and can be referenced by _`join_keys`_. The check includes all the loaded JSON contents. Its behaviour is a very permissive version of `primary_key` extension (there are several examples inside [test-data](test-data)):

  + If `index` is a _`true`_ JSON value, the whole value in that position is recorded.
  
  + If `index` is an array of strings, and the whole value is an object, those strings are the names of the keys whose values form the tuple to be validated.
  
  + If `index` is an object:
  
    + It must have `members` key, which plays the role to describe keys to be included, as it is explained above with _`true`_ and array of strings.
    
    + It could have `name` key, which gives a referencing name. This is useful for complex documents where several parts define their own indexes, so join keys scope can be narrowed to an specific index.
    
    + It could have a `limit_scope` key, which is a boolean and can impose a limitation of the scope of this unique key.

* __Foreign key values check__: When the `foreign_keys` attribute is declared, parts of the values in that part of the schema must correlate to the values obtained from a primary key from JSON documents following other JSON Schema. As there can be more than one foreign key, `foreign_keys` expects an array of objects describing each foreign key relation. Those objects must have next keys:

  + `schema_id`: This optional key is the relative or absolute IRI of the JSON Schema describing the primary key. If it is not declared, it is the same as the document where it is declared this foreign key.
  
  + `members`: This is an array of strings. Those strings are the names of the keys whose values form the tuple to be validated against the gathered primary key values.
  
  + `refers_to`: This optional key is used to tell which is the named primary key to be focused on. This is useful for complex JSON Schemas where a schema has more than one primary key declaration.

* __Join key values check__: When the `join_keys` attribute is declared, parts of the values in that part of the schema must correlate to the values obtained from an index from JSON documents following other JSON Schema. As there can be more than one join key, `join_keys` expects an array of objects describing each join key relation. Those objects must have next keys:

  + `schema_id`: This optional key is the relative or absolute IRI of the JSON Schema describing the index key. If it is not declared, it is the same as the document where it is declared this join key.
  
  + `members`: This is an array of strings. Those strings are the names of the keys whose values form the tuple to be validated against the gathered primary key values.
  
  + `refers_to`: This optional key is used to tell which is the named primary key to be focused on. This is useful for complex JSON Schemas where a schema has more than one primary key declaration.



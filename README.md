# ESQL lookup functionality as an extension
This is an extension to [ESQL](https://github.com/vikashmadhow/esql) 
that creates the tables, macros and functions to work with lookups. A lookup is 
a table of code and labels such as a list of countries, currencies, cities, etc.
which can be, additionally, linked among them. For instance, a city may be linked
to the country that it's in and, in turn, the country can be linked to a continent
and any international groupings that it belongs to. A lookup value can have any
number of other values linking to it and similarly it may link to any number of 
other values, creating a directed graph.

## Using lookups in ESQL
All extensions are loaded into ESQL when initialising the database by passing the
extension class in the `database.extensions`. This parameter takes a collection
of extension classes which are loaded and initialised when the specific implementation 
of `ma.vi.esql.Database` is created.

For example: 

    Database db = new Postgresql(Map.of(
                      CONFIG_DB_NAME, "test",
                      CONFIG_DB_USER, "test",
                      CONFIG_DB_PASSWORD, "test",
                      CONFIG_DB_CREATE_CORE_TABLES, true,
                      CONFIG_DB_EXTENSIONS, Set.of(Lookups.class)));


## What is included in this extension
This extension creates 4 tables: `_platform.lookup.Lookup` which holds information 
on all defined lookups, `_platform.lookup.LookupLink` which contains information
on how lookups are linked, `_platform.lookup.LookupValue` which keeps all lookup
values (codes + labels) and `platform.lookup.LookupValueLink` which links lookup
values.

The following schema shows the tables and their relationships:
```
            target
       +----------------------------+
       v                            |
+-----------------+   source   +-----------------+
|     Lookup      |<-----------|   LookupLink    |
+-----------------+            +-----------------+
       ^
       | belongs to
+-----------------+            +-----------------+
|   LookupValue   |<-----------| LookupValueLink |
+-----------------+   source   +-----------------+
       ^                            |
       +----------------------------+
           target

```

The following shows how some example lookups above are stored in this lookup 
model (the values between brackets are there to clarify the links; they are not 
stored in the database):

```
  Lookup:
  
  | id | name      |
  |----|-----------|
  | 1  | Country   |
  | 2  | Currency  |
  | 3  | Continent |

  LookupValue: 
   
  |  id  |  lookup_id   | code  |  label                 |
  |------|--------------|-------|------------------------|                     
  |  1   |  1           | MU    |  Mauritius             |                         
  |  2   |  1           | US    |  United States         |                             
  |  3   |  1           | CN    |  China                 |                     
  |  4   |  2           | MUR   |  Mauritian Rupee       |                               
  |  5   |  2           | USD   |  United States Dollar  |                                     
  |  6   |  2           | RMB   |  Chinese Yuan          |                             
  |  7   |  3           | AF    |  Africa                |                       
  |  8   |  3           | NA    |  North America         |                             
  |  9   |  3           | AS    |  Asia                  |                     

  LookupValueLink containing links between countries, currencies and continents:

  | Link name  | Source value id | Target value id |       
  |------------|-----------------|-----------------|       
  | cty_to_cur | 1 (MU)          | 4 (MUR)         |     
  | cty_to_cur | 2 (US)          | 5 (USD)         |     
  | cty_to_cur | 3 (CN)          | 6 (RMB)         |     
  | cty_to_con | 1 (MU)	         | 7 (AF)          |   
  | cty_to_con | 2 (US)          | 8 (NA)          |   
  | cty_to_con | 3 (CN)          | 9 (AS)          |   

```

## Functions
This extension also adds three functions to find labels for lookup values from
their codes (including the abilitiy to follow links) and to construct labels 
from tables linked by some keys.

### lookuplabel:
This is a macro which produces a label corresponding to a lookup code. It can 
be used as follows: `lookuplabel(code, X)` will get the label corresponding to
code from a lookup table named `X`.

A variable number of named links can be supplied to find linked valued. 
E.g. `lookuplabel(code, X, Y, Z)` will find the code in lookup `X`, follow its 
link to `Y` and then `Z` and return the label for the latter.

`lookuplabel` takes the following optional named arguments to control how the 
label is produced:
* **show_code**: whether to show the code in the label or not. Default is true.
* **show_text**: whether to show the label text in the label or not. Default is true.
* **code_separator**: an expression for the separator between the code and text. Default is ' - '
* **show_last_only**: Show the last label element in the chain only (a -> b -> c, show c only). Default is true.
* **label_separator**: an expression for the separator between the labels from different lookups. Default is '/'.
* **last_to_first**: Shows the names from the link tables from the last linked table to the first, if true, or otherwise, from the first to the last. Default is true.
* **match_by**: the code column in the LookupValue to match the value to; can be 'code', 'alt_code1' or 'alt_code2'. Default is 'code'.

### lookuplabelf:
`lookuplabel` is a macro that is expanded into a sub-select which works in all
cases on Postgresql but might cause certain issues when used in the group by
clause in SQL Server (although ESQL compilation generates special SQL on SQL
Server to take care of such cases). 

If `lookuplabel` is not working, `lookuplabelf` is similar in function to 
`lookuplabel` which is implemented as a stored function and should work in all
cases. However, being implemented as a function comes with certain limitations:
1) only up to 5 links are supported on SQL Server (as the latter does not support
   variadic arguments) and;
2) named parameters are not supported in `lookuplabelf`

### joinlabel:

A macro function which produces a label corresponding to a sequence of ids from 
linked tables. To get the label corresponding to an id referring to another table. 
For instance, if table `A {b_id}` refers to `B{id, name}` then `joinlabel(b_id, 'id', 'name', 'B')`
will return the name from B corresponding to b_id. `joinlabel(b_id, 'id', 'name', 'B', 'c_id', 'id', 'name', 'C')`
will produce `c_name / b_name` corresponding to `b_id` and following on to `c_id`. 
Any number of links can be specified. `joinlabel` can have the following optional
named arguments to control the value displayed:
* **show_last_only**: Show the last label element in the chain only (a -> b -> c, 
  show c only). Default is false.
* **label_separator**: an expression for the separator between the labels from 
  different tables. Default is '/'.
* **last_to_first**: Shows the names from the link tables from the last linked 
  table to the first, if true, or otherwise, from the first to the last. Default is true.

## Examples
The following query:
```
  select name, country_code, 
         country:lookuplabel(country_code, 'Country')
         currency:lookuplabel(country_code, 'Country', 'Currency', show_last_only:=false, label_separator:=', ')
    from com.example.Customer;
```
could produce something like this:
```
  |      name     | country_code |     country    |               currency                |
  | ------------- | -------------|----------------|---------------------------------------|
  | Vikash Madhow | MU           | MU - Mauritius | MU - Mauritius, MUR - Mauritian Rupee |
  | Avish Madhow  | EN           | EN - England   | EN - England, GBP - Pound Sterling    |
```
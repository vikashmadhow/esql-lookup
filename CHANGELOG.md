# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Planned
- Virtual tables allowing lookups with links to be queried and updated as a 
  single table.

## [0.9.1] - 2023-11-20
### Added
- Method `saveLookupImport` in `LookupExtension` has been made public so that
  it may be invoked by other libraries to manually create an import for a
  lookup. This is needed when the lookup is created manually instead of through
  the `LookupInitializer`.

## [0.9.0] - 2023-11-19
### Added
- Automatic creation of an ETL import to populate and update the contents of
  a lookup.

## [0.8.1] - 2023-10-19
### Fixed
- Fixed loading of lookups where the lookup does not exist; the lookups cache
  would be erroneously set to an empty lookup (to prevent infinite loops during
  load) if the lookup does not exist; the cache should not have any entry in 
  such cases.

## [0.8.0] - 2023-09-04
### Added
- Join label excludes null codes or labels.
- Join label removes duplicates from link table.

## [0.7.10] - 2023-08-24
### Added
- Support for incremental initialisation of lookups.

## [0.7.9] - 2023-08-21
### Added
- `show_code` in `joinlabel` allows joined labels to be searched by codes.

## [0.7.8] - 2023-07-19
### Fixed
- Corrected `lookup(name)` method which was causing a `Recursive update` exception when
  loading linked lookups.

## [0.7.7] - 2023-07-17
### Added
- Lazily load lookups through the new `lookup(name)` method instead of loading and
  caching all lookups on first access. This should improve startup performance.

## [0.7.6] - 2023-03-17
### Added
- Only qualify column references that are not already qualified.

## [0.7.5] - 2023-01-09
### Added
- `LookupInitializer` trims linked lookup values before searching.

## [0.7.4] - 2022-12-28
### Added
- `keywords` string parameter in `joinlabel` and `lookuplabel` (applies only when 
  the code being searched is null, which is used to load the whole table of values)
  to limit loaded labels to those matching the specified keywords.

## [0.7.3] - 2022-12-27
### Added
- `labels_offset` and `labels_limit` controls how many labels are to be loaded
  by `joinlabel` and `lookuplabel` (applies only when the code being searched 
  is null, which is used to load the whole table of values). This can be used to
  implement lazy loading as the user scrolls through the list of values.

## [0.7.2] - 2022-10-03
### Added
- `lookuplabel` can now be supplied with an array (or a column of type array) and
  return the labels (or linked labels) for all codes in the array.
- Trigram based `classify` ESQL function matching source text to lookup labels
  and returning closest code according to similarity.

## [0.7.1] - 2022-09-23
### Added
- Performance optimizations:
  - Separate indexes on `name`, `source_value_id` and `target_value_id` columns 
    of `LookupValueLink` table.
  - Separate indexes on `code` and `lookup_id` and `target_value_id` columns of 
    `LookupValue` table.
  
### Changed
- Tests included for searching by a linked code. This now works after the fix
  in ESQL 1.0.15 which translates select expressions in binary operators correctly.

## [0.7.0] - 2022-09-14
### Changed
- Lookup tables are now created in only schema named `_lookup` instead of a custom
  schema provided as configuration parameter to the extension. The custom schema
  required additional code in all functions and macros to properly refer to the
  dynamic schema for very little gain in flexibility.
- Simplified lookup links using the name and display name of the target lookup
  in the link, instead of a separate unique link name. This reduces the different
  names that need to be maintained in a lookup graph.

## [0.6.1] - 2022-09-12
### Added
- Methods `loadLookups` in `LookupExtension` returns a list of all loaded lookups.
- `display_name` in now required for lookups.

## [0.6.0] - 2022-09-07
### Added
- Methods added to `LookupExtension` to load lookups, links and values. 
- All lookups are loaded and cached on first access. Cache is invalidated when
  a lookup is created or updated. Changes outside this extension is not detected;
  this will be implemented when the notification framework is completed and will
  then automatically invalidate entries irrespective of where the change to the
  lookups was initiated.
- Methods to create and update lookups and lookup values added to LookupExtension.
- `LookupInitializer` to create and update lookup from hierarchical representation,
  such as in a YAML file.

## [0.5.3] - 2022-08-12
### Added
- `display_name` added to `Lookup`.
- `group` column added to `Lookup` table used to assign a lookup to a specific 
  for organisation purposes. 

## [0.5.2] - 2022-08-08
### Added
- `matching` named argument to `lookuplabel` when `code` is null (loading whole
  lookup) to restrict lookup values to load. 
- `matching` named argument to `joinlabel` when `code` is null (loading all 
  code-label pairs) to restrict values to load. 
- Change to work with the changed ESQL `StringLiteral` which not require 
  surrounding single-quotes when creating. 

## [0.5.1] - 2022-07-26
### Added
- `alt_code1` and `alt_code2` are not also returned by `lookuplabel` when the 
  code supplied is explicitly null. 

### Changed
- `show_code` in `lookuplabel` now defaults to false, consistent with`joinlabel` 
  and more aligned to user expectation. 

## [0.5.0] - 2022-07-18
- `joinlabel` returns the list of codes and labels in the target table (after
  following links) when the source is supplied is explicitly null. The labels in
  the returned table applies all parameters supplied to the function.
- Alternate codes for matching is now a string parameter instead of column reference.
  This is more intuitive and can be more robust in the case of a wrong matching
  code being provided.

## [0.4.1] - 2022-07-06
### Added
- `lookuplabel` and `joinlabel` are no longer expanded in `Define` statements to 
  preserve the macro form in the resulting attributes of the defined table (or 
  struct). This defers the macro expansion to query execution time which provides 
  additional context information useful for the expansion. For instance, `lookuplabel`
  expands to a `select` expression which is valid when the query is executed on 
  the server but would not work directly in all cases if executed on the client-side;
  there a different method for obtaining the label (e.g. a hash table lookup) 
  might be more convenient or the only option.
- `lookuplabel` and `joinlabel` are similarly not expanded in uncomputed expressions
  for the same reasons. 

## [0.4.0] - 2022-06-21
### Added
- Column of `select` expression generated by `lookuplabel` is named `label` now.
  Previously it was using the default name (`column1` usually).
- Column of `select` expression generated by `joinlabel` is named `label` now.
  Previously it was using the default name (`column1` usually).

## [0.3.6] - 2022-05-25
### Changed
- Comply with ESQL version 0.9.7 (syntax change to `create table` where comma is 
  not allowed after table metadata, metadata at start of statement only, etc.).
- Additional information added as `description` to lookup values.
- `lookuplabel` macro changed to have an option controlling whether to include the
  description (default is false) in the label produced.
- `show_text` parameter has been renamed to `show_label` in `lookuplabel` macro.

## [0.3.5] - 2022-05-17
### Changed
- Comply with ESQL version 0.9.6.

## [0.3.4] - 2022-05-05
### Changed
- Comply with ESQL version 0.9.4.

## [0.3.3] - 2022-03-17
### Changed
- Comply with ESQL version 0.9.1.

## [0.3.2] - 2022-03-05
### Changed
- All class elements documented.
- Gradle build file normalised.
- Migrated to new ESQL version (0.8.9).

## [0.3.1] - 2022-02-25
### Tests
- All visual tests changed to assertions.

### Changed
- Include changes to the base Configuration object (method param changed to get).

### Fixed
- Test tables changed to remove non-literals from table metadata, as required by
  latest ESQL implementation.

## [0.3.0] - 2022-02-11
### Added
- The schema where the lookup tables are created can be configured on initialisation.
  Default is "_lookup".
- All lookup SQL Server functions are put in the (configured) lookup schema 
  instead of "_core".

### Changed
- Change name of extension class from `Lookups` to `LookupExtension`.

## [0.2.6] - 2022-02-09
### Changed
- Incorporate changes made to the 0.8.0+ line of ESQL. 

## [0.2.5] - 2022-01-14
### Changed
- Changed to work with ESQL version 0.7.0+. 

## [0.2.4] - 2021-12-25
### Changed
- Query generated by expansion of lookuplabel and joinlabel uses human-friendly
  names for the table aliases.

## [0.2.3] - 2021-12-19
### Refactored
- Refactored to include changes in the ESQL base library.

## [0.2.2] - 2021-04-13
### Added
- Join label tests passed.
- Extension documented.

## [0.2.1] - 2021-04-12
### Added
- LookupLabel, JoinLabel macros rewritten.
- LookupLabelFunction renamed to lookuplabelf.
- Column names in Lookup tables shortened.
- Lookup label tests passed.
- Option to match by code, alt_code1 and alt_code2 added to lookuplabel macro.

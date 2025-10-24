# AGENTS.md

This file provides guidance to LLM-based agents when working with files in this repository.

## Security Lakehouse

- Project that allows to ingest security data in different formats, decode and normalize them.
- The `bronze` data layer corresponds to raw data, with minimum of modifications.  Very often, data in the `bronze` layer will be stored using the [Variant data type](https://docs.databricks.com/aws/en/sql/language-manual/data-types/variant-type) and will be extracted using [try_variant_get](https://docs.databricks.com/aws/en/sql/language-manual/functions/try_variant_get) expressions
- The `silver` layer corresponds to decoded data, keeping their structure close to the original data structure.
- The `gold` layer corresponds to fully-normalized data mapped into one of the tables.
- The normalization is performed according to the simplified version of [OCSF](https://schema.ocsf.io/).
- Modified schema is available in the `docs/gold_tables.md` file.

## Presets development

- Preset is a template that is used by the Security Lakehouse UI to configure new data sources.
- Preset is a YAML file with a specific structure, described in the `docs/preset-structure-and-dev.md` file and defined by the JSON schema in `schema/preset.schema.yaml`.
- Existing presets are stored in the `presets` directory.  They are stored in files `presets/<source>/<source_type>/preset.yaml`.  Where `<source>` is typically a name of the company producing a product or service, and `<source_type>` is a specific product or log type.

## Creating presets for a new data source

- When developing a new data source it's important to understand the structure of the data.  This could be done by analyzing the data that may be available in the `samples` folder where they are stored in the `samples/<source>/<source_type>/` directory.
- In some case we may have a documentation available that describes format of the data. In this case documentation will be stored in the `docs/<source>/<source_type>/` directory.
- When defining the gold layer, we need to put data only in tables that make sense. I.e., Zeek conn data will be mapped into `network_activity` table, and Zeek DNS data will be mapped into `dns_activity` table.  Some log types, i.e. Cloudtrail, Crowdstrike, etc. may contain multiple event types, so they need to be mapped into multiple tables.
- More information about preset development is available in the [official documentation](https://docs.sl.antimatter.io/preset-development/notebook-preset-development-tool)

**Created By: [Kristin Dahl](mailto:kristin.dahl@databricks.com)**

## 1\) What is a Preset (in practice)

A Preset is a **declarative ETL** recipe: where to read raw logs, how to reshape/enrich them (Bronze→Silver), and how to produce Gold tables that conform to OCSF for downstream analytics. In DASL, a **Datasource** is the runnable job instance that optionally points at a Preset and fills in environment-specific bits (locations, schedule, etc.) and can override parts of the recipe if needed. [docs.sl.antimatter.io+1](https://docs.sl.antimatter.io/presets/overview)

Key properties:

* Presets are **reusable** and **declarative**; they’re designed to be authored/maintained as code, then referenced by many Datasources. [docs.sl.antimatter.io](https://docs.sl.antimatter.io/preset-development/overview)

* They expect raw data typically in a **Unity Catalog Volume / External location** and commonly use **Databricks Autoloader** for ingest. [docs.sl.antimatter.io](https://docs.sl.antimatter.io/preset-development/overview)

* Gold outputs align with **OCSF** schemas. [docs.sl.antimatter.io+1](https://docs.sl.antimatter.io/presets/overview)

## 2\) Where Presets live: repo & file layout

The public content-pack repo documents the layout and JSON-Schema references:

* Each preset lives at:  
   `presets/{source}/{sourceType}/preset.yaml`  
   (e.g., AWS Security Lake Route53 → `presets/aws_sec_lake/route53/preset.yaml`)

* Every preset **must** also have a `version.yaml` in the same folder.  
* All presets **must** be listed in `presets/index.yaml`.  
* The schemas for these files live under `schema/`:  
  * `schema/preset.schema.yaml`  
  * `schema/version.schema.yaml`  
  * `schema/index.schema.yaml` [GitHub](https://github.com/antimatterhq/dasl-content-packs/tree/dasl-v1.0)

## 2a) Where Presets live: custom presets and how to deploy them

## 3\) The `preset.yaml` reference (syntax & semantics)

### 3.1 Top-level keys

```textproto
name: <machine_name>              # required, unique within repo (e.g., aws_cloudtrail_iam)
author: <string>                  # free text
description: <string>             # short human description
title: <string>                   # display title for UI
iconURL: <string>                 # optional icon used in UI
autoloader:                       # how to read raw files (→Bronze input)
bronze: 				    # (optional) how to modify Bronze data after they read
silver:                           # list(s) describing Bronze→Silver steps
gold:                             # list of Gold outputs (OCSF-aligned)
```

### **`autoloader`**

The configuration for reading raw files. Expect **Databricks Autoloader** semantics.

```textproto
autoloader:
  format: json | csv | ...        # file format for cloudFiles
  cloudFiles:
    schemaHints: |                # Spark schema hints (DDL string) for parsing
      <Spark DDL defining columns>
    # other common cloudFiles params (e.g., includeExistingFiles) may be supported depending on implementation
```

**Tip:** Put nested arrays/structs in `schemaHints` so you can cleanly `explode` or `from_json` later in Silver.

### **`bronze`**

Tunes the read data before writing it into the bronze table. In many cases it will be used to extract the `time` column that is required.

```
bronze:
  loadAsSingleVariant: true   # if source data should be put into a Variant column with the name `data`
  preTransform:               # list of lists describing transformations. Dataframe resulting from the current 
                              # list is passed to the next one in the loop
    -                         # list of strings that are passed to df.selectExpr
      - "*"
      - cast(ts as timestamp) as time
```

### **`silver`**

Two phases are typical:

1. **preTransform**: row-expansion and raw extraction to an intermediate view.  
2. **transform**: logical subsets with optional filtering, temporary parsing, and the final Silver column selections.

```textproto
silver:
  preTransform:
    - name: <step_name>
      fields:
        - name: <col_name>
          expr: <spark_sql_expr>  # OR 'from' to copy an existing col
          # type/nullable/etc. are not typically required; infer from expr
      postFilter: <spark_sql_predicate_string> # optional row filter applied after fields
  transform:
    - name: <transform_name>
      filter: <spark_sql_predicate_string>     # optional WHERE, e.g. restrict to a subset of events
      utils:                                   # optional helpers
        unreferencedColumns: { keep: false }   # pattern: drop or keep extras
        temporaryFields:                       # ephemeral parsing fields you don't keep
          - name: <tmp_col>
            expr: <spark_sql_expr>             # e.g., from_json(...)
      fields:                                  # final Silver columns exposed downstream
        - name: <col_name>
          from: <existing_col>
          # or:
          expr: <spark_sql_expr>
        - name: nested.field.path              # dot-paths create nested structs in output
          from: some.source.path
```

**Common patterns:**

* `preTransform.fields` can **explode** arrays (e.g., `explode_outer(Records)` as `raw`).  
* `postFilter` trims to only relevant records (e.g., `raw.eventSource = 'iam.amazonaws.com'`).  
* `transform.filter` gates subsets by event name lists (e.g., building distinct logical Silver “sub-tables” from one raw stream).  
* `utils.temporaryFields` often wraps **`from_json`** to materialize complex nested objects for easy selection later.

### **`gold`**

Each Gold output is typically an OCSF event class mapped from a specific Silver transform.

```textproto
gold:
  - name: <gold_view_name>         # e.g., account_change
    input: <silver_transform_name> # e.g., aws_cloudtrail_account_change
    fields:
      - name: <ocsf.column.path>
        from: <silver_col_path>
      - name: <ocsf.column.path>
        expr: <spark_sql_expr>     # CASE...WHEN..., COALESCE(...), etc.
```

Notes:

* **`name`** values in **Gold** are logical Gold “views” or outputs.  
* **`input`** must match a **Silver transform**’s `name`.  
* **Nested names** like `actor.user.account.uid` create nested structs aligned with OCSF.  
* **Expressions** may compute **categoricals/IDs** (`action`, `action_id`, `activity_id`, etc.) from raw fields (e.g., checking `errorCode` or `eventName`); these are often essential for OCSF semantics.

### Field item keys (quick grammar)

* `name` (required): output column path (dot-notation for nested)  
* exactly one of:  
  * `from`: copy an existing column by name/path  
  * `expr`: a **Spark SQL expression** (can reference any already-defined column)  
  * `literal`: a constant literal  
* Optional: `type`, `nullable` (generally inferred)

## 4\) Versioning & Index

* Every preset folder must contain a **`version.yaml`** with a monotonically increasing integer and a short “changes” note pointing to the commit that added/updated the preset. See `schema/version.schema.yaml` for the required shape.  
    
* Add each preset to **`presets/index.yaml`** (a flat list of `{source, sourceType}` entries); shape validated by `schema/index.schema.yaml`. This lets the PresetStore discover it.

## 5\) Minimal working example (`preset.yaml`)

Below is a compact, realistic template you can drop into `presets/<source>/<type>/preset.yaml`. It mirrors the style of the CloudTrail-IAM example, but trimmed to essentials:

```textproto
name: aws_cloudtrail_iam
author: Databricks / DASL Team
title: "AWS - CloudTrail (IAM)"
description: "Transforms CloudTrail IAM events into OCSF gold outputs."
iconURL: "https://raw.githubusercontent.com/antimatterhq/dasl-content-packs/refs/heads/main/presets/aws/cloudtrail_iam/icon.png"

autoloader:
  format: json
  cloudFiles:
    schemaHints: |
      Records ARRAY<STRUCT<
        eventTime: STRING,
        eventSource: STRING,
        eventName: STRING,
        eventCategory: STRING,
        userIdentity: STRING,
        awsRegion: STRING,
        sourceIPAddress: STRING,
        requestParameters: STRING,
        responseElements: STRING,
        errorCode: STRING,
        additionalEventData: STRING
      >>

silver:
  preTransform:
    - name: aws_cloudtrail
      fields:
        - name: raw
          expr: explode_outer(Records)
      postFilter: "raw.eventSource = 'iam.amazonaws.com'"

  transform:
    - name: aws_cloudtrail_account_change
      filter: |
        raw.eventName IN (
          'CreateUser','DeleteUser','UpdateUser','AttachUserPolicy','DetachUserPolicy'
        )
      utils:
        temporaryFields:
          - name: userIdentity
            expr: "from_json(raw.userIdentity, 'STRUCT<type: STRING, userName: STRING, accountId: STRING>')"
      fields:
        - name: time
          from: raw.eventTime
        - name: eventName
          from: raw.eventName
        - name: awsRegion
          from: raw.awsRegion
        - name: src_ip
          from: raw.sourceIPAddress
        - name: userIdentity
          from: userIdentity             # from temporaryFields
        - name: errorCode
          from: raw.errorCode

gold:
  - name: account_change
    input: aws_cloudtrail_account_change
    fields:
      - name: time
        from: time
      - name: cloud.region
        from: awsRegion
      - name: actor.user.name
        from: userIdentity.userName
      - name: actor.user.account.uid
        from: userIdentity.accountId
      - name: src.endpoint.ip
        from: src_ip
      - name: activity_name
        expr: "CASE WHEN eventName IS NOT NULL THEN eventName ELSE 'Unknown' END"
      - name: action
        expr: "CASE WHEN errorCode IS NULL THEN 'Allowed' ELSE 'Denied' END"
      - name: action_id
        expr: "CASE WHEN errorCode IS NULL THEN 1 ELSE 2 END"
```

**This illustrates:**

* **Autoloader** schema hints → **explode** array of CloudTrail records  
* **Filtering** to IAM service  
* **Temporary parsing** via `from_json`  
* **Gold mapping** with nested OCSF paths and simple CASE logic

## 6\) Authoring workflow & testing

A sane, repeatable loop:

1. **Scaffold**  
   * Create directories: `presets/<source>/<sourceType>/`  
   * Start a `preset.yaml` from the template above.  
   * Add a small **sample file** to a dev bucket and register it in UC so you can iterate quickly.

2. **Notebook tools**  
   The docs highlight notebook tooling for previewing/iterating on transforms (develop locally, inspect frames, validate field coverage) before registering a Datasource. (See “Notebook Tools” under Preset Development.) [docs.sl.antimatter.io](https://docs.sl.antimatter.io/preset-development/notebook-preset-development-tool) 

3. **Validate against schema**  
   * Use the JSON Schemas in the repo to validate your YAML locally.

4. **Register & run as a Datasource**  
   Create a **Datasource** in DASL that points to your preset and supplies env-specific knobs (paths, schedule). Datasources are implemented as Databricks Jobs and may override preset bits if needed.

5. **Gold verification**  
   Confirm Gold conforms to the intended **event class**: nested paths, enums/IDs (`action_id`, `activity_id`), and datatypes align with your chosen table structure

6. **Version & index**  
   After merge, create/update `version.yaml` and add the preset entry to `presets/index.yaml` so it shows up in the PresetStore.

## 7\) Best practices & common pitfalls

**Time fields & partitioning**

* Be explicit about **time semantics** early in Silver:  
  * `event_time` (producer timestamp),  
  * `ingest_time` (landed/processed),  
  * `detection_time` (analysis).  
    Use a single canonical `time` for Gold if your class expects it, but **preserve raw timestamps** for forensics and reprocessing.

**Explode early, parse once**

* Use `schemaHints` \+ `explode_outer` in preTransform to flatten arrays (e.g., `Records`) and then **`from_json`** to parse stringified JSON columns into structs. Keep these as **temporaryFields** so you can drop them later.

**Filters are your friend**

* Use **`postFilter`** (preTransform) for coarse service selection (e.g., only IAM), and **`transform.filter`** to carve out event subsets into separate Silver transforms that map neatly to Gold classes.

**Build categorical logic in one place**

* Compute things like `action`, `action_id`, `activity_id` at **Gold** so they’re uniform across sources. These are often CASE expressions driven by `eventName`, `errorCode`, etc

**Nested naming \= nested structs**

* Dot-paths in `name` (e.g., `actor.user.account.uid`) build nested structs automatically (important if you want to match OCSF output trees).

**Minimize surprises**

* Prefer `from` over `expr` when feasible (it’s easier to audit and refactor)  
* Normalize **IP**, **region**, **principal** fields to consistent spellings/paths.  
* Document source-specific quirks (e.g., pagination, missing fields, known `errorCode` families) in a `README.md` next to the preset.

**Version like you mean it\!**

* Increment `version.yaml` with a short, human-readable **“changes”** line; record the commit hash that introduced the change. Keep `presets/index.yaml` in sync.


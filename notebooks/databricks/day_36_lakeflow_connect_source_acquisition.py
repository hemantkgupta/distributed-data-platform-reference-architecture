# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Day 36 - Lakeflow Connect Source Acquisition
# MAGIC
# MAGIC **Phase:** Days 26-40 ingestion and loading.
# MAGIC
# MAGIC **Associate mapping:** ingestion/loading, Lakeflow Jobs, troubleshooting/monitoring, and governance/security.
# MAGIC
# MAGIC **Professional extension:** managed CDC connector selection, query-based connector tradeoffs, gateway/staging controls, credential governance, fallback design, and source-retention incident prevention.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Lab Part 1 - Create source acquisition contracts
# MAGIC
# MAGIC **Purpose:** Stage source systems with enough metadata to choose Lakeflow Connect, query-based ingestion, a standard connector, Auto Loader, or a custom pipeline.
# MAGIC
# MAGIC **Expected result:** Ten Day 36 source contracts are created with expected acquisition patterns.
# MAGIC
# MAGIC **Operational meaning:** Connector choice starts with a source contract: change pattern, latency, retention, credentials, PII class, connector availability, and recovery boundary.

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS de_learning;
# MAGIC USE SCHEMA de_learning;
# MAGIC
# MAGIC DROP VIEW IF EXISTS lakeflow_connect_final_checks_day36;
# MAGIC DROP VIEW IF EXISTS lakeflow_connect_incident_view_day36;
# MAGIC DROP TABLE IF EXISTS lakeflow_connect_release_gate_day36;
# MAGIC DROP TABLE IF EXISTS lakeflow_connect_job_trigger_plan_day36;
# MAGIC DROP TABLE IF EXISTS lakeflow_connect_fallback_plan_day36;
# MAGIC DROP TABLE IF EXISTS lakeflow_connect_monitoring_day36;
# MAGIC DROP TABLE IF EXISTS lakeflow_acquisition_contract_day36;
# MAGIC DROP TABLE IF EXISTS lakeflow_connect_decisions_day36;
# MAGIC DROP TABLE IF EXISTS lakeflow_source_contracts_day36;
# MAGIC
# MAGIC CREATE TABLE lakeflow_source_contracts_day36 (
# MAGIC   source_id STRING,
# MAGIC   source_name STRING,
# MAGIC   source_category STRING,
# MAGIC   source_system_type STRING,
# MAGIC   change_pattern STRING,
# MAGIC   latency_sla_minutes INT,
# MAGIC   source_retention_hours INT,
# MAGIC   volume_tier STRING,
# MAGIC   cdc_required BOOLEAN,
# MAGIC   cdc_enabled BOOLEAN,
# MAGIC   cursor_column_available BOOLEAN,
# MAGIC   primary_key_available BOOLEAN,
# MAGIC   managed_connector_available BOOLEAN,
# MAGIC   governance_approved BOOLEAN,
# MAGIC   pii_class STRING,
# MAGIC   network_path STRING,
# MAGIC   expected_pattern STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC INSERT INTO lakeflow_source_contracts_day36 VALUES
# MAGIC   ('src_salesforce_accounts', 'Salesforce Accounts', 'enterprise_app', 'Salesforce', 'incremental_application_objects', 240, 168, 'medium', false, false, true, true, true, true, 'business_contact', 'public_api_with_oauth', 'MANAGED_APP_CONNECTOR'),
# MAGIC   ('src_postgres_orders', 'PostgreSQL Orders', 'database', 'PostgreSQL', 'insert_update_delete_cdc', 15, 24, 'high', true, true, true, true, true, true, 'customer_order', 'private_vpc_peering', 'MANAGED_DATABASE_CDC'),
# MAGIC   ('src_sqlserver_legacy', 'SQL Server Legacy ERP', 'database', 'SQL Server', 'insert_update_delete_cdc', 60, 48, 'medium', true, false, true, true, true, true, 'financial', 'private_link', 'PREPARE_SOURCE_FOR_MANAGED_CDC'),
# MAGIC   ('src_mysql_inventory', 'MySQL Inventory', 'database', 'MySQL', 'append_and_update_latest_state', 120, 72, 'medium', false, false, true, true, true, true, 'operational', 'private_vpc_peering', 'QUERY_BASED_CONNECTOR'),
# MAGIC   ('src_customer_api', 'Customer Success API', 'unsupported_api', 'REST API', 'paginated_incremental_api', 360, 168, 'low', false, false, true, true, false, true, 'customer_contact', 'public_api_with_secret', 'CUSTOM_CONNECTOR'),
# MAGIC   ('src_kafka_clickstream', 'Clickstream Kafka', 'message_bus', 'Apache Kafka', 'continuous_events', 5, 24, 'very_high', true, true, false, false, false, true, 'behavioral', 'private_vpc_peering', 'STANDARD_CONNECTOR_OR_STRUCTURED_STREAMING'),
# MAGIC   ('src_s3_events', 'Partner Event Files', 'cloud_storage', 'S3 JSON', 'new_files', 60, 168, 'high', false, false, false, false, false, true, 'partner_event', 'external_location', 'STANDARD_CONNECTOR_OR_AUTO_LOADER'),
# MAGIC   ('src_sharepoint_docs', 'SharePoint Contract Files', 'file_source', 'SharePoint', 'new_files', 1440, 720, 'low', false, false, true, false, true, true, 'confidential_contract', 'oauth_enterprise_app', 'MANAGED_FILE_SOURCE_CONNECTOR'),
# MAGIC   ('src_oracle_payments', 'Oracle Payments', 'database', 'Oracle', 'insert_update_delete_cdc', 30, 12, 'high', true, true, true, true, true, false, 'restricted_payment_pii', 'direct_connect', 'HOLD_GOVERNANCE_APPROVAL'),
# MAGIC   ('src_spreadsheets_finance', 'Finance Spreadsheet Drop', 'manual_file', 'local spreadsheets', 'manual_overwrite', 1440, 0, 'low', false, false, false, false, false, false, 'financial_forecast', 'manual_upload', 'HOLD_SOURCE_CONTRACT_REQUIRED');
# MAGIC
# MAGIC SELECT
# MAGIC   source_category,
# MAGIC   expected_pattern,
# MAGIC   count(*) AS sources
# MAGIC FROM lakeflow_source_contracts_day36
# MAGIC GROUP BY source_category, expected_pattern
# MAGIC ORDER BY source_category, expected_pattern;

# COMMAND ----------
# MAGIC %md
# MAGIC ## Lab Part 2 - Review connector fit with SQL
# MAGIC
# MAGIC **Purpose:** Summarize whether each source is a managed connector fit, a query-based fit, a standard connector or Auto Loader fit, or a hold condition.
# MAGIC
# MAGIC **Expected result:** Ten sources are grouped by connector family and operational reason.
# MAGIC
# MAGIC **Operational meaning:** Managed connectors reduce custom extraction work, but they still require source prep, credentials, networking, and governance approval.

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT
# MAGIC   source_id,
# MAGIC   source_system_type,
# MAGIC   CASE
# MAGIC     WHEN governance_approved = false THEN 'hold'
# MAGIC     WHEN managed_connector_available = true AND source_category IN ('enterprise_app', 'file_source') THEN 'managed_connector'
# MAGIC     WHEN managed_connector_available = true AND source_category = 'database' AND cdc_required = true THEN 'managed_database_connector'
# MAGIC     WHEN source_category = 'database' AND cursor_column_available = true AND cdc_required = false THEN 'query_based_connector'
# MAGIC     WHEN source_category IN ('cloud_storage', 'message_bus') THEN 'standard_connector_or_custom_pipeline'
# MAGIC     WHEN source_category = 'unsupported_api' THEN 'custom_connector'
# MAGIC     ELSE 'source_contract_hold'
# MAGIC   END AS connector_family,
# MAGIC   CASE
# MAGIC     WHEN governance_approved = false THEN 'Approval, classification, masking, and access controls are incomplete.'
# MAGIC     WHEN cdc_required = true AND cdc_enabled = false THEN 'Source must enable CDC, change tracking, binlogs, or equivalent before managed CDC.'
# MAGIC     WHEN source_category = 'database' AND cdc_required = false THEN 'Cursor column and high-water mark can drive scheduled query-based ingestion.'
# MAGIC     WHEN source_category = 'cloud_storage' THEN 'Object storage ingestion needs checkpoint, schema location, and file-discovery controls.'
# MAGIC     WHEN source_category = 'message_bus' THEN 'Message ingestion needs offset/checkpoint and consumer ownership controls.'
# MAGIC     ELSE 'Connector fit is mostly blocked by target, credential, and monitoring setup.'
# MAGIC   END AS operational_reason
# MAGIC FROM lakeflow_source_contracts_day36
# MAGIC ORDER BY source_id;

# COMMAND ----------
# MAGIC %md
# MAGIC ## Lab Part 3 - Score acquisition decisions with PySpark
# MAGIC
# MAGIC **Purpose:** Use PySpark to turn source contracts into recommended Lakeflow Connect or fallback acquisition patterns.
# MAGIC
# MAGIC **Expected result:** `lakeflow_connect_decisions_day36` has ten rows, all matching the expected pattern.
# MAGIC
# MAGIC **Operational meaning:** Production ingestion intake can be made repeatable: a source contract becomes an explicit connector decision plus required control evidence.

# COMMAND ----------
from pyspark.sql import functions as F

spark.sql("USE SCHEMA de_learning")

sources_df = spark.table("lakeflow_source_contracts_day36")

decisions_df = (
    sources_df
    .withColumn(
        "recommended_pattern",
        F.when(F.col("source_category") == "manual_file", F.lit("HOLD_SOURCE_CONTRACT_REQUIRED"))
        .when(F.col("governance_approved") == F.lit(False), F.lit("HOLD_GOVERNANCE_APPROVAL"))
        .when(
            (F.col("source_category") == "enterprise_app") & F.col("managed_connector_available"),
            F.lit("MANAGED_APP_CONNECTOR"),
        )
        .when(
            (F.col("source_category") == "file_source") & F.col("managed_connector_available"),
            F.lit("MANAGED_FILE_SOURCE_CONNECTOR"),
        )
        .when(
            (F.col("source_category") == "database")
            & F.col("managed_connector_available")
            & F.col("cdc_required")
            & F.col("cdc_enabled"),
            F.lit("MANAGED_DATABASE_CDC"),
        )
        .when(
            (F.col("source_category") == "database")
            & F.col("managed_connector_available")
            & F.col("cdc_required")
            & (F.col("cdc_enabled") == F.lit(False)),
            F.lit("PREPARE_SOURCE_FOR_MANAGED_CDC"),
        )
        .when(
            (F.col("source_category") == "database")
            & (F.col("cdc_required") == F.lit(False))
            & F.col("cursor_column_available"),
            F.lit("QUERY_BASED_CONNECTOR"),
        )
        .when(F.col("source_category") == "unsupported_api", F.lit("CUSTOM_CONNECTOR"))
        .when(F.col("source_category") == "message_bus", F.lit("STANDARD_CONNECTOR_OR_STRUCTURED_STREAMING"))
        .when(F.col("source_category") == "cloud_storage", F.lit("STANDARD_CONNECTOR_OR_AUTO_LOADER"))
        .otherwise(F.lit("HOLD_SOURCE_CONTRACT_REQUIRED"))
    )
    .withColumn(
        "required_control",
        F.when(F.col("recommended_pattern") == "MANAGED_DATABASE_CDC", F.lit("Unity Catalog connection, gateway sizing, staging volume, source retention, and destination streaming table."))
        .when(F.col("recommended_pattern") == "PREPARE_SOURCE_FOR_MANAGED_CDC", F.lit("Enable source CDC or change tracking, grant source privileges, and verify retention before deployment."))
        .when(F.col("recommended_pattern") == "QUERY_BASED_CONNECTOR", F.lit("Cursor column, high-water mark semantics, Lakehouse Federation connection, and schedule."))
        .when(F.col("recommended_pattern") == "MANAGED_APP_CONNECTOR", F.lit("OAuth connection, object selection, target catalog/schema, and connector run monitoring."))
        .when(F.col("recommended_pattern") == "MANAGED_FILE_SOURCE_CONNECTOR", F.lit("Enterprise app connection, file scope, target volume or table, and access review."))
        .when(F.col("recommended_pattern") == "STANDARD_CONNECTOR_OR_AUTO_LOADER", F.lit("External location, checkpoint, schema location, file event mode, and rescue policy."))
        .when(F.col("recommended_pattern") == "STANDARD_CONNECTOR_OR_STRUCTURED_STREAMING", F.lit("Consumer group or offsets, checkpoint, schema contract, and replay boundary."))
        .when(F.col("recommended_pattern") == "CUSTOM_CONNECTOR", F.lit("Connector package, auth handling, pagination, rate limits, tests, and deployment bundle."))
        .when(F.col("recommended_pattern") == "HOLD_GOVERNANCE_APPROVAL", F.lit("PII classification, masking, row filters, grants, and audit approval before ingest."))
        .otherwise(F.lit("Complete the source contract before selecting an ingestion pattern."))
    )
    .withColumn(
        "decision_status",
        F.when(F.col("recommended_pattern") == F.col("expected_pattern"), F.lit("MATCH")).otherwise(F.lit("REVISIT"))
    )
    .withColumn(
        "operational_risk",
        F.when(F.col("recommended_pattern").startswith("HOLD"), F.lit("blocked"))
        .when(F.col("recommended_pattern").contains("PREPARE"), F.lit("source_preparation"))
        .when(F.col("recommended_pattern").contains("CUSTOM"), F.lit("high_custom_ownership"))
        .when(F.col("source_retention_hours") <= 24, F.lit("retention_sensitive"))
        .otherwise(F.lit("standard"))
    )
    .select(
        "source_id",
        "source_name",
        "source_category",
        "source_system_type",
        "expected_pattern",
        "recommended_pattern",
        "decision_status",
        "operational_risk",
        "required_control",
    )
)

(
    decisions_df.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("lakeflow_connect_decisions_day36")
)

display(decisions_df.orderBy("source_id"))

# COMMAND ----------
# MAGIC %md
# MAGIC ### PySpark Notes
# MAGIC
# MAGIC - `sources_df` represents one source acquisition contract per row.
# MAGIC - SQL equivalent: `SELECT ..., CASE WHEN governance_approved = false THEN ... END AS recommended_pattern FROM lakeflow_source_contracts_day36`.
# MAGIC - `F.col("source_category")` and `F.col("cdc_required")` refer to distributed columns, not local Python values.
# MAGIC - `withColumn` builds derived decision columns without changing `sources_df`.
# MAGIC - `F.when(...).otherwise(...)` is PySpark's `CASE WHEN`; combine conditions with `&` and wrap each condition in parentheses.
# MAGIC - The DataFrame is lazy until `.write.saveAsTable(...)` or `display(...)` runs.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Lab Part 4 - Build acquisition contracts from the decision table
# MAGIC
# MAGIC **Purpose:** Convert connector decisions into landing tables, state boundaries, delete semantics, and credential boundaries.
# MAGIC
# MAGIC **Expected result:** Ten acquisition contract rows are created.
# MAGIC
# MAGIC **Operational meaning:** A connector decision is not deployable until it names target objects, state ownership, delete behavior, and credential scope.

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE TABLE lakeflow_acquisition_contract_day36
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC   d.source_id,
# MAGIC   d.recommended_pattern,
# MAGIC   concat('de_learning.', replace(d.source_id, 'src_', ''), '_bronze_day36') AS target_table,
# MAGIC   CASE
# MAGIC     WHEN d.recommended_pattern = 'MANAGED_DATABASE_CDC' THEN 'Unity Catalog connection plus ingestion gateway plus staging volume plus streaming table.'
# MAGIC     WHEN d.recommended_pattern = 'QUERY_BASED_CONNECTOR' THEN 'Unity Catalog connection plus cursor high-water mark stored by connector.'
# MAGIC     WHEN d.recommended_pattern = 'MANAGED_APP_CONNECTOR' THEN 'Managed connector object state plus target streaming table.'
# MAGIC     WHEN d.recommended_pattern = 'MANAGED_FILE_SOURCE_CONNECTOR' THEN 'Managed file-source connector state plus selected enterprise file scope.'
# MAGIC     WHEN d.recommended_pattern = 'STANDARD_CONNECTOR_OR_AUTO_LOADER' THEN 'Auto Loader checkpoint, schema location, and file discovery metadata.'
# MAGIC     WHEN d.recommended_pattern = 'STANDARD_CONNECTOR_OR_STRUCTURED_STREAMING' THEN 'Streaming checkpoint and source offset or consumer-group ownership.'
# MAGIC     WHEN d.recommended_pattern = 'CUSTOM_CONNECTOR' THEN 'Custom connector checkpoint, pagination cursor, and retry ledger.'
# MAGIC     ELSE 'Deployment blocked until source contract is complete.'
# MAGIC   END AS state_boundary,
# MAGIC   CASE
# MAGIC     WHEN d.recommended_pattern = 'MANAGED_DATABASE_CDC' THEN 'Apply inserts, updates, and deletes from source change logs.'
# MAGIC     WHEN d.recommended_pattern = 'QUERY_BASED_CONNECTOR' THEN 'Captures latest changed row state only; intermediate row states between runs are not preserved.'
# MAGIC     WHEN d.recommended_pattern IN ('STANDARD_CONNECTOR_OR_AUTO_LOADER', 'MANAGED_FILE_SOURCE_CONNECTOR') THEN 'File deletion is not business deletion unless producer sends a tombstone.'
# MAGIC     WHEN d.recommended_pattern = 'STANDARD_CONNECTOR_OR_STRUCTURED_STREAMING' THEN 'Deletion semantics depend on event contract and tombstone records.'
# MAGIC     ELSE 'Define delete semantics before publication.'
# MAGIC   END AS delete_semantics,
# MAGIC   CASE
# MAGIC     WHEN d.recommended_pattern LIKE 'MANAGED%' OR d.recommended_pattern = 'QUERY_BASED_CONNECTOR' THEN 'Use Unity Catalog connection and least-privilege service principal.'
# MAGIC     WHEN d.recommended_pattern LIKE 'STANDARD%' THEN 'Use Unity Catalog external location or connection with scoped privileges.'
# MAGIC     WHEN d.recommended_pattern = 'CUSTOM_CONNECTOR' THEN 'Use secrets-backed credentials plus bundle-managed deployment config.'
# MAGIC     ELSE 'No credential should be provisioned until the hold is resolved.'
# MAGIC   END AS credential_boundary,
# MAGIC   d.required_control
# MAGIC FROM lakeflow_connect_decisions_day36 d;
# MAGIC
# MAGIC SELECT recommended_pattern, count(*) AS contract_rows
# MAGIC FROM lakeflow_acquisition_contract_day36
# MAGIC GROUP BY recommended_pattern
# MAGIC ORDER BY recommended_pattern;

# COMMAND ----------
# MAGIC %md
# MAGIC ## Lab Part 5 - Simulate monitoring and incident evidence
# MAGIC
# MAGIC **Purpose:** Create Lakeflow Connect monitoring rows and classify connector health incidents.
# MAGIC
# MAGIC **Expected result:** Ten monitoring rows and an incident view show healthy, blocked, gateway, retention, credential, and quarantine states.
# MAGIC
# MAGIC **Operational meaning:** Managed ingestion still needs operations: gateway health, source retention, credential expiry, failed records, quarantines, and run status.

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE TABLE lakeflow_connect_monitoring_day36 (
# MAGIC   source_id STRING,
# MAGIC   connector_status STRING,
# MAGIC   gateway_status STRING,
# MAGIC   staging_volume_status STRING,
# MAGIC   cdc_lag_minutes INT,
# MAGIC   source_retention_hours INT,
# MAGIC   credential_expiry_days INT,
# MAGIC   failed_records BIGINT,
# MAGIC   quarantine_records BIGINT,
# MAGIC   latest_run_state STRING,
# MAGIC   observed_at TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC INSERT INTO lakeflow_connect_monitoring_day36 VALUES
# MAGIC   ('src_salesforce_accounts', 'RUNNING', 'not_required', 'not_required', 10, 168, 80, 0, 0, 'SUCCEEDED', current_timestamp()),
# MAGIC   ('src_postgres_orders', 'RUNNING', 'healthy', 'healthy', 20, 24, 45, 0, 0, 'SUCCEEDED', current_timestamp()),
# MAGIC   ('src_sqlserver_legacy', 'BLOCKED_SOURCE_PREP', 'not_deployed', 'not_deployed', 0, 48, 60, 0, 0, 'BLOCKED', current_timestamp()),
# MAGIC   ('src_mysql_inventory', 'SCHEDULED', 'not_required', 'not_required', 0, 72, 35, 0, 0, 'SUCCEEDED', current_timestamp()),
# MAGIC   ('src_customer_api', 'CUSTOM_BUILD', 'not_required', 'not_required', 0, 168, 12, 14, 4, 'FAILED', current_timestamp()),
# MAGIC   ('src_kafka_clickstream', 'RUNNING', 'not_required', 'not_required', 4, 24, 90, 0, 0, 'SUCCEEDED', current_timestamp()),
# MAGIC   ('src_s3_events', 'RUNNING', 'not_required', 'not_required', 90, 168, 120, 1, 2, 'SUCCEEDED_WITH_WARNINGS', current_timestamp()),
# MAGIC   ('src_sharepoint_docs', 'RUNNING', 'not_required', 'not_required', 0, 720, 6, 0, 0, 'SUCCEEDED', current_timestamp()),
# MAGIC   ('src_oracle_payments', 'BLOCKED_GOVERNANCE', 'not_deployed', 'not_deployed', 0, 12, 60, 0, 0, 'BLOCKED', current_timestamp()),
# MAGIC   ('src_spreadsheets_finance', 'BLOCKED_CONTRACT', 'not_deployed', 'not_deployed', 0, 0, 0, 0, 0, 'BLOCKED', current_timestamp());
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW lakeflow_connect_incident_view_day36 AS
# MAGIC SELECT
# MAGIC   m.source_id,
# MAGIC   d.recommended_pattern,
# MAGIC   m.connector_status,
# MAGIC   m.gateway_status,
# MAGIC   m.latest_run_state,
# MAGIC   CASE
# MAGIC     WHEN d.recommended_pattern LIKE 'HOLD%' THEN 'DEPLOYMENT_HOLD'
# MAGIC     WHEN m.credential_expiry_days BETWEEN 1 AND 7 THEN 'CREDENTIAL_ROTATION_REQUIRED'
# MAGIC     WHEN m.gateway_status = 'down' THEN 'GATEWAY_REPAIR_REQUIRED'
# MAGIC     WHEN m.source_retention_hours > 0 AND m.cdc_lag_minutes > m.source_retention_hours * 60 * 0.75 THEN 'SOURCE_RETENTION_RISK'
# MAGIC     WHEN m.quarantine_records > 0 THEN 'QUARANTINE_REVIEW'
# MAGIC     WHEN m.latest_run_state NOT IN ('SUCCEEDED', 'SUCCEEDED_WITH_WARNINGS') THEN 'RUN_REPAIR_REQUIRED'
# MAGIC     ELSE 'HEALTHY'
# MAGIC   END AS incident_status,
# MAGIC   CASE
# MAGIC     WHEN d.recommended_pattern LIKE 'HOLD%' THEN 'Resolve source contract or governance hold before deploy.'
# MAGIC     WHEN m.credential_expiry_days BETWEEN 1 AND 7 THEN 'Rotate Unity Catalog connection or secret-backed credential.'
# MAGIC     WHEN m.gateway_status = 'down' THEN 'Repair ingestion gateway network, compute, or source access.'
# MAGIC     WHEN m.source_retention_hours > 0 AND m.cdc_lag_minutes > m.source_retention_hours * 60 * 0.75 THEN 'Scale gateway or pipeline before source change logs expire.'
# MAGIC     WHEN m.quarantine_records > 0 THEN 'Review quarantine records and publish only approved rows.'
# MAGIC     WHEN m.latest_run_state NOT IN ('SUCCEEDED', 'SUCCEEDED_WITH_WARNINGS') THEN 'Repair connector run and rerun from tracked state.'
# MAGIC     ELSE 'No action beyond normal monitoring.'
# MAGIC   END AS first_response
# MAGIC FROM lakeflow_connect_monitoring_day36 m
# MAGIC JOIN lakeflow_connect_decisions_day36 d
# MAGIC   ON m.source_id = d.source_id;
# MAGIC
# MAGIC SELECT incident_status, count(*) AS sources
# MAGIC FROM lakeflow_connect_incident_view_day36
# MAGIC GROUP BY incident_status
# MAGIC ORDER BY incident_status;

# COMMAND ----------
# MAGIC %md
# MAGIC ## Lab Part 6 - Create fallback, trigger, and release-gate plans
# MAGIC
# MAGIC **Purpose:** Document fallback ingestion paths, job trigger choices, and release gates for each source.
# MAGIC
# MAGIC **Expected result:** Ten fallback rows, ten trigger rows, and ten release-gate rows are created.
# MAGIC
# MAGIC **Operational meaning:** Connector deployments need rollback and scheduling decisions, especially when source prep, governance, credentials, or custom connector work is incomplete.

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE TABLE lakeflow_connect_fallback_plan_day36
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC   d.source_id,
# MAGIC   d.recommended_pattern AS primary_plan,
# MAGIC   CASE
# MAGIC     WHEN d.recommended_pattern = 'MANAGED_DATABASE_CDC' THEN 'Temporary bounded COPY INTO snapshot only if CDC outage is approved and source keys reconcile.'
# MAGIC     WHEN d.recommended_pattern = 'PREPARE_SOURCE_FOR_MANAGED_CDC' THEN 'Use query-based ingestion only if a reliable cursor column exists and intermediate states are not required.'
# MAGIC     WHEN d.recommended_pattern = 'QUERY_BASED_CONNECTOR' THEN 'Use managed CDC when deletes or intermediate row states become required.'
# MAGIC     WHEN d.recommended_pattern = 'MANAGED_APP_CONNECTOR' THEN 'Custom connector only if object support, rate limits, or governance cannot be satisfied by managed connector.'
# MAGIC     WHEN d.recommended_pattern = 'MANAGED_FILE_SOURCE_CONNECTOR' THEN 'Auto Loader from governed cloud landing zone after enterprise file export.'
# MAGIC     WHEN d.recommended_pattern = 'STANDARD_CONNECTOR_OR_AUTO_LOADER' THEN 'COPY INTO for bounded backfill; Auto Loader for ongoing incremental files.'
# MAGIC     WHEN d.recommended_pattern = 'STANDARD_CONNECTOR_OR_STRUCTURED_STREAMING' THEN 'Custom Structured Streaming source with explicit offsets and checkpoint.'
# MAGIC     WHEN d.recommended_pattern = 'CUSTOM_CONNECTOR' THEN 'Manual controlled batch export until connector tests and bundle deployment pass.'
# MAGIC     ELSE 'No fallback deploys until contract, governance, or source ownership hold is closed.'
# MAGIC   END AS fallback_plan,
# MAGIC   CASE
# MAGIC     WHEN d.recommended_pattern LIKE 'HOLD%' THEN 'No rollback target because deployment is blocked.'
# MAGIC     ELSE 'Keep raw bronze/state evidence and compare target counts plus primary keys before cutover.'
# MAGIC   END AS rollback_evidence
# MAGIC FROM lakeflow_connect_decisions_day36 d;
# MAGIC
# MAGIC CREATE TABLE lakeflow_connect_job_trigger_plan_day36
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC   d.source_id,
# MAGIC   d.recommended_pattern,
# MAGIC   CASE
# MAGIC     WHEN d.recommended_pattern = 'MANAGED_DATABASE_CDC' THEN 'connector_schedule'
# MAGIC     WHEN d.recommended_pattern = 'QUERY_BASED_CONNECTOR' THEN 'scheduled_pipeline'
# MAGIC     WHEN d.recommended_pattern IN ('STANDARD_CONNECTOR_OR_AUTO_LOADER', 'MANAGED_FILE_SOURCE_CONNECTOR') THEN 'file_arrival_trigger_or_available_now'
# MAGIC     WHEN d.recommended_pattern = 'STANDARD_CONNECTOR_OR_STRUCTURED_STREAMING' THEN 'continuous_or_triggered_stream'
# MAGIC     WHEN d.recommended_pattern = 'CUSTOM_CONNECTOR' THEN 'bundle_deployed_job_schedule'
# MAGIC     ELSE 'blocked'
# MAGIC   END AS trigger_type,
# MAGIC   CASE
# MAGIC     WHEN d.recommended_pattern IN ('STANDARD_CONNECTOR_OR_AUTO_LOADER', 'MANAGED_FILE_SOURCE_CONNECTOR') THEN 'Use debounce to wait for complete batches and cap frequency to reduce idle compute.'
# MAGIC     WHEN d.recommended_pattern = 'QUERY_BASED_CONNECTOR' THEN 'Run on schedule aligned to cursor freshness and source load window.'
# MAGIC     WHEN d.recommended_pattern = 'MANAGED_DATABASE_CDC' THEN 'Gateway captures continuously; ingestion pipeline schedule controls destination freshness.'
# MAGIC     ELSE 'Trigger design depends on hold resolution or custom connector tests.'
# MAGIC   END AS trigger_reason
# MAGIC FROM lakeflow_connect_decisions_day36 d;
# MAGIC
# MAGIC CREATE TABLE lakeflow_connect_release_gate_day36
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC   i.source_id,
# MAGIC   i.recommended_pattern,
# MAGIC   i.incident_status,
# MAGIC   CASE
# MAGIC     WHEN i.incident_status = 'HEALTHY' THEN 'DEPLOY_OR_KEEP_RUNNING'
# MAGIC     WHEN i.incident_status = 'QUARANTINE_REVIEW' THEN 'DEPLOY_WITH_DATA_QUALITY_HOLD'
# MAGIC     WHEN i.incident_status = 'CREDENTIAL_ROTATION_REQUIRED' THEN 'HOLD_FOR_CREDENTIAL_ROTATION'
# MAGIC     WHEN i.incident_status = 'DEPLOYMENT_HOLD' THEN 'HOLD'
# MAGIC     ELSE 'REPAIR_BEFORE_DEPLOY'
# MAGIC   END AS release_action,
# MAGIC   i.first_response
# MAGIC FROM lakeflow_connect_incident_view_day36 i;
# MAGIC
# MAGIC SELECT release_action, count(*) AS sources
# MAGIC FROM lakeflow_connect_release_gate_day36
# MAGIC GROUP BY release_action
# MAGIC ORDER BY release_action;

# COMMAND ----------
# MAGIC %md
# MAGIC ## Lab Part 7 - Run final readiness checks
# MAGIC
# MAGIC **Purpose:** Verify the Day 36 source contracts, decisions, monitoring, fallback plans, triggers, and release gates.
# MAGIC
# MAGIC **Expected result:** The final check view returns only `PASS` rows.
# MAGIC
# MAGIC **Operational meaning:** The lab leaves a full source-acquisition checklist that can be reused before deploying a connector.

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW lakeflow_connect_final_checks_day36 AS
# MAGIC SELECT
# MAGIC   'source_contract_rows' AS check_name,
# MAGIC   count(*) AS observed_value,
# MAGIC   10 AS expected_value,
# MAGIC   CASE WHEN count(*) = 10 THEN 'PASS' ELSE 'FAIL' END AS status
# MAGIC FROM lakeflow_source_contracts_day36
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'decision_rows',
# MAGIC   count(*),
# MAGIC   10,
# MAGIC   CASE WHEN count(*) = 10 THEN 'PASS' ELSE 'FAIL' END
# MAGIC FROM lakeflow_connect_decisions_day36
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'matched_decisions',
# MAGIC   count(*),
# MAGIC   10,
# MAGIC   CASE WHEN count(*) = 10 THEN 'PASS' ELSE 'FAIL' END
# MAGIC FROM lakeflow_connect_decisions_day36
# MAGIC WHERE decision_status = 'MATCH'
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'acquisition_contract_rows',
# MAGIC   count(*),
# MAGIC   10,
# MAGIC   CASE WHEN count(*) = 10 THEN 'PASS' ELSE 'FAIL' END
# MAGIC FROM lakeflow_acquisition_contract_day36
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'monitoring_rows',
# MAGIC   count(*),
# MAGIC   10,
# MAGIC   CASE WHEN count(*) = 10 THEN 'PASS' ELSE 'FAIL' END
# MAGIC FROM lakeflow_connect_monitoring_day36
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'incident_rows',
# MAGIC   count(*),
# MAGIC   10,
# MAGIC   CASE WHEN count(*) = 10 THEN 'PASS' ELSE 'FAIL' END
# MAGIC FROM lakeflow_connect_incident_view_day36
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'fallback_rows',
# MAGIC   count(*),
# MAGIC   10,
# MAGIC   CASE WHEN count(*) = 10 THEN 'PASS' ELSE 'FAIL' END
# MAGIC FROM lakeflow_connect_fallback_plan_day36
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'trigger_rows',
# MAGIC   count(*),
# MAGIC   10,
# MAGIC   CASE WHEN count(*) = 10 THEN 'PASS' ELSE 'FAIL' END
# MAGIC FROM lakeflow_connect_job_trigger_plan_day36
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'release_gate_rows',
# MAGIC   count(*),
# MAGIC   10,
# MAGIC   CASE WHEN count(*) = 10 THEN 'PASS' ELSE 'FAIL' END
# MAGIC FROM lakeflow_connect_release_gate_day36;
# MAGIC
# MAGIC SELECT *
# MAGIC FROM lakeflow_connect_final_checks_day36
# MAGIC ORDER BY check_name;

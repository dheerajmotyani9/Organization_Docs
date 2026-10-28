CREATE OR REPLACE PROCEDURE END_EDW_PRD.CONTROL.PROC_EXPORT_SUPPLIERS()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'pandas')
HANDLER = 'main'
EXECUTE AS OWNER
AS
$$
import csv
import os
import re
from datetime import datetime, timedelta

from snowflake.snowpark import functions as F
from snowflake.snowpark.types import TimestampTimeZone, TimestampType

# ---------------------------------------------------------------------
# 3.1 Constants
# Conversion:
#   Fabric  : unqualified Lakehouse names (vendtable, lakehouse_table_name, ...)
#   Snowpark: fully qualified END_EDW_PRD objects - D365 sources in BRONZE,
#             export stage / tracking table / temp table in CONTROL;
#             EXPORT_COLS keeps the same column order as the Fabric suppliers_df.select(...)
# ---------------------------------------------------------------------
DB = "END_EDW_PRD"
SCH = "BRONZE"        # D365 source and seed tables
CTL_SCH = "CONTROL"   # export stage, record tracking, temp tables
STAGE = f"@{DB}.{CTL_SCH}.STG_BONDED_WAREHOUSE_EXPORT"
TRACKING_TABLE = f"{DB}.{CTL_SCH}.BONDEDWAREHOUSERECORDTRACKING_SUPPLIERS"

EXPORT_COLS = [
    "COMPANY_CODE", "SUPPLIER_CODE", "SUPPLIER_NAME", "COUNTRY_CONSIGNED",
    "STREET", "CITY", "POST_CODE", "COUNTRY", "C109_REF", "C109_DATE",
    "TRADE_TERMS", "IDENTIFICATION_NUMBER",
]

# ---------------------------------------------------------------------
# 3.2 Extract query
# Truncation lengths, N/A -> NA, then non-ASCII / punctuation strip on the 4 free-text columns.
# Raw f-string so regex backslashes reach Snowflake unchanged. One bind (?) = look-back date.
# Conversion:
#   Fabric  : spark.sql(f"... >= '{filterdate}'") then three DataFrame passes:
#             select(substring(...)), replace("N/A","NA"), withColumn(regexp_replace)
#   Snowpark: one SQL statement with CTEs (src -> truncated -> na_fixed -> final);
#             filterdate passed as a bind parameter instead of string formatting
#   Spark SQL -> Snowflake SQL changes:
#             IFNULL(x, y)      -> COALESCE(x, y)
#             DATE(col)         -> col::DATE
#             GREATEST(...)     -> GREATEST_IGNORE_NULLS(...) (Spark skips NULLs)
#             substring(c,1,n)  -> SUBSTR(c, 1, n)
#             '' cast to Date   -> CAST(NULL AS DATE)
#             date_format(dd-MM-yyyy) -> TO_VARCHAR(.., 'DD-MM-YYYY')
#   Added   : ISDELETE = FALSE filters (soft deletes in the Snowflake copies)
# ---------------------------------------------------------------------
SUPPLIERS_SQL = rf"""
-- src: original Fabric query; soft-deleted (ISDELETE) rows excluded
WITH src AS (
    SELECT
        'ENDCTG'                                    AS company_code
      , v.accountnum                                AS supplier_code
      , d.name                                      AS supplier_name
      , COALESCE(cl.isocode2, '')                   AS country_consigned
      , COALESCE(pa.street,  'N/A')                 AS street
      , COALESCE(pa.city,    'N/A')                 AS city
      , COALESCE(pa.zipcode, 'N/A')                 AS post_code
      , cr.isocode                                  AS country
      , ''                                          AS c109_ref
      , CAST(NULL AS DATE)                          AS c109_date
      , tl.langdoncode                              AS trade_terms
      , ''                                          AS identification_number
    FROM {DB}.{SCH}.D365__VENDTABLE v
    INNER JOIN {DB}.{SCH}.D365__DIRPARTYTABLE d
        ON v.party = d.recid
       AND COALESCE(d.isdelete, FALSE) = FALSE
    LEFT JOIN {DB}.{SCH}.D365__LOGISTICSPOSTALADDRESS pa
        ON d.primaryaddresslocation = pa.location
       AND COALESCE(pa.isdelete, FALSE) = FALSE
    LEFT JOIN {DB}.{SCH}.D365__LOGISTICSADDRESSCOUNTRYREGION cr
        ON pa.countryregionid = cr.countryregionid
       AND COALESCE(cr.isdelete, FALSE) = FALSE
    LEFT JOIN {DB}.{SCH}.SEED_D365__TRANDETERMSLOOKUP tl
        ON v.dlvterm = tl.deliveryterms
    LEFT JOIN {DB}.{SCH}.SEED_D365__COUNTRYLOOKUP cl
        ON v.hslsuppliercountryregionid = cl.isocode3
    WHERE v.vendgroup IN ('Stock', 'Proforma', 'SDStock', 'Interco')
      AND v.dataareaid IN ('end.', 'END.')
      AND COALESCE(v.isdelete, FALSE) = FALSE
      AND pa.validfrom::DATE <= CURRENT_DATE
      AND pa.validto::DATE   >  CURRENT_DATE
      -- Spark GREATEST ignores NULLs; Snowflake GREATEST does not
      AND GREATEST_IGNORE_NULLS(v.modifieddatetime, d.modifieddatetime, pa.modifieddatetime) >= ?::DATE
),
-- truncated: max field lengths required by the bonded warehouse file spec
truncated AS (
    SELECT
        SUBSTR(company_code::VARCHAR,                 1, 10)         AS company_code_t
      , SUBSTR(supplier_code::VARCHAR,                1, 12)         AS supplier_code_t
      , SUBSTR(supplier_name::VARCHAR,                1, 35)         AS supplier_name_t
      , SUBSTR(country_consigned::VARCHAR,            1, 3)          AS country_consigned_t
      , SUBSTR(street::VARCHAR,                       1, 35)         AS street_t
      , SUBSTR(city::VARCHAR,                         1, 35)         AS city_t
      , SUBSTR(post_code::VARCHAR,                    1, 9)          AS post_code_t
      , SUBSTR(country::VARCHAR,                      1, 3)          AS country_t
      , c109_ref::VARCHAR                                            AS c109_ref_t
      , TO_VARCHAR(c109_date, 'DD-MM-YYYY')                          AS c109_date_t
      , SUBSTR(trade_terms::VARCHAR,                  1, 17)         AS trade_terms_t
      , identification_number::VARCHAR                               AS identification_number_t
    FROM src
),
na_fixed AS (
    -- DataFrame.replace("N/A", "NA") on every string column (exact value match)
    SELECT
        IFF(company_code_t          = 'N/A', 'NA', company_code_t)          AS company_code
      , IFF(supplier_code_t         = 'N/A', 'NA', supplier_code_t)         AS supplier_code
      , IFF(supplier_name_t         = 'N/A', 'NA', supplier_name_t)         AS supplier_name
      , IFF(country_consigned_t     = 'N/A', 'NA', country_consigned_t)     AS country_consigned
      , IFF(street_t                = 'N/A', 'NA', street_t)                AS street
      , IFF(city_t                  = 'N/A', 'NA', city_t)                  AS city
      , IFF(post_code_t             = 'N/A', 'NA', post_code_t)             AS post_code
      , IFF(country_t               = 'N/A', 'NA', country_t)               AS country
      , IFF(c109_ref_t              = 'N/A', 'NA', c109_ref_t)              AS c109_ref
      , c109_date_t                                                         AS c109_date
      , IFF(trade_terms_t           = 'N/A', 'NA', trade_terms_t)           AS trade_terms
      , IFF(identification_number_t = 'N/A', 'NA', identification_number_t) AS identification_number
    FROM truncated
)
SELECT
    company_code
  , supplier_code
  , TRIM(REGEXP_REPLACE(REGEXP_REPLACE(supplier_name, '[^\\x00-\\x7F]|[|:#/?,%().-]', ''), '\\s+', ' ')) AS supplier_name
  , country_consigned
  , TRIM(REGEXP_REPLACE(REGEXP_REPLACE(street,        '[^\\x00-\\x7F]|[|:#/?,%().-]', ''), '\\s+', ' ')) AS street
  , TRIM(REGEXP_REPLACE(REGEXP_REPLACE(city,          '[^\\x00-\\x7F]|[|:#/?,%().-]', ''), '\\s+', ' ')) AS city
  , TRIM(REGEXP_REPLACE(REGEXP_REPLACE(post_code,     '[^\\x00-\\x7F]|[|:#/?,%().-]', ''), '\\s+', ' ')) AS post_code
  , country
  , c109_ref
  , c109_date
  , trade_terms
  , identification_number
FROM na_fixed
"""
# Final SELECT above: strip non-ASCII and | : # / ? , % ( ) . - from free-text
# columns, collapse whitespace, trim.


# ---------------------------------------------------------------------
# 3.3 Run settings
# Returns (debug, incremental_run, days_lag). Fabric used the workspace name;
# here the procedure's database name decides:
#   *DEV* -> debug, full run, 7 days | *UAT* -> debug, incremental, 7 days
#   other -> no debug, incremental, 1 day (PROD)
# Conversion:
#   Fabric  : workspace_name = notebookutils.mssparkutils.env.getWorkspaceName()
#   Snowpark: SELECT CURRENT_DATABASE() (procedure runs in END_EDW_<env>)
# ---------------------------------------------------------------------
def get_run_settings(session):
    db_name = session.sql("SELECT CURRENT_DATABASE()").collect()[0][0] or ""
    if "DEV" in db_name.upper():
        return True, False, 7
    if "UAT" in db_name.upper():
        return True, True, 7
    return False, True, 7


# ---------------------------------------------------------------------
# 3.4 File helpers
# Conversion:
#   Fabric  : save_dataframe_to_csv(df, path) did df.toPandas() + to_csv and
#             wrote straight to the mounted Lakehouse path
#   Snowpark: build_file_text() keeps the same pandas to_csv options
#             (sep '|', no header, QUOTE_NONE, '\' escape, trailing '|');
#             main() writes it to /tmp and PUTs it to the stage
#   get_file_name(): same 17:15 UTC cutoff and suppliers_YYYY-MM-DD-HH.dat name
# ---------------------------------------------------------------------
def build_file_text(pdf):
    """Pipe-delimited, no header, QUOTE_NONE with '\\' escape, trailing '|' on every line."""
    use_pipes = len(pdf.columns) != 1
    # Replace newlines / carriage returns value by value. pandas replace(regex=True)
    # raises "cannot call vectorize on size 0 inputs" on empty or all-NULL columns.
    newline_re = re.compile(r"\r\n|\n|\r")
    pdf = pdf.apply(
        lambda s: s.map(lambda v: newline_re.sub(" ", v) if isinstance(v, str) else v)
    )
    csv_data = pdf.to_csv(
        sep="|" if use_pipes else "~",
        index=False,
        header=True,
        quoting=csv.QUOTE_NONE,
        escapechar="\\",
    )
    return "\n".join(line + "|" for line in csv_data.split("\n") if line)


def get_file_name(now):
    # Stamp tomorrow's date if after 17:15 UTC (same rule as the Fabric notebook)
    cutoff_time = now.replace(hour=17, minute=15, second=0, microsecond=0)
    stamp_dt = now + timedelta(days=1) if now > cutoff_time else now
    return f"suppliers_{stamp_dt.strftime('%Y-%m-%d-%H')}.dat"


# ---------------------------------------------------------------------
# 3.5 main() - procedure entry point
# Conversion (Fabric cell -> Snowpark step):
#   "Init Days Lag Var"        -> filterdate from get_run_settings()
#   "Init Query(s)"            -> session.sql(SUPPLIERS_SQL, params=[...])
#   "Remove Rows Already Sent" -> spark.read.table(...) left_anti join
#                                 -> session.table(...) join(..., "leftanti")
#   "Export Good"              -> toPandas() -> to_pandas(); file PUT to stage
#   "Init Record Tracking"     -> read + union + dropDuplicates + overwrite
#                                 -> INSERT of new rows only (same end result,
#                                    since sent suppliers were already removed)
#   "Send To Azure Blob"       -> notebookutils.fs.fastcp -> COPY FILES INTO
#   notebook.exit(msg)         -> return msg
#   Dropped: display(), the debug filter on Supplier_Code == 1471
# ---------------------------------------------------------------------
def main(session):
    # Set True to copy the file to ToBeSent/ for SFTP (off, as in Fabric)
    transfer_file = False
    log = []

    # Run mode and look-back date
    debug, incremental_run, default_days_lag = get_run_settings(session)
    filterdate = (datetime.utcnow() - timedelta(days=default_days_lag)).date()
    if debug:
        log.append(f"debug={debug} incremental={incremental_run} from={filterdate}")

    # Extract + cleanse suppliers changed since filterdate
    suppliers_df = session.sql(SUPPLIERS_SQL, params=[str(filterdate)])

    # Remove rows already sent (exist in record tracking)
    if incremental_run:
        sent_df = session.table(TRACKING_TABLE).select("SUPPLIER_CODE").distinct()
        suppliers_df = suppliers_df.join(
            sent_df, suppliers_df["SUPPLIER_CODE"] == sent_df["SUPPLIER_CODE"], "leftanti"
        )

    # Fix column order, sort, and materialise once (reused for file + tracking)
    suppliers_df = suppliers_df.select(EXPORT_COLS).sort("SUPPLIER_CODE")
    suppliers_df = suppliers_df.cache_result()

    # File name based on current UTC time
    now = datetime.utcnow()
    file_name = get_file_name(now)

    # Export good file: write locally in the sandbox, then upload to stage Output/
    # (file is written even when empty, as in Fabric)
    pdf = suppliers_df.to_pandas()
    local_path = os.path.join("/tmp", file_name)
    with open(local_path, "w") as f:
        f.write(build_file_text(pdf))
    session.file.put(local_path, f"{STAGE}/Output", auto_compress=False, overwrite=True)

    row_count = len(pdf)
    ready_to_copy = row_count > 0

    # Record tracking: append exported rows with file name and timestamp
    # (incremental runs only). Appended directly from the DataFrame because
    # EXECUTE AS OWNER procedures cannot create temporary tables.
    if incremental_run and ready_to_copy:
        tracking_rows = (
            suppliers_df.select(EXPORT_COLS)
            .distinct()
            .with_column("EXPORTNAME", F.lit(file_name))
            .with_column("EXPORTDATE", F.current_timestamp().cast(TimestampType(TimestampTimeZone.NTZ)))
        )
        tracking_rows.write.mode("append").save_as_table(TRACKING_TABLE, column_order="name")

    # Nothing exported -> stop here
    if not ready_to_copy:
        return f"Process Complete - 0 rows - {STAGE}/Output/{file_name}" + (f" | {'; '.join(log)}" if debug else "")

    # Copy to ToBeSent for SFTP pickup
    if transfer_file:
        session.sql(
            f"COPY FILES INTO {STAGE}/ToBeSent/ FROM {STAGE}/Output/ FILES = ('{file_name}')"
        ).collect()

    # Status returned to the caller
    msg = f"Process Complete - {row_count} rows - {STAGE}/Output/{file_name}"
    return msg + (f" | {'; '.join(log)}" if debug else "")
$$;
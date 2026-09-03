import streamlit as st
import subprocess

# ==== Change this path if it is different on your system ====
PROJECT_ROOT = "/home/student/uber_pipeline"

st.title("Uber Pipeline Control Panel")


# ============================================================
# Streaming
# ============================================================

st.header("📡 Streaming")

col1, col2 = st.columns(2)

with col1:
    if st.button("▶️ Start Streaming"):
        subprocess.run(
            ["bash", f"{PROJECT_ROOT}/start_streaming.sh"]
        )
        st.success("Streaming started")

with col2:
    if st.button("⏹️ Stop Streaming"):
        subprocess.run(
            ["bash", f"{PROJECT_ROOT}/stop_streaming.sh"]
        )
        st.warning("Streaming stopped")


# ============================================================
# Batch
# ============================================================

st.header("📦 Batch")

col3, col4 = st.columns(2)

with col3:
    if st.button("▶️ Start Batch"):
        subprocess.run(
            ["bash", f"{PROJECT_ROOT}/start_batch.sh"]
        )
        st.success("Batch job started")

with col4:
    if st.button("⏹️ Stop Batch"):
        subprocess.run(
            ["bash", f"{PROJECT_ROOT}/stop_batch.sh"]
        )
        st.warning("Batch job stopped")


# ============================================================
# Silver Union
# ============================================================

st.header("🔗 Silver Union")

if st.button("🔗 Run Silver Union"):

    with st.spinner("Merging Batch and Streaming into Silver ..."):

        result = subprocess.run(
            [
                "spark-submit",
                f"{PROJECT_ROOT}/spark/silver/silver_union.py",
                "--batch-path",
                f"{PROJECT_ROOT}/silver/batch_output",
                "--stream-path",
                f"{PROJECT_ROOT}/silver/stream_output",
                "--unified-output",
                f"{PROJECT_ROOT}/silver/unified_rides",
                "--seen-ids-path",
                f"{PROJECT_ROOT}/checkpoints/seen_trip_ids_unified",
            ],
            capture_output=True,
            text=True,
        )

    if result.returncode == 0:

        st.success(
            "Silver layer updated (batch + streaming merged)"
        )

    else:

        st.error(
            "silver_union.py failed — see details below"
        )

        st.code(
            result.stderr[-2000:]
        )


# ============================================================
# Gold Layer
# ============================================================

st.header("🥇 Gold Layer")

if st.button("🔄 Run Gold ETL"):

    with st.spinner("Running gold_etl.py ..."):

        result = subprocess.run(
            [
                "spark-submit",
                f"{PROJECT_ROOT}/spark/gold/gold_etl.py",
                "--unified-path",
                f"{PROJECT_ROOT}/silver/unified_rides",
                "--gold-base",
                f"{PROJECT_ROOT}/gold",
                "--checkpoints-base",
                f"{PROJECT_ROOT}/checkpoints",
            ],
            capture_output=True,
            text=True,
        )

    if result.returncode == 0:

        st.success(
            "Gold layer updated"
        )

    else:

        st.error(
            "Execution failed — see details below"
        )

        st.code(
            result.stderr[-2000:]
        )


# ============================================================
# Export
# ============================================================

st.header("📤 Export")

if st.button("📤 Export Final CSVs"):

    with st.spinner("Exporting files ..."):

        result = subprocess.run(
            [
                "spark-submit",
                f"{PROJECT_ROOT}/export_final.py",
                "--project-root",
                PROJECT_ROOT,
            ],
            capture_output=True,
            text=True,
        )

    if result.returncode == 0:

        st.success(
            "Export complete"
        )

    else:

        st.error(
            "Export failed — see details below"
        )

        st.code(
            result.stderr[-2000:]
        )


# ============================================================
# Hive Query Runner
# ============================================================

st.header("🐝 Hive Query")

default_query = (
    "USE uber_pipeline;\n"
    "SELECT COUNT(*) AS total_trips FROM gold_fact_trip;"
)

query_text = st.text_area(
    "Write your HiveQL query here "
    "(multiple statements separated by ; are allowed)",
    value=default_query,
    height=150,
)


if st.button("▶️ Run Query"):

    with st.spinner("Executing query on Hive ..."):

        result = subprocess.run(
            [
                "hive",
                "--hiveconf",
                "hive.cli.print.header=true",
                "-S",
                "-e",
                query_text,
            ],
            capture_output=True,
            text=True,
        )


    # ========================================================
    # Hive Query Failed
    # ========================================================

    if result.returncode != 0:

        st.error(
            "Query execution failed — see the error message below"
        )

        st.code(
            result.stderr[-3000:]
        )


    # ========================================================
    # Hive Query Successful
    # ========================================================

    else:

        raw_output = result.stdout.strip()

        if not raw_output:

            st.info(
                "Query executed successfully but returned "
                "no result to display."
            )

        else:

            # Remove empty lines
            lines = [
                line
                for line in raw_output.splitlines()
                if line.strip()
            ]

            # First line is the header
            row_count = max(
                len(lines) - 1,
                0
            )

            st.success(
                f"Query executed successfully — {row_count} rows"
            )

            # =================================================
            # Build a Markdown table manually from the
            # tab-separated Hive output and render it with
            # st.markdown(). This deliberately avoids
            # st.dataframe() / st.table(), which crash on
            # this VM's outdated browser (missing
            # Array.prototype.at support in Streamlit's
            # Quiver/Arrow rendering).
            # =================================================

            def clean_cell(cell: str) -> str:
                # Strip Hive's "table.column" prefixes and
                # escape any literal "|" so it doesn't break
                # the Markdown table syntax.
                cell = cell.split(".")[-1].strip()
                return cell.replace("|", "\\|")

            rows = [line.split("\t") for line in lines]

            # Pad/truncate every row to match the header's
            # column count, in case Hive output is ragged.
            header = [clean_cell(c) for c in rows[0]]
            col_count = len(header)

            data_rows = []
            for row in rows[1:]:
                row = [clean_cell(c) for c in row]
                if len(row) < col_count:
                    row += [""] * (col_count - len(row))
                elif len(row) > col_count:
                    row = row[:col_count]
                data_rows.append(row)

            md_lines = []
            md_lines.append("| " + " | ".join(header) + " |")
            md_lines.append("|" + "|".join(["---"] * col_count) + "|")
            for row in data_rows:
                md_lines.append("| " + " | ".join(row) + " |")

            st.markdown("\n".join(md_lines))
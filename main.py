import io

import duckdb
import numpy as np
import pandas as pd
import streamlit as st

from idc_dst_12b1_rep_allocation import report


CALCULATOR_COLUMNS = [
    "TrailerPayoutDate",
    "TradeDate",
    "RepNumber",
    "CustomAcctNum",
    "DlrCommissionAmt",
    "TrailerFee",
    "Commission",
    "CommissionAmount",
]


def combine_uploaded_csvs(uploaded_files):
    frames = []
    for uploaded_file in uploaded_files:
        uploaded_file.seek(0)
        frames.append(pd.read_csv(uploaded_file))

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def show_uploaded_file_list(uploaded_files):
    if uploaded_files:
        st.write("Selected CSV files:")
        st.table(pd.DataFrame({"File name": [uploaded_file.name for uploaded_file in uploaded_files]}))


def build_529_report(uploaded_files):
    filtered_frames = []
    skipped_files = []

    for uploaded_file in uploaded_files:
        uploaded_file.seek(0)
        frame = pd.read_csv(uploaded_file)
        if "SocialCode" not in frame.columns:
            skipped_files.append(uploaded_file.name)
            continue

        social_code = frame["SocialCode"].astype("string").str.replace('="', "", regex=False).str.replace('"', "", regex=False)
        filtered_frame = frame.loc[social_code == "529"].copy()
        if filtered_frame.empty or not any(column in filtered_frame.columns for column in CALCULATOR_COLUMNS):
            continue

        for column in CALCULATOR_COLUMNS:
            if column not in filtered_frame.columns:
                filtered_frame[column] = ""

        for date_column in ("TradeDate", "TrailerPayoutDate"):
            filtered_frame[date_column] = pd.to_datetime(
                filtered_frame[date_column].astype(str),
                format="%Y%m%d",
                errors="coerce",
            )

        for numeric_column in ("DlrCommissionAmt", "CommissionAmount"):
            filtered_frame[numeric_column] = pd.to_numeric(
                filtered_frame[numeric_column]
                .astype("string")
                .str.replace('="', "", regex=False)
                .str.replace('"', "", regex=False)
                .str.replace(",", "", regex=False),
                errors="coerce",
            )

        filtered_frames.append(filtered_frame[CALCULATOR_COLUMNS])

    if not filtered_frames:
        return pd.DataFrame(columns=CALCULATOR_COLUMNS), skipped_files

    output_df = pd.concat(filtered_frames, ignore_index=True)
    sum_row = pd.DataFrame(output_df.select_dtypes(include=[np.number]).sum()).transpose()
    sum_row.index = ["Total"]
    return pd.concat([output_df, sum_row], ignore_index=False), skipped_files


def render_12b1_report():
    st.title("12B1 Rep Allocation Report")
    st.write("Upload one or more CSV files with `RepNumber`, `CommissionAmount`, and `TrailerPayoutDate` columns.")
    uploaded_files = st.file_uploader(
        "Choose CSV files",
        type=["csv"],
        accept_multiple_files=True,
        key="12b1_files",
    )
    show_uploaded_file_list(uploaded_files)

    if st.button("Process 12B-1 report"):
        if not uploaded_files:
            st.error("Please upload at least one CSV file before submitting.")
            return

        combined_df = combine_uploaded_csvs(uploaded_files)
        conn = duckdb.connect()
        try:
            report.load_sales_data(conn, io.StringIO(combined_df.to_csv(index=False)))
            errors = report.validate_sales_data(conn)
            if errors:
                for error in errors:
                    st.error(error)
                return

            result_df = report.get_rep_commissions(conn, True)
            st.write("Aggregated commissions by RepNumber:")
            st.dataframe(
                result_df,
                hide_index=True,
                column_config={
                    "CommissionAmount": st.column_config.NumberColumn(format="$ %.2f")
                },
            )
            payout_date = report.get_trailer_payout_date_display(conn)
            st.markdown(
                f"<div style='text-align: right;'>Data date: {payout_date}</div>",
                unsafe_allow_html=True,
            )
        finally:
            conn.close()


def render_529_calculator():
    st.title("529 Calculator")
    st.write("Upload one or more CSV files to filter 529 accounts and calculate column totals.")
    st.markdown(
        """
        <style>
        @media print {
            div[data-testid="stFileUploader"] {
                display: none !important;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    uploaded_files = st.file_uploader(
        "Choose CSV files",
        type=["csv"],
        accept_multiple_files=True,
        key="529_files",
    )
    show_uploaded_file_list(uploaded_files)

    if st.button("Process 529 files"):
        if not uploaded_files:
            st.error("Please upload at least one CSV file before submitting.")
            return

        result_df, skipped_files = build_529_report(uploaded_files)
        for filename in skipped_files:
            st.warning(f"Skipped {filename}: missing SocialCode column.")

        if result_df.empty:
            st.warning("No rows with SocialCode 529 were found in the uploaded files.")
            return

        st.table(result_df)
        st.download_button(
            "Download 529 report",
            data=result_df.to_csv(index=True).encode("utf-8"),
            file_name="529_report.csv",
            mime="text/csv",
        )


st.set_page_config(page_title="ASBD CSV Tools")
selected_tool = st.segmented_control(
    "Tool",
    ["12B-1 Report", "529 Calculator"],
    default="12B-1 Report",
    selection_mode="single",
    width="stretch",
)

if selected_tool == "529 Calculator":
    render_529_calculator()
else:
    render_12b1_report()

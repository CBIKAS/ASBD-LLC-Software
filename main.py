import streamlit as st
import duckdb
from idc_dst_12b1_rep_allocation import report

st.set_page_config(page_title="12B1 Rep Allocation")
st.title("12B1 Rep Allocation Report")

st.write("Upload a CSV with `RepNumber` and `CommissionAmount` columns.")

uploaded_file = st.file_uploader("Choose CSV file", type=["csv"])

if st.button("Submit"):
    if uploaded_file is None:
        st.error("Please upload a CSV file before submitting.")
    else:
        conn = duckdb.connect()
        try:
            report.load_sales_data(conn, uploaded_file)
            errors = report.validate_sales_data(conn)
            if errors:
                for e in errors:
                    st.error(e)
            else:
                df = report.get_rep_commissions(conn, True)
                st.write("Aggregated commissions by RepNumber:")
                st.dataframe(df)
        finally:
            conn.close()

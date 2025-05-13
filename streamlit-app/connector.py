import streamlit as st
import pandas as pd
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create or connect to Spark session
spark = SparkSession.builder.appName("KPI_Dashboard").getOrCreate()

# Load precomputed data (Parquet recommended)
operator_kpis = spark.read.parquet("output/avg_signal_per_operator.parquet") \
    .join(spark.read.parquet("output/avg_precision_per_operator.parquet"), on="operator", how="outer") \
    .toPandas()

status_kpis = spark.read.parquet("output/status_count_per_postal.parquet").toPandas()

# Sidebar selection
view_type = st.sidebar.selectbox("Select View Type", ["Operator KPIs", "Postal Code Statuses"])

if view_type == "Operator KPIs":
    selected_operator = st.sidebar.selectbox("Select Operator", operator_kpis["operator"].dropna().unique())
    filtered = operator_kpis[operator_kpis["operator"] == selected_operator]

    st.metric("Average Signal Strength", round(filtered["avg_signal_strength"].values[0], 2))
    st.metric("Average GPS Precision", round(filtered["avg_gps_precision"].values[0], 2))

    st.bar_chart(filtered[["avg_signal_strength", "avg_gps_precision"]].T)

    st.subheader("Raw Data")
    st.dataframe(filtered)

elif view_type == "Postal Code Statuses":
    selected_postal = st.sidebar.selectbox("Select Postal Code", status_kpis["postal_code"].dropna().unique())
    filtered = status_kpis[status_kpis["postal_code"] == selected_postal]

    st.subheader("Network Status Counts")
    st.bar_chart(filtered.set_index("status")["status_count"])

    st.subheader("Raw Data")
    st.dataframe(filtered)

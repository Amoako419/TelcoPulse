import streamlit as st
import pandas as pd
import plotly.express as px
from connector import S3Connector
import os
from dotenv import load_dotenv
import time

load_dotenv()

# Set page config
st.set_page_config(
    page_title="S3 Parquet Dashboard",
    page_icon="📊",
    layout="wide"
)

# Initialize S3 connector
@st.cache_resource
def get_s3_connector():
    return S3Connector()

# Add auto-refresh functionality
def auto_refresh():
    time.sleep(300)  # Sleep for 5 minutes
    st.experimental_rerun()

# Sidebar configuration
st.sidebar.title("Configuration")
bucket_name = st.sidebar.text_input("S3 Bucket Name", value=os.getenv('S3_BUCKET_NAME', ''))
prefix = st.sidebar.text_input("Prefix (optional)", value='')

# Add refresh status in sidebar
st.sidebar.markdown("---")
st.sidebar.info("🔄 Auto-refreshing every 5 minutes")

# Main content
st.title("📊 S3 Parquet Data Dashboard")

# Load data
if bucket_name:
    with st.spinner("Loading data from S3..."):
        s3_connector = get_s3_connector()
        df = s3_connector.get_all_data(bucket_name, prefix)
        
        if df is not None:
            st.success(f"Successfully loaded {len(df)} rows of data!")
            
            # Display basic information
            st.subheader("Dataset Overview")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Rows", len(df))
            with col2:
                st.metric("Total Columns", len(df.columns))
            with col3:
                st.metric("Memory Usage", f"{df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
            
            # Display data preview
            st.subheader("Data Preview")
            st.dataframe(df.head())
            
            # Column selection for visualization
            st.subheader("Data Visualization")
            col1, col2 = st.columns(2)
            
            with col1:
                x_col = st.selectbox("Select X-axis", df.columns)
            with col2:
                y_col = st.selectbox("Select Y-axis", df.columns)
            
            # Create visualization
            if x_col and y_col:
                try:
                    fig = px.scatter(df, x=x_col, y=y_col, title=f"{y_col} vs {x_col}")
                    st.plotly_chart(fig, use_container_width=True)
                except Exception as e:
                    st.error(f"Error creating visualization: {str(e)}")
            
            # Display column statistics
            st.subheader("Column Statistics")
            st.dataframe(df.describe())
            
        else:
            st.error("No data found in the specified location. Please check your bucket name and prefix.")
else:
    st.info("Please enter an S3 bucket name in the sidebar to begin.")

# Start auto-refresh in background
if bucket_name:
    auto_refresh() 
import streamlit as st
import pandas as pdc
import boto3
import json
import awswrangler as wr
import pg8000
import plotly.express as px
from datetime import datetime
from botocore.exceptions import ClientError, NoCredentialsError
import time
from sqlalchemy import create_engine

# Page configuration
st.set_page_config(
    page_title="Data Warehouse Explorer",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Get DB credentials from Secrets Manager ---
@st.cache_resource(show_spinner=False)
def get_rds_secret():
    try:
        secret_name = "warehouse-db-credentials"
        region_name = "eu-west-2"

        client = boto3.client("secretsmanager", region_name=region_name)
        response = client.get_secret_value(SecretId=secret_name)
        return json.loads(response["SecretString"])
    except Exception as e:
        st.error(f"❌ Error getting secrets: {e}")
        return None

# --- Connect to Warehouse using Session Manager ---
@st.cache_resource(show_spinner=False)
def connect_to_warehouse():
    try:
        creds = get_rds_secret()
        if not creds:
            return None
            
        st.sidebar.info("🔗 Connecting to warehouse...")
        
        # Use pg8000 with correct parameters (no connect_timeout)
        conn = pg8000.connect(
            host=creds["host"],
            port=int(creds["port"]),
            database=creds.get("database", "warehouse"),
            user=creds.get("username") or creds.get("user"),
            password=creds["password"]
            # pg8000 doesn't support connect_timeout parameter
        )
        
        # Test connection
        with conn.cursor() as cur:
            cur.execute("SELECT 1")

        st.sidebar.success("✅ Connected via port forwarding (Session Manager)")
        return conn

    except Exception as e:
        st.sidebar.error(f"❌ Connection failed: {str(e)}")
        
        # Provide specific error guidance
        if "timeout" in str(e).lower():
            st.sidebar.info("⏰ Connection timeout - check security groups and network connectivity")
        elif "password" in str(e).lower():
            st.sidebar.info("🔑 Authentication failed - check database credentials")
        elif "host" in str(e).lower():
            st.sidebar.info("🌐 Host not found - check RDS endpoint")
        
        return None
        
# --- Check AWS Session Manager Connectivity ---
def check_session_manager_connectivity():
    try:
        st.sidebar.info("🔍 Checking Session Manager access...")
        
        # Test Session Manager permissions
        ssm_client = boto3.client("ssm", region_name="eu-west-2")
        response = ssm_client.describe_sessions(State='Active')
        
        # Test Secrets Manager access
        sm_client = boto3.client("secretsmanager", region_name="eu-west-2")
        sm_client.list_secrets(MaxResults=1)
        
        st.sidebar.success("✅ AWS Session Manager access confirmed")
        return True
        
    except Exception as e:
        st.sidebar.error(f"❌ AWS Session Manager check failed: {e}")
        return False

# --- Get table data using Session Manager connection ---
@st.cache_data(ttl=300)
def get_tables_metadata(_conn):
    try:
        query = """
        SELECT 
            t.table_name,
            COUNT(c.column_name) as column_count,
            pg_size_pretty(pg_total_relation_size('"' || t.table_name || '"')) as table_size,
            (SELECT COUNT(*) FROM public."' || t.table_name || '") as row_count
        FROM information_schema.tables t
        LEFT JOIN information_schema.columns c 
            ON t.table_name = c.table_name AND t.table_schema = c.table_schema
        WHERE t.table_schema = 'public'
        GROUP BY t.table_name
        ORDER BY t.table_name;
        """
        return wr.postgresql.read_sql_query(query, _conn)
    except Exception as e:
        st.error(f"Error getting table metadata: {e}")
        return pd.DataFrame()

# --- Get table schema ---
@st.cache_data(ttl=300)
def get_table_schema(_conn, table_name):
    try:
        query = f"""
        SELECT 
            column_name, 
            data_type, 
            is_nullable,
            ordinal_position
        FROM information_schema.columns 
        WHERE table_name = '{table_name}' AND table_schema = 'public'
        ORDER BY ordinal_position;
        """
        return wr.postgresql.read_sql_query(query, _conn)
    except Exception as e:
        st.error(f"Error getting schema for {table_name}: {e}")
        return pd.DataFrame()

# --- Query table data ---
@st.cache_data(ttl=60)
def get_table_data(_conn, table_name, limit=100):
    try:
        query = f'SELECT * FROM public."{table_name}" LIMIT {limit};'
        return wr.postgresql.read_sql_query(query, _conn)
    except Exception as e:
        st.error(f"Error loading data from {table_name}: {e}")
        return pd.DataFrame()

# --- Main Streamlit App with Session Manager ---
def main():
    st.title("📊 Data Warehouse Explorer (AWS Session Manager)")
    
    # Check Session Manager connectivity
    sm_connected = check_session_manager_connectivity()
    
    if not sm_connected:
        st.error("""
        ❌ AWS Session Manager not configured properly. Please:
        1. Ensure IAM permissions are attached to your user/role
        2. Check that Session Manager is enabled in your AWS account
        3. Verify network connectivity to RDS
        """)
        return
    
    # Try to connect to warehouse
    conn = connect_to_warehouse()
    
    if conn is None:
        st.error("""
        ❌ Could not connect to data warehouse. Possible issues:
        - IAM permissions for Session Manager
        - RDS security group rules
        - Database credentials
        - Network connectivity
        """)
        return
    
    try:
        # Get list of tables
        query = "SELECT table_name FROM information_schema.tables WHERE table_schema='public';"
        tables_df = wr.postgresql.read_sql_query(query, conn)
        
        if tables_df.empty:
            st.warning("No tables found in the warehouse.")
            return
        
        table_names = tables_df['table_name'].tolist()
        
        # Dashboard Overview
        st.header("📈 Warehouse Overview")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Total Tables", len(table_names))
        
        with col2:
            try:
                sample_query = f'SELECT COUNT(*) as count FROM public."{table_names[0]}"'
                count_df = wr.postgresql.read_sql_query(sample_query, conn)
                st.metric("Sample Table Rows", f"{count_df['count'].iloc[0]:,}")
            except:
                st.metric("Sample Table Rows", "N/A")
        
        with col3:
            st.metric("Connection Method", "AWS Session Manager")
        
        # Table Explorer
        st.header("🔍 Table Explorer")
        selected_table = st.selectbox("Choose a table to explore:", table_names)
        
        if selected_table:
            st.subheader(f"📄 Table: {selected_table}")
            
            # Get table data
            table_data = get_table_data(conn, selected_table, 100)
            
            if not table_data.empty:
                st.dataframe(table_data, use_container_width=True)
                
                # Show schema info
                schema_df = get_table_schema(conn, selected_table)
                if not schema_df.empty:
                    st.subheader("📋 Schema")
                    st.dataframe(schema_df, use_container_width=True, hide_index=True)
            else:
                st.warning(f"No data available for table {selected_table}")
    
    except Exception as e:
        st.error(f"Error interacting with database: {e}")
    
    finally:
        try:
            conn.close()
        except:
            pass

# --- Debug information ---
def show_debug_info():
    st.sidebar.header("🔧 Debug Info")
    st.sidebar.write(f"**Region:** eu-west-2")
    st.sidebar.write(f"**Connection:** AWS Session Manager")
    st.sidebar.write(f"**Time:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Test connection button
    if st.sidebar.button("🔄 Test Connection"):
        with st.sidebar:
            with st.spinner("Testing connection..."):
                conn = connect_to_warehouse()
                if conn:
                    st.success("✅ Connection successful!")
                    conn.close()
                else:
                    st.error("❌ Connection failed")

if __name__ == "__main__":
    show_debug_info()
    main()
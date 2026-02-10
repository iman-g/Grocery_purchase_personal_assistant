import streamlit as st
import pandas as pd
import plotly.express as px
import google.generativeai as genai
from streamlit_gsheets import GSheetsConnection


# --- CONFIGURATION ---
st.set_page_config(page_title="Family Grocery Tracker", page_icon="🛒", layout="wide")
st.title("🛒 Family Grocery Advisor")

# --- 1. DATA LOADING & CLEANING ---
@st.cache_data(ttl=60) # Refresh data every 60 seconds
def load_data():
    # Connect to Google Sheets
    conn = st.connection("gsheets", type=GSheetsConnection)
    df = conn.read()
    
    # Clean Data (Data Science best practices!)
    # Convert Price and Quantity to numeric, coercing errors to NaN
    df['price'] = pd.to_numeric(df['price'], errors='coerce').fillna(0)
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').fillna(0)
    
    # Create 'Total' column if it doesn't exist
    if 'Total' not in df.columns:
        df['Total'] = df['price'] * df['quantity']
        
    # Convert Date to datetime
    df['date'] = pd.to_datetime(df['date'], dayfirst=True, errors='coerce')
    
    return df

try:
    df = load_data()
    # Sidebar Filters
    st.sidebar.header("Filters")
    selected_product = st.sidebar.multiselect("Select Product", df['product_english'].unique())
    
    if selected_product:
        df_filtered = df[df['product_english'].isin(selected_product)]
    else:
        df_filtered = df

except Exception as e:
    st.error(f"Error loading data: {e}")
    st.stop()

# --- TABS SETUP ---
tab1, tab2 = st.tabs(["📊 Dashboard", "🤖 AI Advisor"])

# --- TAB 1: VISUALIZATIONS ---
with tab1:
    # KPI Metrics
    col1, col2, col3 = st.columns(3)
    
    # Calculate Date Ranges
    today = pd.Timestamp.now()
    last_week = today - pd.Timedelta(days=7)
    last_month = today - pd.Timedelta(days=30)

    recent_spend = df[df['date'] >= last_week]['Total'].sum()
    monthly_spend = df[df['date'] >= last_month]['Total'].sum()
    top_item = df.groupby('product_english')['quantity'].sum().idxmax() if not df.empty else "N/A"

    col1.metric("Spent Last 7 Days", f"€{recent_spend:.2f}")
    col2.metric("Spent Last 30 Days", f"€{monthly_spend:.2f}")
    col3.metric("Top Item (All Time)", top_item)

    st.divider()

    # Plots
    c1, c2 = st.columns(2)
    
    with c1:
        st.subheader("Spending Over Time")
        # Group by Date
        daily_spend = df_filtered.groupby('date')['Total'].sum().reset_index()
        fig_line = px.line(daily_spend, x='date', y='Total', markers=True, title="Daily Spending Trend")
        st.plotly_chart(fig_line, use_container_width=True)

    with c2:
        st.subheader("Top Products by Cost")
        # Top 10 expensive items
        top_products = df_filtered.groupby('product_english')['Total'].sum().nlargest(10).reset_index()
        fig_bar = px.bar(top_products, x='Total', y='product_english', orientation='h', title="Top 10 Most Expensive Items")
        st.plotly_chart(fig_bar, use_container_width=True)

# --- TAB 2: AI CHATBOT (RAG) ---
with tab2:
    st.subheader("Ask Gemini about your purchases")
    
    # Initialize Chat History
    if "messages" not in st.session_state:
        st.session_state.messages = []

    # Display Chat History
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # Chat Input
    if prompt := st.chat_input("Ex: How much did we spend on milk last month?"):
        # 1. User Message
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        # 2. Prepare Context (The "RAG" part)
        # We convert the dataframe to CSV string so Gemini can read it
        # Limit to last 100 rows to save tokens if data gets huge
        csv_context = df.tail(100).to_csv(index=False)
        
        system_prompt = f"""
        You are a helpful household data assistant. 
        You have access to the family's grocery data in CSV format below.
        
        DATA CONTEXT:
        {csv_context}
        
        Answer the user's question based strictly on this data. 
        If the answer isn't in the data, say you don't know. 
        Use Euro (€) symbol for currency.
        """

        # 3. Call Gemini
        try:
            genai.configure(api_key=st.secrets["gemini"]["api_key"])
            model = genai.GenerativeModel('gemini-2.5-flash')
            
            response = model.generate_content([system_prompt, prompt])
            bot_reply = response.text
            
            # 4. Display Bot Message
            with st.chat_message("assistant"):
                st.markdown(bot_reply)
            st.session_state.messages.append({"role": "assistant", "content": bot_reply})
            
        except Exception as e:
            st.error(f"AI Error: {e}")
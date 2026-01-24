import streamlit as st
import requests
import json
import pandas as pd
import plotly.express as px
import re

# 1. Configuration & Layout
st.set_page_config(
    page_title="Pulsar | Data Platform AI",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for Glassmorphism and Premium Feel
st.markdown("""
<style>
    /* Google Fonts */
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600&display=swap');
    
    * {
        font-family: 'Outfit', sans-serif;
    }

    /* Main Background */
    .stApp {
        background: radial-gradient(circle at top right, #1a1a2e, #16213e);
        color: #e94560;
    }

    /* Glassmorphism Sidebar */
    [data-testid="stSidebar"] {
        background: rgba(26, 26, 46, 0.7);
        backdrop-filter: blur(15px);
        border-right: 1px solid rgba(255, 255, 255, 0.1);
    }

    /* Chat Input Styling */
    .stChatInputContainer {
        border-radius: 20px;
        background: rgba(255, 255, 255, 0.05);
        border: 1px solid rgba(255, 255, 255, 0.1);
        padding: 10px;
        backdrop-filter: blur(5px);
    }

    /* Message Bubbles */
    [data-testid="stChatMessage"] {
        background: rgba(255, 255, 255, 0.03);
        border-radius: 20px;
        border: 1px solid rgba(255, 255, 255, 0.05);
        margin-bottom: 20px;
        padding: 15px;
        transition: all 0.3s ease;
    }
    
    [data-testid="stChatMessage"]:hover {
        background: rgba(255, 255, 255, 0.05);
        border: 1px solid rgba(255, 255, 255, 0.1);
        transform: translateY(-2px);
    }

    /* Expander Styling */
    .streamlit-expanderHeader {
        background: rgba(255, 255, 255, 0.05) !important;
        border-radius: 10px !important;
        border: none !important;
    }

    h1, h2, h3 {
        color: #e94560 !important;
        font-weight: 600 !important;
    }

    p, span {
        color: #cfd8dc !important;
    }

    .stButton>button {
        background: linear-gradient(90deg, #e94560, #950740);
        color: white;
        border: none;
        border-radius: 10px;
        font-weight: 600;
        transition: all 0.3s ease;
    }

    .stButton>button:hover {
        opacity: 0.9;
        box-shadow: 0 4px 15px rgba(233, 69, 96, 0.4);
    }
</style>
""", unsafe_allow_html=True)

# 2. Helper Functions
def parse_and_plot(text):
    """Detects price data in text and returns a plotly chart if found."""
    # Pattern 1: Date/Time followed by Price (optional $)
    # Supports YYYY-MM-DD and YYYY-MM-DD HH:MM:SS
    matches = re.findall(r"(\d{4}-\d{2}-\d{2}(?:\s\d{2}:\d{2}:\d{2})?)[:\s]+(?:\$)?(\d+\.?\d*)", text)
    
    # Pattern 2: Price (optional $) followed by "on/for/at/as of" and Date/Time
    if not matches:
        conv_matches = re.findall(r"(?:\$)?(\d+\.?\d*)\s+(?:on|for|at|as of)\s+(\d{4}-\d{2}-\d{2}(?:\s\d{2}:\d{2}:\d{2})?)", text)
        if conv_matches:
            matches = [(d, p) for p, d in conv_matches]

    print(f"DEBUG - Text to parse: {text}")
    print(f"DEBUG - Matches found: {matches}")

    if matches:
        df = pd.DataFrame(matches, columns=["Date", "Price"])
        df["Price"] = pd.to_numeric(df["Price"])
        df = df.sort_values("Date")
        
        fig = px.line(
            df, x="Date", y="Price", 
            markers=True,
            template="plotly_dark",
            title="Equity Performance"
        )
        fig.update_traces(line_color='#e94560', marker=dict(size=8, color='#e94560'))
        fig.update_layout(
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font_family="Outfit",
            xaxis=dict(showgrid=False),
            yaxis=dict(showgrid=True, gridcolor='rgba(255,255,255,0.05)')
        )
        return fig
    return None

# 3. Main UI Header
col1, col2 = st.columns([1, 10])
with col1:
    st.image("https://img.icons8.com/isometric/100/e94560/rocket.png", width=60)
with col2:
    st.title("Pulsar | Financial AI")

# Sidebar
with st.sidebar:
    st.markdown("### 🛠️ Configuration")
    tenant_id = st.text_input("Tenant ID", value="IBM")
    st.divider()
    
    if st.button("🗑️ Clear Context"):
        st.session_state.messages = []
        st.rerun()

    st.markdown("---")
    st.markdown("### 📊 System Status")
    st.success("Query Engine: Online")
    st.success("Agent Core: Active")
    st.info("Tenant Context: Registered")

# 4. State Management
if "messages" not in st.session_state:
    st.session_state.messages = []

# 5. Rendering Chat History
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        if message.get("tool_used"):
            with st.expander("🔍 Trace Log"):
                st.code(f"EXECUTE: {message['tool_used']}", language="bash")
        
        st.markdown(message["content"])
        
        # Re-render plot if saved
        if message.get("chart_data"):
             # Re-parse for metrics persistence
             ticks = re.findall(r"(\d{4}-\d{2}-\d{2}(?:\s\d{2}:\d{2}:\d{2})?)[:\s]+(?:\$)?(\d+\.?\d*)", message["content"]) or \
                     [(d, p) for p, d in re.findall(r"(?:\$)?(\d+\.?\d*)\s+(?:on|for|at)\s+(\d{4}-\d{2}-\d{2}(?:\s\d{2}:\d{2}:\d{2})?)", message["content"])]
             
             df_hist = pd.DataFrame(ticks, columns=["Date", "Price"])
             if not df_hist.empty:
                df_hist["Price"] = pd.to_numeric(df_hist["Price"])
                df_hist = df_hist.sort_values("Date")
                latest = df_hist.iloc[-1]["Price"]
                prev = df_hist.iloc[0]["Price"] if len(df_hist) > 1 else latest
                m1, m2 = st.columns(2)
                m1.metric("Latest Price", f"${latest:.2f}", f"{latest-prev:+.2f}")
                m2.metric("High", f"${df_hist['Price'].max():.2f}")

             st.plotly_chart(message["chart_data"], width='stretch')

# 6. Interaction Logic
if query := st.chat_input("Analyze market data (e.g., Show me IBM stock performance for Jan 2025)"):
    st.chat_message("user").markdown(query)
    st.session_state.messages.append({"role": "user", "content": query})

    with st.chat_message("assistant"):
        answer = ""
        tool_used = None
        
        with st.status("Analyzing Financial Microstructures...", expanded=True) as status:
            try:
                # API Call to Agent Service
                response = requests.post(
                    "http://localhost:8000/api/chat",
                    json={"query": query, "tenant_id": tenant_id},
                    timeout=60
                )
                response.raise_for_status()
                data = response.json()
                
                answer = data.get("answer", "Analysis complete.")
                tool_used = data.get("tool_used")
                
                status.update(label="Analysis Finalized", state="complete", expanded=False)
            except Exception as e:
                status.update(label="Analysis Interrupted", state="error")
                st.error(f"Engine Failure: {str(e)}")
                st.stop() # Exit early on error

        # --- RENDER RESULTS OUTSIDE STATUS ---
        if tool_used:
            with st.expander("🔍 Trace Log"):
                st.code(f"EXECUTE: {tool_used}", language="bash")

        st.markdown(answer)
        
        # Check for chartable data
        fig = parse_and_plot(answer)
        if fig:
            # Re-parse for metrics (could optimize but keeping it simple)
            ticks = re.findall(r"(\d{4}-\d{2}-\d{2}(?:\s\d{2}:\d{2}:\d{2})?)[:\s]+(?:\$)?(\d+\.?\d*)", answer) or \
                    [(d, p) for p, d in re.findall(r"(?:\$)?(\d+\.?\d*)\s+(?:on|for|at)\s+(\d{4}-\d{2}-\d{2}(?:\s\d{2}:\d{2}:\d{2})?)", answer)]
            
            df_plot = pd.DataFrame(ticks, columns=["Date", "Price"])
            df_plot["Price"] = pd.to_numeric(df_plot["Price"])
            df_plot = df_plot.sort_values("Date")
            
            latest_price = df_plot.iloc[-1]["Price"]
            prev_price = df_plot.iloc[0]["Price"] if len(df_plot) > 1 else latest_price
            delta = latest_price - prev_price
            
            m1, m2 = st.columns(2)
            m1.metric("Latest Price", f"${latest_price:.2f}", f"{delta:+.2f}")
            m2.metric("High", f"${df_plot['Price'].max():.2f}")
            
            st.plotly_chart(fig, width='stretch')
        
        # Save to session state
        st.session_state.messages.append({
            "role": "assistant", 
            "content": answer, 
            "tool_used": tool_used,
            "chart_data": fig
        })

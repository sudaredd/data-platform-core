from langchain_google_genai import ChatGoogleGenerativeAI
from langchain.agents import AgentExecutor, create_tool_calling_agent
from langchain_core.prompts import ChatPromptTemplate
from app.tools import fetch_daily_data, fetch_realtime_data
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize LLM
llm = ChatGoogleGenerativeAI(model="gemini-2.0-flash", temperature=0)

# Define Tools
tools = [fetch_daily_data, fetch_realtime_data]

# Define System Prompt
prompt = ChatPromptTemplate.from_messages([
    ("system", "You are a financial data assistant. You have two tools: \n"
               "1. 'fetch_daily_data' for historical pricing over a date range (start/end dates).\n"
               "2. 'fetch_realtime_data' for current, live, or intraday pricing (terms like 'now', 'current', 'real-time').\n"
               "Always use the appropriate tool when asked about prices. "
               "For 'fetch_daily_data', the user provides the tenant_id: {tenant_id}. "
               "You MUST pass this tenant_id to the tool when calling it. "
               "For real-time queries about what is happening 'now', use 'fetch_realtime_data' with the correct ticker."),
    ("human", "{input}"),
    ("placeholder", "{agent_scratchpad}"),
])

# Create Agent
agent = create_tool_calling_agent(llm, tools, prompt)

# Create Executor
agent_executor = AgentExecutor(
    agent=agent, 
    tools=tools, 
    verbose=True, 
    return_intermediate_steps=True
)

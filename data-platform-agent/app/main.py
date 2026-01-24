from fastapi import FastAPI, HTTPException
from app.schemas import ChatRequest, ChatResponse
from app.agent import agent_executor

app = FastAPI(title="Data Platform Agent")

@app.post("/api/chat", response_model=ChatResponse)
async def chat_endpoint(request: ChatRequest):
    try:
        # Pass tenant_id and input (query) to the agent
        # The prompt template expects 'input' and we want to make 'tenant_id' available for the agent to use
        result = await agent_executor.ainvoke({
            "input": request.query,
            "tenant_id": request.tenant_id
        })
        
        # Extract the answer
        answer = result.get("output", "")
        
        # Determine tool usage for the UI trace log by checking intermediate_steps
        tool_used = None
        intermediate_steps = result.get("intermediate_steps", [])
        if intermediate_steps:
            # Get the name of the tool from the first action
            tool_used = intermediate_steps[0][0].tool
        elif "fetch_realtime_data" in str(result):
            # Fallback for older LangChain results or direct strings
            tool_used = "fetch_realtime_data"
        elif "fetch_daily_data" in str(result):
            tool_used = "fetch_daily_data"
        
        return ChatResponse(answer=answer, tool_used=tool_used)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

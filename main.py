import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from workflow_runner import WorkflowExecutor
import json

app = FastAPI()

# Embedded Workflow Data
WORKFLOW_DATA = {
  "nodes": [
    {
      "id": "api-1768636097809",
      "type": "api",
      "position": {
        "x": 198.6969974650221,
        "y": 391.42640713412345
      },
      "data": {
        "label": "New api"
      },
      "measured": {
        "width": 322,
        "height": 415
      }
    }
  ],
  "edges": []
}

@app.all("/{path:path}")
async def handle_request(request: Request):
    # Construct input data
    body = None
    try:
        body = await request.json()
    except:
        pass
        
    input_data = {
        "method": request.method,
        "path": request.url.path,
        "query": dict(request.query_params),
        "params": dict(request.path_params),
        "body": body,
        "headers": dict(request.headers)
    }
    
    executor = WorkflowExecutor(WORKFLOW_DATA)
    result = executor.run(input_data)
    
    if result.get("status") == "success":
        return result.get("response")
    else:
        # Return error details
        return JSONResponse(status_code=500, content=result)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

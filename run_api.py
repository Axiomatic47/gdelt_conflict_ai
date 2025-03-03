"""
Run the FastAPI application with Uvicorn

This script runs the FastAPI application with the proper import string,
allowing for hot reloading and multiple workers.
"""

import uvicorn

if __name__ == "__main__":
    # Run the application with the proper import string
    uvicorn.run("app.main:app", host="127.0.0.1", port=4041, reload=True)
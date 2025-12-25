from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def hello():
    return {"msg": "Hello from in-situ task on Raspberry Pi!"}

import asyncio
import csv
from pathlib import Path
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import HTMLResponse

# Shared telemetry state
engine_data = {
    "temperature": 75.0,
    "pressure": 30.0,
    "fuel_flow": 10.0
}

# Control State
is_system_active = True
is_stream_paused = False
stream_delay = 1.0  # Default 1 second interval

class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []
        self.host_socket: WebSocket | None = None

    async def connect(self, websocket: WebSocket) -> str:
        await websocket.accept()
        self.active_connections.append(websocket)
        
        # First client to connect becomes Host
        if self.host_socket is None:
            self.host_socket = websocket
            role = "host"
        else:
            role = "client"

        # Broadcast updated active client count
        await self.broadcast_node_count()
        return role

    async def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        if websocket == self.host_socket:
            self.host_socket = None
        await self.broadcast_node_count()

    async def broadcast_node_count(self):
        count = len(self.active_connections)
        for connection in list(self.active_connections):
            try:
                await connection.send_json({"type": "node_count", "count": count})
            except Exception:
                pass

    async def broadcast(self, message: dict):
        for connection in list(self.active_connections):
            try:
                await connection.send_json(message)
            except Exception:
                await self.disconnect(connection)

manager = ConnectionManager()

# Background CSV simulator
async def simulate_engine():
    global is_stream_paused, stream_delay
    csv_file_path = Path(__file__).parent / "data.csv"

    while is_system_active:
        if csv_file_path.exists():
            with open(csv_file_path, mode="r", encoding="utf-8") as file:
                reader = csv.DictReader(file)
                for row in reader:
                    if not is_system_active:
                        break
                    
                    while is_stream_paused and is_system_active:
                        await asyncio.sleep(0.2)
                    
                    if not is_system_active:
                        break

                    await asyncio.sleep(stream_delay)

                    engine_data["temperature"] = float(row["temperature"])
                    engine_data["pressure"] = float(row["pressure"])
                    engine_data["fuel_flow"] = float(row["fuel_flow"])

                    await manager.broadcast({"type": "tick", "data": engine_data})
        else:
            await asyncio.sleep(1)

@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(simulate_engine())
    yield
    task.cancel()

app = FastAPI(lifespan=lifespan)

@app.get("/", response_class=HTMLResponse)
async def get_dashboard(request: Request):
    html_file = Path(__file__).parent / "templates" / "client.html"
    return HTMLResponse(content=html_file.read_text(encoding="utf-8"))

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    global is_system_active, is_stream_paused, stream_delay
    
    assigned_role = await manager.connect(websocket)
    
    await websocket.send_json({"type": "role_assignment", "role": assigned_role})
    await websocket.send_json({
        "type": "stream_state", 
        "paused": is_stream_paused, 
        "delay": stream_delay
    })
    await websocket.send_json({"type": "tick", "data": engine_data})
    
    try:
        while True:
            data = await websocket.receive_json()
            action = data.get("action")
            
            if action == "update":
                new_vals = data.get("data", {})
                for key in ["temperature", "pressure", "fuel_flow"]:
                    if key in new_vals and new_vals[key] is not None:
                        engine_data[key] = float(new_vals[key])
                await manager.broadcast({"type": "tick", "data": engine_data})

            elif action == "toggle_pause" and websocket == manager.host_socket:
                is_stream_paused = not is_stream_paused
                await manager.broadcast({
                    "type": "stream_state", 
                    "paused": is_stream_paused, 
                    "delay": stream_delay
                })

            elif action == "set_speed" and websocket == manager.host_socket:
                new_delay = float(data.get("delay", 1.0))
                stream_delay = new_delay
                await manager.broadcast({
                    "type": "stream_state", 
                    "paused": is_stream_paused, 
                    "delay": stream_delay
                })

            elif action == "terminate_system" and websocket == manager.host_socket:
                is_system_active = False
                await manager.broadcast({
                    "type": "system_terminated",
                    "message": "Host terminated the telemetry session globally."
                })
                break

            elif action == "disconnect_client":
                await manager.disconnect(websocket)
                await websocket.close()
                break

    except WebSocketDisconnect:
        await manager.disconnect(websocket)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=True)
    
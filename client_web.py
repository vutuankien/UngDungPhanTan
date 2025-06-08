from flask import Flask, request, render_template
import grpc
import kvstore_pb2
import kvstore_pb2_grpc
import sys
import asyncio
import grpc.aio as aio  # Bước 1: Import gRPC AsyncIO

if len(sys.argv) > 1:
    port = sys.argv[1]
else:
    port = 50051  # default server port if not specified

# Danh sách các port node để tìm kiếm song song
NODE_PORTS = ["50051", "50052", "50053"]  # Đổi từ all_ports thành NODE_PORTS

app = Flask(__name__)

# Bước 2: Hàm async gửi truy vấn đến một node
async def async_search_node(port, keyword):
    async with aio.insecure_channel(f"localhost:{port}") as channel:
        stub = kvstore_pb2_grpc.KeyValueStoreStub(channel)
        try:
            res = await stub.SearchKey(kvstore_pb2.SearchRequest(keyword=keyword))
            return port, res
        except Exception as e:
            return port, None

# Hàm async tìm kiếm tuần tự từng node
async def sequential_search(keyword):
    for p in NODE_PORTS:
        port, res = await async_search_node(p, keyword)
        if res and res.results:
            return port, res
    return None

@app.route("/", methods=["GET", "POST"])
def index():
    message = ""
    result = ""
    if request.method == "POST":
        key = request.form["key"]
        value = request.form.get("value", "")
        action = request.form["action"]

        grpc_port = port
        if "grpc_port" in request.form and request.form["grpc_port"]:
            grpc_port = request.form["grpc_port"]

        if action == "search":
            try:
                if not key.strip() and not value.strip():
                    # Nếu cả key và value đều rỗng, lấy toàn bộ data ở port đã chọn
                    async def search_all(port):
                        async with aio.insecure_channel(f"localhost:{port}") as channel:
                            stub = kvstore_pb2_grpc.KeyValueStoreStub(channel)
                            res = await stub.SearchKey(kvstore_pb2.SearchRequest(keyword=""))
                            return port, res
                    port_found, res = asyncio.run(search_all(grpc_port))
                    if res and res.results:
                        message = f"All data from port {port_found}: {res.message}"
                        result = "\n".join([f"{k}: {v}" for k, v in res.results.items()])
                    else:
                        message = "No data found."
                else:
                    # Search tuần tự như cũ (ưu tiên tìm theo key, value)
                    search_keyword = key if key.strip() else value
                    search_result = asyncio.run(sequential_search(search_keyword))
                    if search_result and search_result[1]:
                        port_found, res = search_result
                        message = f"Found on port {port_found}: {res.message}"
                        result = "\n".join([f"{k}: {v}" for k, v in res.results.items()])
                    else:
                        message = "Not found on any node."
            except Exception as e:
                message = f"Async Search Error: {str(e)}"
        elif action == "delete":
            for node_port in NODE_PORTS:
                try:
                    with grpc.insecure_channel(f"localhost:{node_port}") as node_channel:
                        node_stub = kvstore_pb2_grpc.KeyValueStoreStub(node_channel)
                        res = node_stub.DeleteKey(kvstore_pb2.KeyRequest(key=key))
                        if res.message == "Deleted":
                            deleted_any = True
                        print(f"Deleted on node {node_port}: {res.message}")
                except Exception as e:
                    print(f"Failed to delete on node {node_port}: {e}")

            if deleted_any:
                message = "Deleted on all nodes (where existed)"
            else:
                message = "Key not found on any node"
        else:
            # Các thao tác khác giữ nguyên, dùng gRPC sync
            with grpc.insecure_channel(f"localhost:{grpc_port}") as channel:
                stub = kvstore_pb2_grpc.KeyValueStoreStub(channel)
                try:
                    if action == "get":
                        res = stub.GetKey(kvstore_pb2.KeyRequest(key=key))
                        message = res.message
                        result = f"{res.key}: {res.value}"
                    elif action == "update":
                        res = stub.UpdateKey(kvstore_pb2.UpdateRequest(key=key, value=value))
                        message = res.message
                        if res.message == "Not found":
                            result = ""
                        else:
                            result = f"{res.key}: {res.value}"
                    elif action == "put":
                        res = stub.PutKey(kvstore_pb2.PutKeyRequest(key=key, value=value))
                        message = res.message
                        result = f"{res.key}: {res.value}"
                except grpc.RpcError as e:
                    message = f"gRPC Error: {e.details()}"

    return render_template('index.html', message=message, result=result)

if __name__ == "__main__":
    app.run(port=port)

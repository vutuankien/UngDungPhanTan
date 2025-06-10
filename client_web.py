from flask import Flask, request, render_template
import grpc
import kvstore_pb2
import kvstore_pb2_grpc
import sys
import asyncio
import grpc.aio as aio  

if len(sys.argv) > 1:
    port = sys.argv[1]
else:
    port = 50051 

NODE_PORTS = ["50051", "50052", "50053"] 

app = Flask(__name__)

async def async_search_node(port, keyword):
    async with aio.insecure_channel(f"localhost:{port}") as channel:
        stub = kvstore_pb2_grpc.KeyValueStoreStub(channel)
        try:
            res = await stub.SearchKey(kvstore_pb2.SearchRequest(keyword=keyword))
            return port, res
        except Exception as e:
            return port, None


async def sequential_search(keyword):
    for p in NODE_PORTS:
        port, res = await async_search_node(p, keyword)
        if res and res.results:
            return port, res
    return None

async def parallel_search_all_nodes(keyword):
    tasks = [async_search_node(p, keyword) for p in NODE_PORTS]
    results = await asyncio.gather(*tasks)
    combined = {}
    port_messages = []
    for port, res in results:
        if res and res.results:
            combined.update(res.results)
            port_messages.append(f"{port}: {res.message}")
    return combined, port_messages

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
                    search_keyword = key if key.strip() else value
                    combined_results, port_messages = asyncio.run(parallel_search_all_nodes(search_keyword))
                    if combined_results:
                        message = " | ".join(port_messages)
                        result = "\n".join([f"{k}: {v}" for k, v in combined_results.items()])
                    else:
                        message = "Not found on any node."
            except Exception as e:
                message = f"Async Search Error: {str(e)}"
        elif action == "delete":
            NODE_PORTS = ["50051", "50052", "50053"] 
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
        elif action == "update":
            NODE_PORTS = ["50051", "50052", "50053"]
            updated_any = False
            result = ""

            for node_port in NODE_PORTS:
                try:
                    with grpc.insecure_channel(f"localhost:{node_port}") as channel:
                        stub = kvstore_pb2_grpc.KeyValueStoreStub(channel)
                        res = stub.UpdateKey(kvstore_pb2.UpdateRequest(key=key, value=value))
                        print(f"Updated key '{key}' on node {node_port}: {res.message}")

                        if res.message == "Updated":
                            updated_any = True
                            result += f"{res.key}: {res.value} (on port {node_port})\n"
                except grpc.RpcError as e:
                    print(f"Failed to update on node {node_port}: {e.details()}")
                except Exception as e:
                    print(f"Error connecting to node {node_port}: {e}")

            if updated_any:
                message = "Updated key on all nodes where it existed"
            else:
                message = "Key not found on any node to update"

        else:
            with grpc.insecure_channel(f"localhost:{grpc_port}") as channel:
                stub = kvstore_pb2_grpc.KeyValueStoreStub(channel)
                try:
                    if action == "get":
                        res = stub.GetKey(kvstore_pb2.KeyRequest(key=key))
                        message = res.message
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
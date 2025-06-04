from flask import Flask, request, render_template
import grpc
import kvstore_pb2
import kvstore_pb2_grpc
import sys
if len(sys.argv) > 1:
    port = sys.argv[1]
else:
    port = 50051  # default server port if not specified

app = Flask(__name__)

@app.route("/", methods=["GET", "POST"])
def index():
    message = ""
    result = ""
    if request.method == "POST":
        key = request.form["key"]
        value = request.form.get("value", "")
        action = request.form["action"]

        # Allow user to specify server port via a form field (optional)
        grpc_port = port
        if "grpc_port" in request.form and request.form["grpc_port"]:
            grpc_port = request.form["grpc_port"]

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
                elif action == "delete":
                    res = stub.DeleteKey(kvstore_pb2.KeyRequest(key=key))
                    message = res.message
                elif action == "search":
                    res = stub.SearchKey(kvstore_pb2.SearchRequest(keyword=key))
                    message = res.message
                    result = "\n".join([f"{k}: {v}" for k, v in res.results.items()])
            except grpc.RpcError as e:
                message = f"gRPC Error: {e.details()}"

    return render_template('index.html', message=message, result=result)

if __name__ == "__main__":
    app.run(port=port)

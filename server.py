import grpc
from concurrent import futures
import json
import os
import sys
import random
import kvstore_pb2
import kvstore_pb2_grpc

port = sys.argv[1] if len(sys.argv) > 1 else "50051"
all_ports = ["50051", "50052", "50053"]  # Danh sách toàn bộ node

class KeyValueStoreServicer(kvstore_pb2_grpc.KeyValueStoreServicer):

    def __init__(self):
        self.synchronize_data_on_startup()

    def synchronize_data_on_startup(self):
        local_data = self.load_data()
        other_ports = [p for p in all_ports if p != port]
        for backup_port in other_ports:
            try:
                with grpc.insecure_channel(f'localhost:{backup_port}') as channel:
                    stub = kvstore_pb2_grpc.KeyValueStoreStub(channel)
                    res = stub.SearchKey(kvstore_pb2.SearchRequest(keyword=""))
                    if res and res.results:
                        print(f"Synchronizing (merging) data from node {backup_port} to port {port}")
                        remote_data = dict(res.results)
                        merged_data = {**remote_data}
                        self.save_data(merged_data)
                        return
            except Exception as e:
                print(f"Could not sync from node {backup_port}: {e}")
        print("No available node to synchronize data from.")

    
    def get_data_file(self):
        return f"data_{port}.json"

    def load_data(self):
        try:
            path = self.get_data_file()
            if not os.path.exists(path):
                with open(path, 'w') as f:
                    f.write('{}')
            with open(path, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading data: {e}")
            return {}

    def save_data(self, data):
        try:
            path = self.get_data_file()
            with open(path, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            print(f"Error saving data: {e}")
    def UpdateKey(self, request, context):
        try:
            data = self.load_data()
            if request.key not in data:
                return kvstore_pb2.KeyResponse(key=request.key, value="", message="Not found")
            data[request.key] = request.value
            self.save_data(data)
            print(f"Updated key '{request.key}' locally on port {port}")
            put_request = kvstore_pb2.PutKeyRequest(key=request.key, value=request.value)
            self.replicate_to_other_node(put_request)

            return kvstore_pb2.KeyResponse(key=request.key, value=request.value, message="Updated")

        except Exception as e:
            print(f"Error in UpdateKey: {e}")
            return kvstore_pb2.KeyResponse(key=request.key, value="", message="Internal error")

    def GetKey(self, request, context):
        try:
            data = self.load_data()
            value = data.get(request.key, "")
            return kvstore_pb2.KeyResponse(key=request.key, value=value, message="Found" if value else "Not found")
        except Exception as e:
            print(f"Error in GetKey: {e}")
            return kvstore_pb2.KeyResponse(key=request.key, value="", message="Internal error")


    def DeleteKey(self, request, context):
        try:
            key = request.key
            deleted_locally = False
            data = self.load_data()
            if key in data:
                del data[key]
                self.save_data(data)
                deleted_locally = True
                print(f"Local delete key '{key}' on port {port}")
            else:
                print(f"Key '{key}' not found locally on port {port}")
            other_ports = [p for p in all_ports if p != port]
            for backup_port in other_ports:
                try:
                    with grpc.insecure_channel(f'localhost:{backup_port}') as channel:
                        stub = kvstore_pb2_grpc.KeyValueStoreStub(channel)
                        stub.InternalDelete(kvstore_pb2.KeyRequest(key=key))
                        print(f"Sent InternalDelete to port {backup_port}")
                except Exception as e:
                    print(f"Could not reach node {backup_port} for delete: {e}")
            msg = "Deleted" if deleted_locally else "Not found locally (still attempted remote deletes)"
            return kvstore_pb2.KeyResponse(key=key, value="", message=msg)

        except Exception as e:
            print(f"Error in DeleteKey: {e}")
            return kvstore_pb2.KeyResponse(key=request.key, value="", message="Internal error")

    def SearchKey(self, request, context):
        try:
            data = self.load_data()
            if not request.keyword.strip():
                # Nếu keyword rỗng, trả về toàn bộ data
                results = data
                msg = f"All data ({len(results)})"
            else:
                results = {k: v for k, v in data.items()
                        if request.keyword.lower() in k.lower() or request.keyword.lower() in v.lower()}
                msg = f"Found {len(results)} results"
            return kvstore_pb2.SearchResponse(results=results, message=msg)
        except Exception as e:
            print(f"Error in SearchKey: {e}")
            return kvstore_pb2.SearchResponse(results={}, message="Internal error")

    def PutKey(self, request, context):
        try:
            data = self.load_data()
            if request.key in data:
                return kvstore_pb2.KeyResponse(
                    key=request.key,
                    value=data[request.key],
                    message="Key already exists"
                )
            data[request.key] = request.value
            self.save_data(data)
            self.replicate_to_other_node(request)
            return kvstore_pb2.KeyResponse(
                key=request.key,
                value=request.value,
                message="Inserted"
            )
        except Exception as e:
            print(f"Error in PutKey: {e}")
            return kvstore_pb2.KeyResponse(key=request.key, value="", message="Internal error")

    def replicate_to_other_node(self, request):
        try:
            other_ports = [p for p in all_ports if p != port]
            if not other_ports:
                print(" No other nodes to replicate to.")
                return

            for backup_port in other_ports:
                try:
                    with grpc.insecure_channel(f'localhost:{backup_port}') as channel:
                        stub = kvstore_pb2_grpc.KeyValueStoreStub(channel)
                        stub.PutKey(kvstore_pb2.PutKeyRequest(key=request.key, value=request.value))
                        print(f"Replicated key '{request.key}' to node {backup_port}")
                except Exception as e:
                    print(f"Failed to replicate to node {backup_port}: {e}")
        except Exception as e:
            print(f"Error in replicate_to_other_node: {e}")


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    kvstore_pb2_grpc.add_KeyValueStoreServicer_to_server(KeyValueStoreServicer(), server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    print(f" Node gRPC server started on port {port}")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()

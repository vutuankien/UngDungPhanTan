import grpc
from concurrent import futures
import json
import os
import sys
import random
import kvstore_pb2
import kvstore_pb2_grpc

# Đọc cổng từ tham số dòng lệnh
port = sys.argv[1] if len(sys.argv) > 1 else "50051"
all_ports = ["50051", "50052", "50053"]  # Danh sách toàn bộ node

class KeyValueStoreServicer(kvstore_pb2_grpc.KeyValueStoreServicer):
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

    def GetKey(self, request, context):
        try:
            data = self.load_data()
            value = data.get(request.key, "")
            return kvstore_pb2.KeyResponse(key=request.key, value=value, message="Found" if value else "Not found")
        except Exception as e:
            print(f"Error in GetKey: {e}")
            return kvstore_pb2.KeyResponse(key=request.key, value="", message="Internal error")

    def UpdateKey(self, request, context):
        try:
            data = self.load_data()
            if request.key not in data:
                return kvstore_pb2.KeyResponse(key=request.key, value="", message="Not found")
            data[request.key] = request.value
            self.save_data(data)
            return kvstore_pb2.KeyResponse(key=request.key, value=request.value, message="Updated")
        except Exception as e:
            print(f"Error in UpdateKey: {e}")
            return kvstore_pb2.KeyResponse(key=request.key, value="", message="Internal error")

    def DeleteKey(self, request, context):
        try:
            key = request.key
            deleted_locally = False

            # 1. Xóa ở node hiện tại (local file)
            data = self.load_data()
            if key in data:
                del data[key]
                self.save_data(data)
                deleted_locally = True
                print(f"Local delete key '{key}' on port {port}")
            else:
                print(f"Key '{key}' not found locally on port {port}")

            # 2. Gửi InternalDelete đến các node còn lại
            other_ports = [p for p in all_ports if p != port]
            for backup_port in other_ports:
                try:
                    with grpc.insecure_channel(f'localhost:{backup_port}') as channel:
                        stub = kvstore_pb2_grpc.KeyValueStoreStub(channel)
                        stub.InternalDelete(kvstore_pb2.KeyRequest(key=key))
                        print(f"Sent InternalDelete to port {backup_port}")
                except Exception as e:
                    print(f"Could not reach node {backup_port} for delete: {e}")

            # 3. Trả kết quả
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
                # Tìm theo cả key và value (không phân biệt hoa thường)
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
                print(" No other node to replicate to.")
                return

            backup_port = random.choice(other_ports)
            try:
                with grpc.insecure_channel(f'localhost:{backup_port}') as channel:
                    stub = kvstore_pb2_grpc.KeyValueStoreStub(channel)
                    stub.PutKey(kvstore_pb2.PutKeyRequest(key=request.key, value=request.value))
                    print(f" Replicated key '{request.key}' to node {backup_port}")
            except Exception as e:
                print(f" Replication failed to node {backup_port}: {e}")
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

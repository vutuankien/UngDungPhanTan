## Cách cài đặt

### Giới thiệu

Đây là dự án mở rộng cho ứng dụng web sử dụng Python, gRPC và Flask để lưu trữ và truy xuất key-value.

### Yêu cầu

- Python >= 3.8
- pip
- Docker (nếu sử dụng dịch vụ phụ thuộc)

### Cài đặt

```bash
git clone https://github.com/vutuankien/UngDungPhanTan.git
cd grpc_kv_web_extended_v2
pip install -r requirements.txt
```

### Chạy ứng dụng

Chạy server gRPC:

```bash
python server.py 50051
python server.py 50052
python server.py 50053
```

Chạy Flask web:

```bash
python client.py
```

### Cấu trúc dự án

- `templates/` - chứa cấu hình html file giúp render giao diện
- `proto/` - Các file định nghĩa gRPC
- `README.md` - Tài liệu hướng dẫn

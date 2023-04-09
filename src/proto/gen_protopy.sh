python3 -m grpc_tools.protoc --proto_path=. --python_out=. --grpc_python_out=. KVService.proto
sed -i '' 's/import KVService_pb2 as KVService__pb2/import src.proto.KVService_pb2 as KVService__pb2/g' KVService_pb2_grpc.py

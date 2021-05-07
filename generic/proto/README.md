protoc on macos

brew install protobuf
protoc -I/opt/homebrew/Cellar/protobuf/3.15.8/include -I=./proto --descriptor_set_out=testSchemas.desc --include_imports $(find proto -iname "*.proto")
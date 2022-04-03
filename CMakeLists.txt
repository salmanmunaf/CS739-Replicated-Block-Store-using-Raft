cmake_minimum_required(VERSION 3.15)

project(blockstore)

# Protobuf
set(protobuf_MODULE_COMPATIBLE TRUE)
find_package(Protobuf CONFIG REQUIRED)
message(STATUS "Using protobuf ${protobuf_VERSION}")

# Protobuf-compiler
set(_PROTOBUF_PROTOC $<TARGET_FILE:protobuf::protoc>)

# gRPC
find_package(gRPC CONFIG REQUIRED)
message(STATUS "Using gRPC ${gRPC_VERSION}")
set(_GRPC_GRPCPP gRPC::grpc++)
set(_REFLECTION gRPC::grpc++_reflection)
set(_GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:gRPC::grpc_cpp_plugin>)

# Proto file
get_filename_component(bs_proto "proto/blockstore.proto" ABSOLUTE)
get_filename_component(bs_proto_path "${bs_proto}" PATH)

# Generated sources
set(bs_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/blockstore.pb.cc")
set(bs_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/blockstore.pb.h")
set(bs_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/blockstore.grpc.pb.cc")
set(bs_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/blockstore.grpc.pb.h")
add_custom_command(
	OUTPUT "${bs_proto_srcs}" "${bs_proto_hdrs}" "${bs_grpc_srcs}" "${bs_grpc_hdrs}"
	COMMAND ${_PROTOBUF_PROTOC}
	ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
	  --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
	  -I "${bs_proto_path}"
	  --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
	  "${bs_proto}"
	DEPENDS "${bs_proto}"
)

include_directories("include")
# Include generated *.pb.h file
include_directories("${CMAKE_CURRENT_BINARY_DIR}")

# Targets (client|server)
foreach(_target client server)
	add_executable(${_target} "${_target}/${_target}.cc"
		${bs_proto_srcs} ${bs_grpc_srcs})
	target_link_libraries(${_target}
		${_REFLECTION}
		${_GRPC_GRPCPP}
		${_PROTOBUF_LIBPROTOBUF})
endforeach()

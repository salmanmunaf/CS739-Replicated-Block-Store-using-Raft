/*
 *
 * Copyright 2015 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#include <iostream>
#include <memory>
#include <string>
#include <cerrno>
#include <cstdio>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>

#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

#include "blockstore.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using helloworld::Greeter;
using helloworld::HelloReply;
using helloworld::HelloRequest;
using helloworld::PBInterface;
using helloworld::WritePacket;
using helloworld::StatusReply;
using helloworld::Empty;

uint64_t time_since_last_response = 0;
std::string data_dir = "/";

// Logic and data behind the server's behavior.
class GreeterServiceImpl final : public Greeter::Service {
  Status SayHello(ServerContext* context, const HelloRequest* request,
                  HelloReply* reply) override {
    std::string prefix("Hello ");
    reply->set_message(prefix + request->name());
    return Status::OK;
  }
};

class PBInterfaceImpl final : public PBInterface::Service {
  Status Heartbeat(ServerContext* context, const Empty* request,
                Empty* reply) override {
    // Simply respond with a success
    return Status::OK;
  }

  Status CopyToSecondary(ServerContext* context, const WritePacket* request,
                StatusReply* reply) override {
    int fd;
    int ret;
    // For now, assume that the data is stored as <block number>.dat
    std::string data_path = data_dir + std::to_string(request->address()) + ".dat";
    std::string tmp_path = data_path + ".tmp";

    // First, write the changes to a tmp file
    fd = open(tmp_path.c_str(), O_RDWR);
    if (fd == -1) {
      goto error;
    }

    ret = write(fd, request->data().c_str(), request->data().length());
    if (ret == -1) {
      goto error;
    }

    ret = close(fd);
    if (ret == -1) {
      goto error;
    }

    // Then, atomically rename the tmp file to the target file
    ret = rename(tmp_path.c_str(), data_path.c_str());
    if (ret == -1) {
      goto error;
    }

    return Status::OK;
error:
    std::cout << "CopyToSecondary: " << strerror(errno) << std::endl;
    reply->set_status(errno);
    return Status::OK;
  }
};

void RunServer() {
  std::string server_address("0.0.0.0:50051");
  GreeterServiceImpl service;

  grpc::EnableDefaultHealthCheckService(true);
  grpc::reflection::InitProtoReflectionServerBuilderPlugin();
  ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(&service);
  // Finally assemble the server.
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
}

int main(int argc, char** argv) {
  RunServer();

  return 0;
}

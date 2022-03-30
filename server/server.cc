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
#include <mutex>

#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

#include "blockstore.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
// using helloworld::Greeter;
// using helloworld::HelloReply;
// using helloworld::HelloRequest;

#define MAX_NUM_BLOCKS (1000)
#define KB (1024)
#define BLOCK_SIZE (4*KB)
std::mutex lockArray [MAX_NUM_BLOCKS];
const std::string FILE_PATH = "blockstore.log";

// Logic and data behind the server's behavior.
class RBSImpl final : public RBS::Service {
  // Status SayHello(ServerContext* context, const HelloRequest* request,
  //                 HelloReply* reply) override {
  //   std::string prefix("Hello ");
  //   reply->set_message(prefix + request->name());
  //   return Status::OK;
  // }

  Status pread(ServerContext* context, const ReadRequest* request,
                  Response* reply) override {
    
    int fd = open(FILE_PATH, O_RDONLY);
    if (fd < 0) {
      reply->set_error_number(errno);
      printf("%s : Failed to open file\n", __func__);
      perror(strerror(errno));
      return Status::OK;
    }
    
    char* buf = new char[BLOCK_SIZE];
    int ret = pread(fd, buf, BLOCK_SIZE, request->address());
    printf("block pread: \n<%s>\n", buf);

    if (close(fd) != 0) {
      reply->set_error_number(errno);
      printf("%s : Failed to close file\n", __func__);
      perror(strerror(errno));
      return Status::OK;
    }

    reply->set_data(buf);
    reply->set_return_code(0);
    reply->error_number(0);
    return Status::OK;
  }

  Status pwrite(ServerContext* context, const ReadRequest* request,
                  Response* reply) override {
    
    int fd = open(FILE_PATH, O_WRONLY);
    if (fd < 0) {
      reply->set_error_number(errno);
      printf("%s : Failed to open file\n", __func__);
      perror(strerror(errno));
      return Status::OK;
    }

    int block_num = request->address()/BLOCK_SIZE;
    if (block_num >= MAX_NUM_BLOCKS) {
      reply->set_error_number(errno);
      printf("%s : Invalid block number\n", __func__);
      perror(strerror(errno));
      return Status::OK;
    }

    lockArray[block_num].lock();
    int res = pwrite(fd, request->data().c_str(), BLOCK_SIZE, request->address());
    fsync(fd);
    lockArray[block_num].unlock();

    if (res == -1) {
      reply->set_error_number(errno);
      printf("%s : Failed to write to file\n", __func__);
      perror(strerror(errno));
      return Status::OK;
    }

    if (close(fd) != 0) {
      reply->set_error_number(errno);
      printf("%s : Failed to close file\n", __func__);
      perror(strerror(errno));
      return Status::OK;
    }
    
    reply->set_return_code(0);
    reply->error_number(0);
    return Status::OK;
  }
};

void RunServer() {
  std::string server_address("0.0.0.0:50051");
  RBSImpl service;

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

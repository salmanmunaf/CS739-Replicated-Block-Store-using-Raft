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
#include <queue>

#include <fcntl.h>
#include <unistd.h>
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
using namespace cs739;
// using helloworld::Greeter;
// using helloworld::HelloReply;
// using helloworld::HelloRequest;

#define MAX_NUM_BLOCKS (1000)
#define KB (1024)
#define BLOCK_SIZE (4*KB)
std::mutex lockArray [MAX_NUM_BLOCKS];
const std::string FILE_PATH = "blockstore.log";

uint64_t time_since_last_response = 0;
queue<string> data_log; //queue to log data when backup fails
queue<int> block_log; //queue to log block num when backup fails

// Logic and data behind the server's behavior.
class RBSImpl final : public RBS::Service {
  // Status SayHello(ServerContext* context, const HelloRequest* request,
  //                 HelloReply* reply) override {
  //   std::string prefix("Hello ");
  //   reply->set_message(prefix + request->name());
  //   return Status::OK;
  // }

  Status Read(ServerContext* context, const ReadRequest* request,
                  Response* reply) override {
    
    std::cout << "Data to read at offset: " << request->address() << std::endl;
    int fd = open(FILE_PATH.c_str(), O_RDONLY);
    if (fd < 0) {
      reply->set_error_code(errno);
      printf("%s : Failed to open file\n", __func__);
      perror(strerror(errno));
      return Status::OK;
    }
    
    char* buf = new char[BLOCK_SIZE];
    int ret = pread(fd, buf, BLOCK_SIZE, request->address());
    printf("block pread: \n<%s>\n", buf);

    if (close(fd) != 0) {
      reply->set_error_code(errno);
      printf("%s : Failed to close file\n", __func__);
      perror(strerror(errno));
      return Status::OK;
    }

    reply->set_data(buf);
    reply->set_return_code(0);
    reply->set_error_code(0);
    return Status::OK;
  }

  Status Write(ServerContext* context, const WriteRequest* request,
                  Response* reply) override {
    
    std::cout << "Data to write: " << request->data().c_str() << std::endl;
    int fd = open(FILE_PATH.c_str(), O_WRONLY);
    if (fd < 0) {
      reply->set_error_code(errno);
      printf("%s : Failed to open file\n", __func__);
      perror(strerror(errno));
      return Status::OK;
    }

    int block_num = request->address()/BLOCK_SIZE;
    if (block_num >= MAX_NUM_BLOCKS) {
      reply->set_error_code(errno);
      printf("%s : Invalid block number\n", __func__);
      perror(strerror(errno));
      return Status::OK;
    }

    lockArray[block_num].lock();
    int res = pwrite(fd, request->data().c_str(), BLOCK_SIZE, request->address());
    fsync(fd);
    lockArray[block_num].unlock();

    if (res == -1) {
      reply->set_error_code(errno);
      printf("%s : Failed to write to file\n", __func__);
      perror(strerror(errno));
      return Status::OK;
    }

    if (close(fd) != 0) {
      reply->set_error_code(errno);
      printf("%s : Failed to close file\n", __func__);
      perror(strerror(errno));
      return Status::OK;
    }
    
    reply->set_return_code(0);
    reply->set_error_code(0);
    return Status::OK;
  }
};

class PBInterfaceImpl final : public PBInterface::Service {
  Status Heartbeat(ServerContext* context, const EmptyPacket* request,
                EmptyPacket* reply) override {
    // Simply respond with a success
    return Status::OK;
  }

  /********************************************************************************/

  //if no heartbeat response from the backup, primary starts logging client requests
  //even when the backup comes up and the log transfer to backup has started, new requests from clients should still be
  //put in the back of the queue unless backup catches up with the primary
  void request_logger(int block_num, string data)
  {
	  data_log.push(data);
	  block_log.push(block_num);
	  return;
  }

  //when the backup comes back again, the primary transfers the log to backup
  int LogTransfer()
  {
	  ClientContext context;
	  TransferRequest request;
	  TransferResponse response;
	  Status status;

	  while (data_log.empty() == 0) {
		  request.set_blockNum(block_log.front());
		  request.set_data(data_log.front());
		  status = stub_->LogTransfer(&context, request, &response);

		  if (status.ok()) {
			  if (response.return_code() == 1) { //FIXME: check this condition if needed or not!
				  block_log.pop();
				  data_log.pop();
				  //return response.return_code();
			  }
			  //return response.error_code(); //FIXME: check which error codes to return
		  } else {
			  return -1;
		  }
	  }
	  return 0;
  }

  /********************************************************************************/

  Status CopyToSecondary(ServerContext* context, const WriteRequest* request,
                Response* reply) override {
    int fd;
    int ret;
    // For now, assume that the data is stored as <block number>.dat
    std::string data_path = FILE_PATH + std::to_string(request->address());
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
    reply->set_error_code(errno);
    return Status::OK;
  }

private: //FIXME: might have to remove/modify
    std::unique_ptr<RBS::Stub> stub_;
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

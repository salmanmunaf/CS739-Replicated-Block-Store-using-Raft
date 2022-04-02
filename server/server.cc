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

using grpc::ClientContext;
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
std::mutex queue_lock; //lock to ensure atomicity in queue operations
const std::string data_dir = "./";

uint64_t time_since_last_response = 0;
std::queue<std::string> data_log; //queue to log data to send to backup
std::queue<uint64_t> address_log; //queue to log address to send to backup

bool block_aligned(uint64_t address) {
    return address & (BLOCK_SIZE - 1);
}

int read_block_data(std::string block_file, char *buf, size_t count, off_t offset) {
  int fd;
  int ret;

  fd = open(block_file.c_str(), O_RDONLY);
  if (fd < 0) {
    goto err;
  }

  ret = pread(fd, buf, count, offset);
  if (ret < 0) {
    goto err;
  }

  if (close(fd) != 0) {
    goto err;
  }

  return 0;
err:
  printf("%s : Failed to open file %s\n", __func__, block_file.c_str());
  return -1;
}

int write_block_data(std::string block_file, const char *buf, size_t count, off_t offset) {
  int fd;
  int ret;

  fd = open(block_file.c_str(), O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR);
  if (fd < 0) {
    goto err;
  }

  ret = pwrite(fd, buf, count, offset);
  if (ret < 0) {
    goto err;
  }

  if (fsync(fd) != 0) {
    goto err;
  }

  if (close(fd) != 0) {
    goto err;
  }

  return 0;
err:
    printf("%s : Failed to open file %s\n", __func__, block_file.c_str());
    return -1;
}

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
    char* buf = new char[BLOCK_SIZE];
    uint64_t address = request->address();
    uint64_t block = address / BLOCK_SIZE;
    uint64_t offset = address % BLOCK_SIZE;
    uint64_t first_block_read_size = BLOCK_SIZE - offset;
    uint64_t second_block_read_size = offset;
    std::string data1_path = data_dir + "/" + std::to_string(block) + ".dat";
    std::string data2_path = data_dir + "/" + std::to_string(block + 1) + ".dat";
    int fd;
    int ret;

    // Read the data from the first block
    ret = read_block_data(data1_path, buf, first_block_read_size, offset);
    if (ret < 0) {
      goto err;
    }

    // If needed, read data from the second block
    if (second_block_read_size > 0) {
      ret = read_block_data(data2_path, &buf[first_block_read_size],
                             second_block_read_size, 0);
      if (ret < 0) {
        goto err;
      }
    }

    reply->set_data(buf);
    reply->set_return_code(1);
    reply->set_error_code(0);
    return Status::OK;

err:
    printf("Read %lx failed\n", address);
    reply->set_return_code(-1);
    reply->set_error_code(errno);
    perror(strerror(errno));
    return Status::OK;
  }

  Status Write(ServerContext* context, const WriteRequest* request,
                  Response* reply) override {
    
    std::cout << "Data to write: " << request->data().c_str() << std::endl;
    char* undo_buf = new char[2*BLOCK_SIZE];
    const char* write_buf = request->data().c_str();
    uint64_t address = request->address();
    uint64_t block = address / BLOCK_SIZE;
    uint64_t offset = address % BLOCK_SIZE;
    uint64_t first_block_write_size = BLOCK_SIZE - offset;
    uint64_t second_block_write_size = offset;
    uint64_t undo_write_size = second_block_write_size > 0 ? 2 * BLOCK_SIZE : BLOCK_SIZE;
    std::string data1_path = data_dir + "/" + std::to_string(block) + ".dat";
    std::string data2_path = data_dir + "/" + std::to_string(block + 1) + ".dat";
    std::string undo_path = data_dir + "/" + std::to_string(block) + ".undo";
    int fd;
    int ret;


    lockArray[block].lock();
    if (second_block_write_size > 0) {
      lockArray[block + 1].lock();
    }

    // Read the data from the first block
    ret = read_block_data(data1_path, undo_buf, BLOCK_SIZE, 0);
    if (ret < 0) {
      goto err;
    }

    // If needed, read data from the second block
    if (second_block_write_size > 0) {
      ret = read_block_data(data2_path, &undo_buf[BLOCK_SIZE],
                             BLOCK_SIZE, 0);
      if (ret < 0) {
        goto err;
      }
    }

    // Write the old block data into the undo file
    fd = open(undo_path.c_str(), O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR);
    if (fd < 0) {
      goto err;
    }

    ret = write(fd, undo_buf, undo_write_size);
    if (ret < 0) {
      goto err;
    }

    ret = fsync(fd);
    if (fd < 0) {
      goto err;
    }
    ret = close(fd);
    if (fd < 0) {
      goto err;
    }

    // Write the new data into the data files
    ret = write_block_data(data1_path, write_buf, first_block_write_size, offset);
    if (ret < 0) {
      goto err;
    }

    if (second_block_write_size > 0) {
      ret = write_block_data(data2_path, &write_buf[first_block_write_size],
                              second_block_write_size, 0);
      if (ret < 0) {
        goto err;
      }
      lockArray[block + 1].unlock();
    }
    lockArray[block].unlock();

    // write_block_data fsyncs the data, so the new data should be persisted,
    // so we can delete the undo file
    ret = unlink(undo_path.c_str());
    if (ret < 0) {
      goto err;
    }

    reply->set_return_code(1);
    reply->set_error_code(0);
    return Status::OK;
err:
    if (second_block_write_size > 0) {
      lockArray[block + 1].unlock();
    }
    lockArray[block].unlock();

    printf("Write %lx failed\n", address);
    reply->set_return_code(-1);
    reply->set_error_code(errno);
    perror(strerror(errno));
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
  void request_logger(uint64_t address, std::string data)
  {
	  queue_lock.lock();
	  data_log.push(data);
	  address_log.push(address);
	  queue_lock.unlock();
	  return;
  }

  //when the backup comes back again, the primary transfers the log to backup
  int LogTransfer() {
	  ClientContext context;
	  WriteRequest request;
	  Response response;
	  Status status;

	  while (data_log.empty() == 0) {
		  request.set_address(address_log.front());
		  request.set_data(data_log.front());
		  status = stub_->Write(&context, request, &response);

		  if (status.ok()) {
			  if (response.return_code() == 0) { //FIXME: check this condition if needed or not!
				  queue_lock.lock();
				  address_log.pop();
				  data_log.pop();
				  queue_lock.unlock();
				  //return response.return_code();
			  } else {
				  return response.error_code(); //FIXME: check which error codes to return
			  }
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
    std::string data_path = data_dir + "/" + std::to_string(request->address()) + ".dat";
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

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

using grpc::Channel;
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
const std::string FILE_PATH = "blockstore.log";

uint64_t time_since_last_response = 0;
std::queue<std::string> data_log; //queue to log data to send to backup
std::queue<uint64_t> address_log; //queue to log address to send to backup
bool is_primary = false;
uint64_t last_comm_time = 0;

bool is_block_aligned(uint64_t addr) {
  return (addr & (BLOCK_SIZE - 1)) == 0;
}

int read_block_data(std::string block_file, char *buf, off_t offset) {
  int fd;
  int ret;

  fd = open(block_file.c_str(), O_RDONLY);
  if (fd < 0) {
    goto err;
  }

  ret = pread(fd, buf, BLOCK_SIZE, offset);
  if (ret < 0) {
    goto err;
  }

  if (close(fd) != 0) {
    goto err;
  }

  return ret;
err:
  printf("%s : Failed to read file %s\n", __func__, block_file.c_str());
  return -1;
}

int write_block_data(std::string block_file, const char *buf, off_t offset) {
  int fd;
  int ret;

  fd = open(block_file.c_str(), O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR);
  if (fd < 0) {
    goto err;
  }

  ret = pwrite(fd, buf, BLOCK_SIZE, offset);
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

int write_undo_file(std::string undo_path, off_t address) {
  char* undo_buf = new char[BLOCK_SIZE];
  int undo_write_size;
  int fd;
  int ret;

  // Read the data that will be overwritten
  undo_write_size = read_block_data(FILE_PATH, undo_buf, address);
  if (undo_write_size < 0) {
    // It's fine if the file doesn't exist, that's fine, it just means that
    // this block hasn't been allocated yet.
    if (errno == ENOENT) {
      undo_write_size = 0;
    } else {
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

  delete undo_buf;
  return 0;
err:
  delete undo_buf;
  printf("%s : Failed to create undo file %s\n", __func__, undo_path.c_str());
  return -1;
}

Status do_atomic_write(const WriteRequest* request, Response *reply) {
  const char* write_buf = request->data().c_str();
  uint64_t address = request->address();
  uint64_t block = address / BLOCK_SIZE;
  std::string undo_path = FILE_PATH + std::to_string(address) + ".undo";
  int fd;
  int ret;

  lockArray[block].lock();
  if (!is_block_aligned(address)) {
    lockArray[block + 1].lock();
  }

  ret = write_undo_file(undo_path, address);
  if (ret < 0) {
    goto err;
  }

  // Write the new data into the data files
  ret = write_block_data(FILE_PATH, write_buf, address);
  if (ret < 0) {
    goto err;
  }

  // write_block_data fsyncs the data, so the new data should be persisted,
  // so we can delete the undo file
  ret = unlink(undo_path.c_str());
  if (ret < 0) {
    goto err;
  }

  if (!is_block_aligned(address)) {
    lockArray[block + 1].unlock();
  }
  lockArray[block].unlock();

  reply->set_return_code(1);
  reply->set_error_code(0);
  return Status::OK;
err:
  if (!is_block_aligned(address)) {
    lockArray[block + 1].unlock();
  }
  lockArray[block].unlock();

  printf("Write %lx failed\n", address);
  reply->set_return_code(-1);
  reply->set_error_code(errno);
  perror(strerror(errno));
  return Status::OK;
}

class PBInterfaceClient {
  public:
    PBInterfaceClient(std::shared_ptr<Channel> channel)
      : stub_(PBInterface::NewStub(channel)) {}

    int Heartbeat() {
      ClientContext context;
      EmptyPacket request, response;

      Status status = stub_->Heartbeat(&context, request, &response);

      if (status.ok()) {
        return 0;
      } else {
        return -1;
      }
    }

    int CopyToSecondary(uint64_t address, std::string data) {
      ClientContext context;
      WriteRequest request;
      Response response;

      request.set_address(address);
      request.set_data(data);

      Status status = stub_->CopyToSecondary(&context, request, &response);

      if (status.ok()) {
        if(response.return_code() == 1) {
            return response.return_code();
        }
        return -response.error_code();
      } else {
        return -1;
      }
    }
  private:
    std::unique_ptr<PBInterface::Stub> stub_;
};

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
    int ret;

    // Read the data from the first block
    ret = read_block_data(FILE_PATH, buf, address);
    if (ret < 0) {
      goto err;
    }

    reply->set_data(std::string(buf, BLOCK_SIZE));
    reply->set_return_code(1);
    reply->set_error_code(0);
    delete buf;
    return Status::OK;

err:
    delete buf;
    printf("Read %lx failed\n", address);
    reply->set_return_code(-1);
    reply->set_error_code(errno);
    perror(strerror(errno));
    return Status::OK;
  }

  Status Write(ServerContext* context, const WriteRequest* request,
                  Response* reply) override {
    std::cout << "Data to write: " << request->data().c_str() << std::endl;

    // If we are the primary, we better forward the data to the backup
    if (is_primary) {
      int ret = pb_client->CopyToSecondary(request->address(), request->data());
      if (ret < 0) {
        reply->set_return_code(-1);
        reply->set_error_code(-ret);
        return Status::OK;
      }
    }

    return do_atomic_write(request, reply);
  }
public:
    PBInterfaceClient *pb_client;
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
    int ret;

	  while (data_log.empty() == 0) {
		  ret = pb_client->CopyToSecondary(address_log.front(), data_log.front());

		  if (ret >= 0) {
				queue_lock.lock();
				address_log.pop();
				data_log.pop();
				queue_lock.unlock();
		  } else {
			  return ret;
		  }
	  }
	  return 0;
  }

  /********************************************************************************/

  Status CopyToSecondary(ServerContext* context, const WriteRequest* request,
                Response* reply) override {
    std::cout << "Data from primary: " << request->data() << std::endl;
    return do_atomic_write(request, reply);
  }

public:
    PBInterfaceClient *pb_client;
};

void RunServer(std::string listen_port, std::string other_server) {
  std::string server_address("0.0.0.0:" + listen_port);
  PBInterfaceImpl pb_service;
  PBInterfaceClient pb_client(
    grpc::CreateChannel(other_server, grpc::InsecureChannelCredentials()));
  RBSImpl service;

  pb_service.pb_client = &pb_client;
  service.pb_client = &pb_client;

  grpc::EnableDefaultHealthCheckService(true);
  grpc::reflection::InitProtoReflectionServerBuilderPlugin();
  ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(&service);
  builder.RegisterService(&pb_service);
  // Finally assemble the server.
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
}

int main(int argc, char** argv) {
  if (argc < 3) {
    printf("Usage: ./server <listen_port> <server_ip:port> [is_primary]\n");
    return -1;
  }
  std::string listen_port = argv[1];
  std::string other_server = argv[2];
  if (argc >= 4)
    is_primary = true;

  RunServer(listen_port, other_server);

  return 0;
}

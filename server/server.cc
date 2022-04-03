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
#include <atomic>
#include <chrono>
#include <thread>
#include <filesystem>

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

#include "blockstore.h"
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
std::atomic<bool> is_primary(false);
std::atomic<bool> backup_up(false);
std::atomic<uint64_t> last_comm_time(0);

bool is_block_aligned(uint64_t addr) {
  return (addr & (BLOCK_SIZE - 1)) == 0;
}

uint64_t cur_time() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
    std::chrono::system_clock::now().time_since_epoch()
  ).count();
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
  if (ret < 0) {
    goto err;
  }
  ret = close(fd);
  if (ret < 0) {
    goto err;
  }

  delete undo_buf;
  return 0;
err:
  delete undo_buf;
  printf("%s : Failed to create undo file %s\n", __func__, undo_path.c_str());
  return -1;
}

uint64_t find_address_from_path(std::string undo_path) {
  // copy the path because the delimiting code will modify the string
  std::string s(undo_path);
  std::string token;
  int pos;

  // The address is before the last "."
  while ((pos = s.find(".")) != std::string::npos) {
    token = s.substr(0, pos);
    s.erase(0, pos + 1);
  }

  return std::stoi(token);
}

int recover_undo_file(std::string undo_path) {
  char* undo_buf = new char[BLOCK_SIZE];
  uint64_t address;
  int undo_write_size;
  int fd;
  int ret;

  address = find_address_from_path(undo_path);

  // Read the data that will be overwritten
  undo_write_size = read_block_data(undo_path, undo_buf, 0);
  if (undo_write_size < 0) {
    goto err;
  }

  // Write the recovered data into the correct spot in our data store
  fd = open(FILE_PATH.c_str(), O_WRONLY);
  if (fd < 0) {
    goto err;
  }

  ret = pwrite(fd, undo_buf, undo_write_size, address);
  if (ret < 0) {
    goto err;
  }

  // If the undo file was smaller than a block, that must mean that
  // the write being undone extended the file, so we should truncate it
  // back
  if (undo_write_size != BLOCK_SIZE) {
    ret = ftruncate(fd, address + undo_write_size);
    if (ret < 0) {
      goto err;
    }
  }

  ret = fsync(fd);
  if (ret < 0) {
    goto err;
  }

  ret = close(fd);
  if (ret < 0) {
    goto err;
  }

  unlink(undo_path.c_str());

  delete undo_buf;
  return 0;
err:
  delete undo_buf;
  printf("%s : Failed to recover undo file %s\n", __func__, undo_path.c_str());
  return -1;
}

Status do_atomic_write(const WriteRequest* request, Response *reply) {
  const char* write_buf = request->data().c_str();
  uint64_t address = request->address();
  uint64_t block = address / BLOCK_SIZE;
  uint64_t write_hash = std::hash<std::string>{}(request->data());
  std::string undo_path = FILE_PATH + "." + std::to_string(address) + ".undo";
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

  // To test the the undo logging stuff, if we see the data to write has the 
  // hash of the signal value (which is "hello_world" followed by a blocks worth of 0)
  // write some "corrupted" data and "crash" the server
  if (write_hash == 13494594211096014138ull && is_primary.load()) {
    char corruption[BLOCK_SIZE] = "corrupted_data_is_here";
    int len = strlen(corruption);
    memset(&corruption[BLOCK_SIZE], ' ', BLOCK_SIZE - len);
    write_block_data(FILE_PATH, corruption, address);
    exit(0);
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

  reply->set_return_code(BLOCKSTORE_SUCCESS);
  reply->set_error_code(0);
  return Status::OK;
err:
  if (!is_block_aligned(address)) {
    lockArray[block + 1].unlock();
  }
  lockArray[block].unlock();

  printf("Write %lx failed\n", address);
  reply->set_return_code(BLOCKSTORE_FAIL);
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
  PBInterfaceClient pb_client;


  Status Read(ServerContext* context, const ReadRequest* request,
                  Response* reply) override {
    std::cout << "Data to read at offset: " << request->address() << std::endl;
    char* buf;
    uint64_t address = request->address();
    int ret;

    // Return without doing anything if we are not the primary
    if (!is_primary.load()) {
      reply->set_return_code(BLOCKSTORE_NOT_PRIM);
      return Status::OK;
    }

    buf = new char[BLOCK_SIZE];

    // Read the data from the first block
    ret = read_block_data(FILE_PATH, buf, address);
    if (ret < 0) {
      goto err;
    }

    reply->set_data(std::string(buf, BLOCK_SIZE));
    reply->set_return_code(BLOCKSTORE_SUCCESS);
    reply->set_error_code(0);
    delete buf;
    return Status::OK;

err:
    delete buf;
    printf("Read %lx failed\n", address);
    reply->set_return_code(BLOCKSTORE_FAIL);
    reply->set_error_code(-errno);
    perror(strerror(errno));
    return Status::OK;
  }

  Status Write(ServerContext* context, const WriteRequest* request,
                  Response* reply) override {
    Status status;
    int ret;
    std::cout << "Data to write: " << request->data().c_str() << std::endl;

    // Make sure we are the primary
    if (!is_primary.load()) {
      reply->set_return_code(BLOCKSTORE_NOT_PRIM);
      return Status::OK;
    }

    status = do_atomic_write(request, reply);
    if (!status.ok()) {
      return status;
    }

    // If we think the backup is up, we better forward the data to it
    if (backup_up.load()) {
      ret = pb_client.CopyToSecondary(request->address(), request->data());
      // If it failed, add the write to the queue and move on
      if (ret < 0) {
        queue_lock.lock();
        backup_up = false;
        data_log.push(request->data());
        address_log.push(request->address());
        queue_lock.unlock();
      }
    } else {
      queue_lock.lock();
      data_log.push(request->data());
      address_log.push(request->address());
      queue_lock.unlock();
    }

    return Status::OK;
  }
public:
  RBSImpl(std::string other_server)
    : pb_client(grpc::CreateChannel(other_server, grpc::InsecureChannelCredentials()))
  {}

};

class PBInterfaceImpl final : public PBInterface::Service {
  PBInterfaceClient pb_client;

  Status Heartbeat(ServerContext* context, const EmptyPacket* request,
                EmptyPacket* reply) override {
    std::cout << "Received Heartbeat\n";

    // Update when we last heard from the server
    last_comm_time = cur_time();

    // Simply respond with a success
    return Status::OK;
  }

  /********************************************************************************/

  Status CopyToSecondary(ServerContext* context, const WriteRequest* request,
                Response* reply) override {
    std::cout << "Data from primary: " << request->data() << std::endl;

    // Update when we last heard from the primary
    last_comm_time = cur_time();

    // Actually do the write
    return do_atomic_write(request, reply);
  }

public:
  PBInterfaceImpl(std::string other_server)
    : pb_client(grpc::CreateChannel(other_server, grpc::InsecureChannelCredentials()))
  {}
};

void RunServer(std::string listen_port, std::string other_server) {
  std::string server_address("0.0.0.0:" + listen_port);
  PBInterfaceImpl pb_service(other_server);
  RBSImpl service(other_server);

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

//when the backup comes back again, the primary transfers the log to backup
int LogTransfer(PBInterfaceClient &pb_client) {
  int ret;

  while (data_log.empty() == 0) {
	  ret = pb_client.CopyToSecondary(address_log.front(), data_log.front());

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

void handle_heartbeats(std::string other_server) {
  PBInterfaceClient pb_client(
      grpc::CreateChannel(other_server, grpc::InsecureChannelCredentials())
  );
  const int PRIMARY_TIMEOUT = 5000;
  int ret;

  while(true) {
    // If we are the primary, give a heartbeat to the backup
    if (is_primary.load()) {
      ret = pb_client.Heartbeat();

      if (ret != 0) {
        // If the heart beat is not responded to, assume the backup is down
        backup_up = false;
      } else {
        // If this is the first successful heartbeat since the backup went down,
        // send out the queued changes
        if (!backup_up) {
          ret = LogTransfer(pb_client);
          // Only say the backup is up and running if our entire queue was
          // emptied successfully.
          if (ret == 0) {
            backup_up = true;
          }
        }
      }
    } else {
      // If we haven't heard from the server in over 5 seconds, assume
      // the primary has failed and take over
      uint64_t last_time = last_comm_time.load();
      uint64_t cur = cur_time();

      if (cur_time() > last_time && cur_time() - last_comm_time.load() >= PRIMARY_TIMEOUT) {
        std::cout << "Becoming the primary!\n";
        is_primary = true;
        backup_up = false;
      }
    }
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
}

int main(int argc, char** argv) {
  if (argc < 3) {
    std::cout << "Usage: ./server <listen_port> <server_ip:port> [is_primary]\n";;
    return -1;
  }
  std::string listen_port = argv[1];
  std::string other_server = argv[2];
  if (argc >= 4)
    is_primary = true;

  if (is_primary)
    std::cout << "Running as primary!\n";
  else
    std::cout << "Running as backup!\n";

  for (const auto & entry : std::filesystem::directory_iterator("./")) {
    if (std::filesystem::is_regular_file(entry) && entry.path().extension() == ".undo") {
      recover_undo_file(entry.path().filename());
    }
  }

  // On startup, give the primary an extra few seconds to send a heartbeat
  last_comm_time = cur_time() + 10000;

  std::thread server_thread(RunServer, listen_port, other_server);

  handle_heartbeats(other_server);

  return 0;
}

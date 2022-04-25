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
#include <shared_mutex>
#include <queue>
#include <atomic>
#include <chrono>
#include <thread>

#include <fcntl.h>
#include <unistd.h>
#include <cerrno>
#include <cstdio>
#include <fcntl.h>
#include <dirent.h>
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

#define MAX_NUM_BLOCKS (1000)
#define KB (1024)
#define BLOCK_SIZE (4*KB)
#define HAVENT_VOTED (-1)

std::shared_mutex lockArray [MAX_NUM_BLOCKS];
std::mutex queue_lock; //lock to ensure atomicity in queue operations
const std::string FILE_PATH = "blockstore.log";

enum server_state {
    STATE_LEADER,
    STATE_FOLLOWER,
    STATE_CANDIDATE
};

uint64_t time_since_last_response = 0;
std::queue<std::string> data_log; //queue to log data to send to backup
std::queue<uint64_t> address_log; //queue to log address to send to backup
std::atomic<bool> is_primary(false);
std::atomic<uint64_t> last_comm_time(0);
std::atomic<uint64_t> curTerm(0);
std::atomic<int64_t> voted_for(HAVENT_VOTED);
enum server_state state;

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
  std::string undo_tmp_path = undo_path + ".tmp";
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

  // Write the old block data into the temporary undo file
  fd = open(undo_tmp_path.c_str(), O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR);
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

  // Once we know the undo data has been written correctly, we can
  // rename it to be the actual undo file.
  ret = rename(undo_tmp_path.c_str(), undo_path.c_str());
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

Status do_atomic_write(const WriteRequest* request, Response *reply, std::string undo_path) {
  const char* write_buf = request->data().c_str();
  uint64_t address = request->address();
  uint64_t write_hash = std::hash<std::string>{}(request->data());
  int fd;
  int ret;

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

  reply->set_return_code(BLOCKSTORE_SUCCESS);
  reply->set_error_code(0);
  return Status::OK;
err:
  printf("Write %lx failed\n", address);
  reply->set_return_code(BLOCKSTORE_FAIL);
  reply->set_error_code(-errno);
  perror(strerror(errno));
  return Status::OK;
}

// Class is used to send RPCs to other clients
class RaftInterfaceClient {
  public:
    RaftInterfaceClient(std::shared_ptr<Channel> channel)
      : stub_(RaftInterface::NewStub(channel)) {}

    int RequestVote() {
      return 0;
    }

    int Heartbeat() {
      return 0;
    }

  private:
    std::unique_ptr<RaftInterface::Stub> stub_;
};

// Logic and data behind the server's behavior.
class RBSImpl final : public RBS::Service {
  std::vector<RaftInterfaceClient> servers;

  Status Read(ServerContext* context, const ReadRequest* request,
                  Response* reply) override {
    std::cout << "Data to read at offset: " << request->address() << std::endl;
    char* buf;
    uint64_t address = request->address();
    uint64_t block = address / BLOCK_SIZE;
    int ret;

    // Return without doing anything if we are not the primary
    if (!is_primary.load()) {
      reply->set_return_code(BLOCKSTORE_NOT_PRIM);
      return Status::OK;
    }

    buf = new char[BLOCK_SIZE];

    // Read the data from the first block
    lockArray[block].lock_shared();
    if (!is_block_aligned(address)) {
      lockArray[block + 1].lock_shared();
    }

    ret = read_block_data(FILE_PATH, buf, address);

    if (!is_block_aligned(address)) {
      lockArray[block + 1].unlock_shared();
    }
    lockArray[block].unlock_shared();

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
    uint64_t address = request->address();
    uint64_t block = address / BLOCK_SIZE;
    std::string undo_path = FILE_PATH + "." + std::to_string(address) + ".undo";
    std::cout << "Data to write: " << request->data().c_str() << std::endl;

    // Make sure we are the primary
    if (!is_primary.load()) {
      reply->set_return_code(BLOCKSTORE_NOT_PRIM);
      return Status::OK;
    }

    lockArray[block].lock();
    if (!is_block_aligned(address)) {
      lockArray[block + 1].lock();
    }

    status = do_atomic_write(request, reply, undo_path);
    if (!status.ok()) {
      goto unlock;
    }

    // If we think the backup is up, we better forward the data to it
    /*
     * TODO: Write replication strategy
     */

    // The data should be persisted locally and is either persisted on the primary
    // or we have added it to the queue to do so, so we can delete the undo file
    unlink(undo_path.c_str());

unlock:
    if (!is_block_aligned(address)) {
      lockArray[block + 1].unlock();
    }
    lockArray[block].unlock();

    return status;
  }
public:
  RBSImpl(std::vector<std::string> other_servers)
  {
    for (auto it = other_servers.begin(); it != other_servers.end(); it++) {
      servers.push_back(grpc::CreateChannel(*it, grpc::InsecureChannelCredentials()));
    }
  }

};

// Class that handles incoming RPCs
class RaftInterfaceImpl final : public RaftInterface::Service {
  Status RequestVote(ServerContext *context, const RequestVoteRequest *request,
                RequestVoteResponse *reply) override {
    static std::mutex vote_lock;
    uint64_t requestTerm = request->term();
    uint64_t candidateId = request->candidate_id();

    // Use a lock to make sure we don't respond to two simultaneous vote requests
    vote_lock.lock();

    if (requestTerm > curTerm) {
      state = STATE_FOLLOWER;
      curTerm = requestTerm;
      voted_for = candidateId;
      reply->set_term(requestTerm);
      reply->set_vote_granted(true);
    } else if (state == STATE_LEADER) {
      // From the last if case, we know that requestTerm <= curTerm
      // and we're the leader, so let the requester know we're the leader
      reply->set_term(curTerm);
      reply->set_vote_granted(false);
    } else {
      if (requestTerm < curTerm || voted_for != HAVENT_VOTED) {
        reply->set_term(curTerm);
        reply->set_vote_granted(false);
      } else {
        // If we're a candidate, this sets us back to being a follower
        state = STATE_FOLLOWER;

        curTerm = requestTerm;
        voted_for = candidateId;
        reply->set_term(requestTerm);
        reply->set_vote_granted(true);
      }
    }

    vote_lock.unlock();
    return Status::OK;
  }

  Status AppendEntries(ServerContext *context, const AppendEntriesRequest * request,
                AppendEntriesResponse *reply) override {

      uint64_t requestTerm = request->term();

      if(state == STATE_LEADER) {
        if(requestTerm > curTerm) {
          state = STATE_FOLLOWER;
          // reply->set_success(true);
        }
        reply->set_success(false);

      } else {

        // check on valid term
        if(requestTerm < curTerm) {
          reply->set_term(curTerm);
          reply->set_success(false);
        }

        state = STATE_FOLLOWER;
        curTerm = requestTerm;

        reply->set_term(curTerm);
        reply->set_success(true);

      }

    return Status::OK;
  }
};

void RunServer(std::string listen_port, std::vector<std::string> other_servers) {
  std::string server_address("0.0.0.0:" + listen_port);
  RaftInterfaceImpl raft_service;
  RBSImpl service(other_servers);

  grpc::EnableDefaultHealthCheckService(true);
  grpc::reflection::InitProtoReflectionServerBuilderPlugin();
  ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(&service);
  builder.RegisterService(&raft_service);
  // Finally assemble the server.
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
}

void handle_heartbeats(std::vector<std::string> other_servers) {
  /*
   * TODO: Send out heartbeats if necessary
   */
}

int main(int argc, char** argv) {
  if (argc < 3) {
    std::cout << "Usage: ./server <listen_port> <server_ip:port> [is_primary]\n";;
    return -1;
  }
  std::string listen_port = argv[1];
  std::string other_server = argv[2];
  std::vector<std::string> other_servers;
  other_servers.push_back(other_server);

  if (argc >= 4)
    is_primary = true;

  if (is_primary)
    std::cout << "Running as primary!\n";
  else
    std::cout << "Running as backup!\n";

  DIR *dir;
  struct dirent *entry;

  dir = opendir("./");
  while (entry = readdir(dir)) {
    char *filename = entry->d_name;
    int name_size = strlen(filename);
    int extension_index;

    if (name_size >= 5) {
      extension_index = name_size - 5;
    } else {
      extension_index = 0;
    }
    if (entry->d_type == DT_REG && strcmp(&filename[extension_index], ".undo") == 0) {
      recover_undo_file(std::string(filename));
    }
  }

  // On startup, give the primary an extra few seconds to send a heartbeat
  last_comm_time = cur_time() + 10000;

  std::thread server_thread(RunServer, listen_port, other_servers);

  handle_heartbeats(other_servers);

  return 0;
}

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
#include <fstream>
#include <algorithm>
#include <random>

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
//Entry size = sizeof(term + address + block data)
#define ENTRY_SIZE (8 + 8 + BLOCK_SIZE)
//Log intro size = sizeof(cur_term + voted_for)
#define LOG_INTRO_SIZE (8 + 8)
#define HAVENT_VOTED (-1)

struct LogEntry {
    int64_t term;
    int64_t address;
    char data[BLOCK_SIZE];
};

std::shared_mutex lockArray [MAX_NUM_BLOCKS];
std::mutex log_lock; //lock to ensure atomicity in queue operations
//lock to ensure we don't have data races with updating the term
//and who we voted for
std::mutex vote_lock;
const std::string FILE_PATH = "blockstore.log";
// This file stores the current term, voted for, and then the entries
const std::string RAFT_LOG_FILE = "raft.log";

enum server_state {
    STATE_LEADER,
    STATE_FOLLOWER,
    STATE_CANDIDATE
};

std::vector<struct LogEntry> raft_log;
std::vector<int64_t> nextIndex;
std::vector<int64_t> matchIndex;
std::atomic<int64_t> last_comm_time(0);
std::atomic<int64_t> curTerm(0);
std::atomic<int64_t> voted_for(HAVENT_VOTED);
std::atomic<int64_t> commit_index(-1);
std::atomic<int64_t> last_applied(-1);
int64_t current_leader_id = 0;
int64_t server_id;
int64_t num_servers;
enum server_state state;

bool is_block_aligned(int64_t addr) {
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

int64_t get_int64_from_buf(char *buf) {
    uint64_t num = 0;
    num |= (uint64_t) buf[7];
    num |= (uint64_t) buf[6] << 8;
    num |= (uint64_t) buf[5] << 16;
    num |= (uint64_t) buf[4] << 24;
    num |= (uint64_t) buf[3] << 32;
    num |= (uint64_t) buf[2] << 40;
    num |= (uint64_t) buf[1] << 48;
    num |= (uint64_t) buf[0] << 56;

    return (int64_t)num;
}

void put_int64_to_buf(char *buf, int64_t n) {
    // Convert the input to be unsigned to prevent any sign extension shenanigans
    uint64_t num = (uint64_t)n;
    buf[7] = num & 0xFF;
    buf[6] = (num >> 8) & 0xFF;
    buf[5] = (num >> 16) & 0xFF;
    buf[4] = (num >> 24) & 0xFF;
    buf[3] = (num >> 32) & 0xFF;
    buf[2] = (num >> 40) & 0xFF;
    buf[1] = (num >> 48) & 0xFF;
    buf[0] = (num >> 56) & 0xFF;
}

// Append entry to the end of the raft log file
int persist_entry_to_log(LogEntry &entry) {
    std::string truncate_file;
    char num_buf[8];
    off_t write_offset;
    int fd = 0;
    int undo_fd = 0;
    int ret;

    fd = open(RAFT_LOG_FILE.c_str(), O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        goto err;
    }

    // Go to write to the end of the log and get the offset
    write_offset = lseek(fd, 0, SEEK_END);
    if (write_offset == -1) {
        goto err;
    }

    // Create the file that indicates where the raft log should be truncated
    // to on failure recovery
    truncate_file = std::to_string(write_offset) + ".rlog";
    undo_fd = creat(truncate_file.c_str(), S_IRUSR | S_IWUSR);
    if (undo_fd < 0) {
        goto err;
    }

    // Actually write the entry to the file
    // Start with the term
    put_int64_to_buf(num_buf, entry.term);
    ret = write(fd, num_buf, 8);
    if (ret < 0) {
        goto err;
    }

    // Then the address of the entry
    put_int64_to_buf(num_buf, entry.address);
    ret = write(fd, num_buf, 8);
    if (ret < 0) {
        goto err;
    }

    // and finally, the actual data
    ret = write(fd, entry.data, BLOCK_SIZE);
    if (ret < 0) {
        goto err;
    }

    // We're done, so delete the undo file
    unlink(truncate_file.c_str());

    return 0;
err:
    if (fd > 0)
        close(fd);
    if (undo_fd > 0)
        close(undo_fd);
    printf("%s : Failed to persist entry to log: %s\n", __func__, strerror(errno));
    return -1;
}

int64_t find_address_from_path(std::string undo_path) {
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
  int64_t address;
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

void fix_raft_log(std::string path) {
    off_t address;

    address = find_address_from_path(path);
    truncate(RAFT_LOG_FILE.c_str(), address);

    unlink(path.c_str());
}

// Updates curTerm and voted_for as well as their persistent counterpart
// Requires the vote_lock
void update_term_and_voted_for(int64_t new_term, int64_t new_voted_for) {
    char buf[8];
    int fd;
    int ret;

    fd = open(RAFT_LOG_FILE.c_str(), O_WRONLY);
    if (fd < 0) {
        printf("%s: %s\n", __func__, strerror(errno));
        return;
    }

    curTerm = new_term;
    put_int64_to_buf(buf, curTerm);
    write(fd, buf, 8);

    voted_for = new_voted_for;
    put_int64_to_buf(buf, voted_for);
    write(fd, buf, 8);

    close(fd);
}

void create_empty_raft_log() {
    char buf[8];
    int fd;

    fd = open(RAFT_LOG_FILE.c_str(), O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        printf("%s: %s\n", __func__, strerror(errno));
        return;
    }

    // The first part of the raft.log file is the current term, which we will initialize to 0
    curTerm = 0;
    put_int64_to_buf(buf, curTerm);
    write(fd, buf, 8);

    // The second part of the file is who we voted for, which we will initialize to HAVENT_VOTED
    voted_for = HAVENT_VOTED;
    put_int64_to_buf(buf, HAVENT_VOTED);
    write(fd, buf, 8);

    close(fd);
}

void read_raft_log() {
    char buf[ENTRY_SIZE];
    int fd;
    int ret;
    int count = 0;

    fd = open(RAFT_LOG_FILE.c_str(), O_RDONLY);
    if (fd < 0) {
        create_empty_raft_log();
        return;
    }

    // Read in the curren term
    ret = read(fd, buf, 8);
    if (ret < 0)
        goto err;
    curTerm = get_int64_from_buf(buf);

    // Read in who we voted for this term
    ret = read(fd, buf, 8);
    if (ret < 0)
        goto err;
    voted_for = get_int64_from_buf(buf);

    while (read(fd, buf, ENTRY_SIZE) > 0) {
        LogEntry entry;

        entry.term = get_int64_from_buf(buf);
        entry.address = get_int64_from_buf(&buf[8]);
        memcpy(entry.data, &buf[16], BLOCK_SIZE);

        // Don't need to grab the log lock because we haven't started any threads
        raft_log.push_back(entry);
        count++;
    }

    close(fd);

    printf("Read %d entries from the persistent log!\n", count);
    return;
err:
    close(fd);
    printf("%s Error reading the persistent log!: %s", __func__, strerror(errno));
}

int do_atomic_write(int64_t address, std::string data) {
  std::string undo_path = FILE_PATH + "." + std::to_string(address) + ".undo";
  const char* write_buf = data.c_str();
  int64_t write_hash = std::hash<std::string>{}(data);
  int fd;
  int ret;

  ret = write_undo_file(undo_path, address);
  if (ret < 0) {
    goto err;
  }

  // To test the the undo logging stuff, if we see the data to write has the 
  // hash of the signal value (which is "hello_world" followed by a blocks worth of 0)
  // write some "corrupted" data and "crash" the server
  if (write_hash == 13494594211096014138ull && state == STATE_LEADER) {
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
  unlink(undo_path.c_str());

  return 0;
err:
  printf("Write %lx failed\n", address);
  perror(strerror(errno));
  return -1;
}

void apply_entries(int64_t first, int64_t last) {
  for (int i = first; i <= last; i++) {
    LogEntry entry;
    int64_t address;
    int64_t block;

    log_lock.lock();
    entry = raft_log[i];
    log_lock.unlock();

    address = entry.address;
    block = address / BLOCK_SIZE;

    lockArray[block].lock();
    if (!is_block_aligned(address)) {
        lockArray[block + 1].lock();
    }

    do_atomic_write(entry.address, std::string(entry.data, BLOCK_SIZE));

    if (!is_block_aligned(address)) {
        lockArray[block + 1].unlock();
    }
    lockArray[block].unlock();
  }
}

// Class is used to send RPCs to other clients
class RaftInterfaceClient {
  private:
    std::vector<std::unique_ptr<RaftInterface::Stub>> stubs;

    static void RequestVote(std::unique_ptr<RaftInterface::Stub> &stub,
        std::shared_ptr<std::atomic<uint8_t>> yes_votes,
        std::shared_ptr<std::atomic<uint8_t>> no_votes,
        int64_t term, int callingServerId)
    {
      ClientContext context;
      RequestVoteRequest request;
      RequestVoteResponse response;
      int64_t last_log_index = 0;

      // Fill in the data
      request.set_term(term);
      request.set_candidate_id(server_id);

      // Get the last log index and term
      log_lock.lock();
      last_log_index = raft_log.size() - 1;
      request.set_last_log_index(last_log_index);
      if (last_log_index >= 0)
        request.set_last_log_term(raft_log[last_log_index].term);
      else
        request.set_last_log_term(0);
      log_lock.unlock();

      Status status = stub->RequestVote(&context, request, &response);

      if (status.ok()) {
        if (response.vote_granted()) {
          std::cout << callingServerId << ": Yes vote\n";
          (*yes_votes).fetch_add(1);
        } else {
          std::cout << callingServerId << ": No vote\n";
          (*no_votes).fetch_add(1);
        }
      } else {
        std::cout << callingServerId << ": Comm error\n";
        // For now, assume network failure stuff is a no vote
        (*no_votes).fetch_add(1);
      }
    }

    static void EmptyAppendEntries(std::unique_ptr<RaftInterface::Stub> &stub, int64_t term)
    {
      Status status;
      ClientContext context;
      AppendEntriesRequest request;
      AppendEntriesResponse response;

      request.set_term(curTerm);
      request.set_leader_id(server_id);

      stub->AppendEntries(&context, request, &response);
    }

    static void AppendEntries(std::unique_ptr<RaftInterface::Stub> &stub, int64_t serverIdx, int64_t term)
    {
      AppendEntriesRequest request;
      AppendEntriesResponse response;
      Entry* entry;

      std::cout << "Sending append entry to " << serverIdx << "for term " << term << std::endl;

      bool success = false;
      int64_t update_index = 0;

      request.set_term(curTerm);
      request.set_leader_id(server_id);
      request.set_leader_commit(commit_index);

      while (!success) {
        // Create ClientContext as unique_ptr here because reusing a context
        // when retrying an RPC can cause gRPC to trash
        auto context = std::make_unique<ClientContext>();;

        log_lock.lock();
        // This represents the last index we are sending to the follower
        update_index = raft_log.size() - 1;

        if (nextIndex[serverIdx] - 1 < 0) {
          request.set_prev_log_term(-1);
          request.set_prev_log_index(-1);
        } else {
          request.set_prev_log_term(raft_log[nextIndex[serverIdx] - 1].term);
          request.set_prev_log_index(nextIndex[serverIdx] - 1);
        }

        for (int i = nextIndex[serverIdx]; i < raft_log.size(); i++) {
          entry = request.add_entries();
          entry->set_term(raft_log[i].term);
          entry->set_address(raft_log[i].address);
          entry->set_data(raft_log[i].data);
        }

        log_lock.unlock();

        stub->AppendEntries(context.get(), request, &response);
        success = response.success();
        if (!success) {
          std::cout << "Unsuccessful response received from server: " << serverIdx << " for term: " << term << std::endl;

          // If the write fails when we are trying to write the first index,
          // it is likely that the server is down, so stop trying for now
          if (nextIndex[serverIdx] > 0) {
            nextIndex[serverIdx]--;
          } else {
            return;
          }
        }
      }

      nextIndex[serverIdx] = update_index + 1;
      matchIndex[serverIdx] = update_index;
    }


  public:
    RaftInterfaceClient(std::vector<std::string> other_servers) {
      for (auto it = other_servers.begin(); it != other_servers.end(); it++) {
        stubs.push_back(RaftInterface::NewStub(grpc::CreateChannel(*it, grpc::InsecureChannelCredentials())));
        std::cout << "Initializing matchIndex and nextIndex" << std::endl;
        matchIndex.push_back(-1);
        nextIndex.push_back(-1);
      }
    }

    int StartElection() {
      // Start with one yes vote (we are voting for ourself)
      auto yes_votes = std::make_shared<std::atomic<uint8_t>>(1);
      auto no_votes = std::make_shared<std::atomic<uint8_t>>(0);
      int64_t majority = (num_servers / 2) + 1;
      int64_t term;

      // Increment the term
      vote_lock.lock();
      term = curTerm + 1;
      update_term_and_voted_for(term, server_id);
      vote_lock.unlock();
      std::cout << "Starting term: " << term << "\n";

      // Spawn threads to request the votes from our peers
      for (int i = 0; i < stubs.size(); i++) {
        std::thread t(RaftInterfaceClient::RequestVote,
          std::ref(stubs[i]), std::ref(yes_votes), std::ref(no_votes), term, i);
        t.detach();
      }

      // Wait until we have a majority of votes in some direction
      while (*yes_votes < majority && *no_votes < majority) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }

      std::cout << "Hello, received vote responses from all servers\n";

      if (*yes_votes >= majority) {
        state = STATE_LEADER;
        std::cout << "Elected leader" << std::endl;
        for (int i = 0; i < stubs.size(); i++) {
          nextIndex[i] = raft_log.size();
          matchIndex[i] = -1;
        }
        return 1;
      } else {
        state = STATE_FOLLOWER;
        last_comm_time = cur_time();
        return 0;
      }
    }

    int Heartbeat() {
      std::vector<std::thread> threads;
      int64_t term;

      term = curTerm;

      std::cout << "Sending Append Entries for term: " << term << std::endl;

      // Spawn threads to do the heartbeat
      for (int i = 0; i < stubs.size(); i++) {
        threads.push_back(std::thread(RaftInterfaceClient::AppendEntries,
          std::ref(stubs[i]), i, term));
      }

      std::cout << "Append Entries sent for term: " << term << ", waiting for responses" << std::endl;

      for (auto it = threads.begin(); it != threads.end(); it++) {
        (*it).join();
      }

      std::cout << "Append Entries sent for term: " << term << ", received responses" << std::endl;

      return 0;
    }
};

// Logic and data behind the server's behavior.
class RBSImpl final : public RBS::Service {
  RaftInterfaceClient servers;

  Status Read(ServerContext* context, const ReadRequest* request,
                  Response* reply) override {
    std::cout << "Data to read at offset: " << request->address() << std::endl;
    char* buf;
    int64_t address = request->address();
    int64_t block = address / BLOCK_SIZE;
    int ret;

    // Return without doing anything if we are not the primary
    if (state != STATE_LEADER) {
      reply->set_return_code(BLOCKSTORE_NOT_PRIM);
      reply->set_current_leader(current_leader_id);
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

    reply->set_log_size(raft_log.size());
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
    int entry_index;
    int64_t address = request->address();
    struct LogEntry log_entry;
    std::cout << "Data to write: " << request->data().c_str() << std::endl;

    // Make sure we are the primary
    if (state != STATE_LEADER) {
      reply->set_return_code(BLOCKSTORE_NOT_PRIM);
      reply->set_current_leader(current_leader_id);
      return Status::OK;
    }

    log_entry.term = curTerm;
    log_entry.address = address;
    memcpy(log_entry.data, request->data().c_str(), request->data().length());

    // Add the new entry to the log
    log_lock.lock();
   
    persist_entry_to_log(log_entry);
    raft_log.push_back(log_entry);
    entry_index = raft_log.size() - 1;

    log_lock.unlock();

    // Wait for the update to be commited before returning to the client
    while (commit_index < entry_index) {
        // Have we somehow been demoted from leader?
        // If so, forward the client to the new leader
        if (state != STATE_LEADER) {
            reply->set_return_code(BLOCKSTORE_NOT_PRIM);
            reply->set_current_leader(current_leader_id);
            return Status::OK;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    reply->set_log_size(raft_log.size());
    reply->set_return_code(BLOCKSTORE_SUCCESS);
    return Status::OK;
  }
public:
  RBSImpl(std::vector<std::string> other_servers)
    : servers(other_servers) {}

};

// Class that handles incoming RPCs
class RaftInterfaceImpl final : public RaftInterface::Service {
  Status RequestVote(ServerContext *context, const RequestVoteRequest *request,
                RequestVoteResponse *reply) override {
    int64_t requestTerm = request->term();
    int64_t candidateId = request->candidate_id();
    int64_t requestLastLogIndex = request->last_log_index();
    int64_t requestLastLogTerm = request->last_log_term();
    int64_t ourLastLogIndex;
    int64_t ourLastLogTerm;

    std::cout << "Received RequestVote from " << candidateId << " for term " << requestTerm << std::endl;

    log_lock.lock();
    ourLastLogIndex = raft_log.size() - 1;
    if (ourLastLogIndex >= 0)
        ourLastLogTerm = raft_log[ourLastLogIndex].term;
    else
        ourLastLogTerm = 0;
    log_lock.unlock();

    // Use a lock to make sure we don't respond to two simultaneous vote requests
    vote_lock.lock();

    if (requestTerm > curTerm) {
        state = STATE_FOLLOWER;
        update_term_and_voted_for(requestTerm, HAVENT_VOTED);
    } else if (requestTerm < curTerm) {
        reply->set_term(curTerm);
        reply->set_vote_granted(false);
        goto out;
    }

    reply->set_term(curTerm);

    if (requestLastLogTerm < ourLastLogTerm) {
        reply->set_vote_granted(false);
    } else if (requestLastLogTerm == ourLastLogTerm && requestLastLogIndex < ourLastLogIndex) {
        reply->set_vote_granted(false);
    } else if (voted_for != HAVENT_VOTED) {
        reply->set_vote_granted(false);
    } else {
        update_term_and_voted_for(curTerm, candidateId);
        reply->set_vote_granted(true);

        last_comm_time = cur_time();
    }

out:
    vote_lock.unlock();
    return Status::OK;
  }

  Status AppendEntries(ServerContext *context, const AppendEntriesRequest * request,
                AppendEntriesResponse *reply) override {

      int64_t requestTerm = request->term();
      int64_t leaderId = request->leader_id();
      int64_t prevLogIndex = request->prev_log_index();
      int64_t prevLogTerm = request->prev_log_term();

      std::cout << "Recieved Append Entries from " <<  leaderId << " for term " << requestTerm <<
        ", prevLogIndex: " << prevLogIndex << " and prevLogTerm: " << prevLogTerm << std::endl;

      last_comm_time = cur_time();

      if (requestTerm > curTerm) {
        vote_lock.lock();
        update_term_and_voted_for(requestTerm, HAVENT_VOTED);
        state = STATE_FOLLOWER;
        current_leader_id = leaderId;
        vote_lock.unlock();
        //reply->set_success(false);
        //reply->set_term(curTerm);
      }
  // check on valid term
      if(requestTerm < curTerm) {
        reply->set_term(curTerm);
        reply->set_success(false);
        return Status::OK;
      } else {
        
        // vote_lock.lock();
        // curTerm = requestTerm;
        // voted_for = HAVENT_VOTED;
        // state = STATE_FOLLOWER;
        // current_leader_id = leaderId;
        // vote_lock.unlock();

        state = STATE_FOLLOWER;

        reply->set_term(curTerm);
        reply->set_success(true);
      }

      log_lock.lock();
      // if prevlogindex is more than our last index, or term on prev log index is not same
      if(prevLogIndex >= (int64_t)raft_log.size() || (prevLogIndex >= 0 && raft_log[prevLogIndex].term != prevLogTerm)) {
        reply->set_success(false);
	log_lock.unlock();
        return Status::OK;
      }

      // deleting entries after index with same term
      if(prevLogIndex+1 < raft_log.size()) {
        truncate(RAFT_LOG_FILE.c_str(), LOG_INTRO_SIZE + ((prevLogIndex + 1) * ENTRY_SIZE));
        raft_log.erase(raft_log.begin()+prevLogIndex+1, raft_log.end());
      }

      // if(request->entries().size() > 0) {
      //   int64_t entryTerm = request->entries(0).term();
      //   if (raft_log[prevLogIndex+1].term != entryTerm) {
      //     raft_log.erase(raft_log.begin()+prevLogIndex+1, raft_log.end());
      //   }
      // }

      struct LogEntry newEntry;
      //run a loop, keep on appending entries from WriteRequest
      for (int i = 0; i < request->entries().size(); i++) { //confirm syntax???
        newEntry.term = request->entries(i).term();
        newEntry.address = request->entries(i).address();
        memcpy(newEntry.data, request->entries(i).data().c_str(), request->entries(i).data().length());
        persist_entry_to_log(newEntry);
        raft_log.push_back(newEntry);
        //commit_index = raft_log.size() - 1;
      }
      log_lock.unlock();

      int64_t leaderCommitIdx = request->leader_commit();
      if (leaderCommitIdx > commit_index) { //comparison should be with commit index
        int64_t new_commit_index = std::min(leaderCommitIdx, (int64_t)(raft_log.size()-1));

        // Apply the log entries
        apply_entries(commit_index + 1, new_commit_index);

        commit_index = new_commit_index;
      }


    reply->set_success(true);
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

void commit_thread() {
    int64_t last_log_index = 0;
    int64_t majority = (num_servers / 2) + 1;
    int64_t votes;

    while (true) {
        if (state != STATE_LEADER)
            goto sleep;

        last_log_index = raft_log.size() - 1;
        for (int i = last_log_index; i > commit_index; i--) {
            // Start with 1 vote counting ourselves
            votes = 1;
            for (int j = 0; j < matchIndex.size(); j++) {
                if (matchIndex[j] >= i)
                    votes++;
            }

            if (votes >= majority) {
                // We can only commit if the log has an entry of out term
                int64_t term;

                log_lock.lock();
                term = raft_log[i].term;
                log_lock.unlock();

                if (term == curTerm) {
                    apply_entries(commit_index + 1, i);
                    commit_index = i;
                }
            }
        }
sleep:
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

void handle_heartbeats(std::vector<std::string> other_servers) {
  RaftInterfaceClient servers(other_servers);
  std::random_device dev;
  std::mt19937 rng(dev());
  std::uniform_int_distribution<std::mt19937::result_type> dist(0, 100);
  const int ELECTION_TIMEOUT = 5000 + dist(rng);
  int ret;

  while (true) {
    if (state == STATE_LEADER) {
      // TODO: Handle sending heartbeats
      servers.Heartbeat();
      // TODO: We will want to lower this sleep time for performance, but for debugging
      // this helps with the logging output
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
    } else {
      // If we haven't heard from the leader since the timeout time,
      // let's try to become the leader
      int64_t last_time = last_comm_time.load();
      int64_t cur = cur_time();

      if (cur_time() > last_time && cur_time() - last_comm_time.load() >= ELECTION_TIMEOUT) {
        std::cout << "Trying to become the leader!\n";
        state = STATE_CANDIDATE;

        ret = servers.StartElection();

        if (ret) {
          std::cout << "Became the leader for term " << curTerm << ", id: " << server_id << "! Democracy works!\n";
        } else {
          std::cout << "Did not become the leader\n";
          last_comm_time = cur_time();
        }
      }
    }
  }
}

void process_server_file(std::vector<std::string> &list, std::string filename) {
  std::ifstream file(filename);
  std::string line;

  num_servers = 1;
  if (file.is_open()) {
    while(std::getline(file, line)) {
      num_servers++;
      list.push_back(line);
    }

    file.close();
  }
}

int main(int argc, char** argv) {
  if (argc < 3) {
    std::cout << "Usage: ./server <id> <listen_port> <servers_file> [is_primary]\n";;
    return -1;
  }
  server_id = std::stoi(argv[1]);
  std::string listen_port = argv[2];
  std::string servers_file = argv[3];
  std::vector<std::string> other_servers;

  if (argc >= 5) {
    std::cout << "Running as leader!\n";
    state = STATE_LEADER;
  }
  else {
    std::cout << "Running as follower!\n";
    state = STATE_FOLLOWER;
  }

  process_server_file(other_servers, servers_file);

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
    if (entry->d_type == DT_REG && strcmp(&filename[extension_index], ".rlog") == 0) {
      fix_raft_log(std::string(filename));
    }
  }

  // Read the raft log file and populate our in memory raft log
  read_raft_log();

  // On startup, give the primary an extra few seconds to send a heartbeat
  last_comm_time = cur_time() + 10000;

  std::thread server_thread(RunServer, listen_port, other_servers);
  std::thread ldr_commit_thread(commit_thread);

  handle_heartbeats(other_servers);

  return 0;
}

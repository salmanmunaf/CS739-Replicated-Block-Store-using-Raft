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

// standard c++
#include <iostream>
#include <sstream>
#include <fstream>
#include <iomanip>
#include <string>
#include <unordered_map>
#include <chrono>
#include <thread>
#include <map>
// standard c
#include <unistd.h>
#include <stdlib.h> // malloc
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <dirent.h>
#include <sys/stat.h>
// openssl library
#include <openssl/sha.h>
// grpc library
#include <grpcpp/grpcpp.h>
// program's header
#include "blockstore.h"
#include "blockstore.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientWriter;
using grpc::ClientReader;
using grpc::Status;
using grpc::StatusCode;
using namespace cs739;
using namespace std;

#define KB (1024)
#define BLOCK_SIZE (4*KB)
#define TIMEOUT (7000)

int primary;
int log_size;
class RBSClient {
  public:
    RBSClient(std::shared_ptr<Channel> channel)
      : stub_(RBS::NewStub(channel)) {}

    int CheckPrimary() {
        ClientContext context;

        EmptyPacket request;

        Response response;

        Status status = stub_->CheckPrimary(&context, request, &response);

        if(status.ok()) {
            return response.primary();
        } else {
            // return error code
        }
    }

    int Read(off_t offset) {
        ClientContext context;

        ReadRequest request;
        request.set_address(offset);

        Response response;

        Status status = stub_->Read(&context, request, &response);

        if(status.ok()) {
            if(response.return_code() == BLOCKSTORE_SUCCESS) {
                std::cout << std::hash<std::string>{}(response.data()) << std::endl;
                log_size = response.log_size();
                return response.return_code();
            } else if (response.return_code() == BLOCKSTORE_NOT_PRIM) {
                primary = response.primary();
                return response.return_code();
            }
            return response.error_code();
        } else {
            return -1;
        }
    }

    int Write(off_t offset, const std::string& data) {
        ClientContext context;

        WriteRequest request;
        request.set_address(offset);
        request.set_data(data);

        Response response;

        Status status = stub_->Write(&context, request, &response);

        if(status.ok()) {
            int return_code = response.return_code();
            if(return_code == BLOCKSTORE_SUCCESS) {
                log_size = response.log_size();
                return response.return_code();
            } else if (return_code == BLOCKSTORE_NOT_PRIM) {
                primary = response.primary();
                return response.return_code();
            }
            return response.error_code();
        } else {
            return -1;
        }
    }

  private:
    std::unique_ptr<RBS::Stub> stub_;
};

int do_read(std::vector<RBSClient> &serverArr, off_t offset, ostream& output) {
    bool first_try = true;
    int result = -1, retry = 1;
    int64_t request_start_time = cur_time();

    while (result != BLOCKSTORE_SUCCESS && cur_time() - request_start_time < TIMEOUT) {
        // Wait some time between sending requests
        if (!first_try) {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
        first_try = false;

        RBSClient &rbsClient = serverArr[primary];
        auto ts_read_start = std::chrono::steady_clock::now();
        result = rbsClient.Read(offset);
        auto ts_read_end = std::chrono::steady_clock::now();

        if (result == BLOCKSTORE_SUCCESS) {
          output << "log size: " << log_size <<   ", duration: " << std::chrono::duration_cast<std::chrono::microseconds>(ts_read_end - ts_read_start).count() << std::endl;
        }

        // If we couldn't communicate with the given server, increment the primary
        if (result == -1) {
            primary = (primary + 1) % serverArr.size();
        }
    
        std::cout << primary << ": " << result << std::endl;
    }
    
    return primary;
}

int do_write(std::vector<RBSClient> &serverArr, off_t offset, std::string str, ostream& output) {
    bool first_try = true;
    int result = -1, retry = 1;
    int64_t request_start_time = cur_time();

    while (result != BLOCKSTORE_SUCCESS && cur_time() - request_start_time < TIMEOUT) {
        if (!first_try) {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
        first_try = false;

        RBSClient &rbsClient = serverArr[primary];
        auto ts_write_start = std::chrono::steady_clock::now();
        result = rbsClient.Write(offset, std::string(str));
        auto ts_write_end = std::chrono::steady_clock::now();

        if (result == BLOCKSTORE_SUCCESS) {
          output << "log size: " << log_size <<   ", duration: " << std::chrono::duration_cast<std::chrono::microseconds>(ts_write_end - ts_write_start).count() << std::endl;
        }

        // If we couldn't communicate with the given server, increment the primary
        if (result == -1) {
            primary = (primary + 1) % serverArr.size();
        }

        std::cout << primary << ": " << result << std::endl;
    }

    return primary;
}

void process_server_file(std::vector<std::string> &list, std::string filename) {
  std::ifstream file(filename);
  std::string line;

  if (file.is_open()) {
    while(std::getline(file, line)) {
      list.push_back(line);
    }

    file.close();
  }
}

void getRandomText(std::string &s, int size) {
    int num_bytes_written = 0;
    while (num_bytes_written < size) {
        s.push_back((rand() % 26) + 'a');
        num_bytes_written++;
    }
}

int main(int argc, char** argv) {

  std::string server_file = argv[1];
  std::vector<std::string> servers;

  process_server_file(servers, server_file);

  std::vector<RBSClient> serverArr;
  for (int i = 0; i < servers.size(); i++) {
    serverArr.push_back(RBSClient(grpc::CreateChannel(servers[i], grpc::InsecureChannelCredentials())));
  }

  int user_input;
  off_t offset;
  std::string str;
  int64_t request_start_time;
  primary=1;

  ofstream writeLogFile;
  writeLogFile.open("writeLatency.txt");
  ofstream readLogFile;
  readLogFile.open("readLatency.txt");
  size_t string_size = 4096;
  srand(time(NULL));
  getRandomText(str, string_size);
  str.resize(string_size, ' ');

  unsigned int num_iterations = 1000;
  offset = 0;
  for (int i = 0; i < num_iterations; i++) {
    primary = do_write(serverArr, offset, str, writeLogFile);
    primary = do_read(serverArr, offset, readLogFile);
    offset += BLOCK_SIZE;
  }
  writeLogFile.close();
  readLogFile.close();

  return 0;
}

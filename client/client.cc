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

int do_read(RBSClient* serverArr, off_t offset) {
    bool first_try = true;
    int result = -1, retry = 1;
    uint64_t request_start_time = cur_time();

    while (result != BLOCKSTORE_SUCCESS && cur_time() - request_start_time < TIMEOUT) {
        // Wait some time between sending requests
        if (!first_try) {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
        first_try = false;

        RBSClient &rbsClient = serverArr[primary];
        result = rbsClient.Read(offset);
    
        std::cout << primary << ": " << result << std::endl;
    }
    
    return primary;
}

int do_write(RBSClient* serverArr, off_t offset, std::string str) {
    bool first_try = true;
    int result = -1, retry = 1;
    uint64_t request_start_time = cur_time();

    while (result != BLOCKSTORE_SUCCESS && cur_time() - request_start_time < TIMEOUT) {
        if (!first_try) {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
        first_try = false;

        RBSClient &rbsClient = serverArr[primary];
        result = rbsClient.Write(offset, std::string(str));

        std::cout << primary << ": " << result << std::endl;
    }

    return primary;
}

int main(int argc, char** argv) {

  std::string server1 = argv[1];
  std::string server2 = argv[2];
  std::string server3 = argv[3];
  std::string server4 = argv[4];
  std::string server5 = argv[5];

  RBSClient rbsClient1(
      grpc::CreateChannel(server1, grpc::InsecureChannelCredentials()));
  RBSClient rbsClient2(
      grpc::CreateChannel(server2, grpc::InsecureChannelCredentials()));
  RBSClient rbsClient3(
      grpc::CreateChannel(server3, grpc::InsecureChannelCredentials()));
  RBSClient rbsClient4(
      grpc::CreateChannel(server4, grpc::InsecureChannelCredentials()));
  RBSClient rbsClient5(
      grpc::CreateChannel(server5, grpc::InsecureChannelCredentials()));

  RBSClient serverArr [5] = { RBSClient(grpc::CreateChannel(server1, grpc::InsecureChannelCredentials())),
                            RBSClient(grpc::CreateChannel(server2, grpc::InsecureChannelCredentials())),
                            RBSClient(grpc::CreateChannel(server3, grpc::InsecureChannelCredentials())),
                            RBSClient(grpc::CreateChannel(server4, grpc::InsecureChannelCredentials())),
                            RBSClient(grpc::CreateChannel(server5, grpc::InsecureChannelCredentials())) };

  int user_input;
  off_t offset;
  std::string str;
  uint64_t request_start_time;
  primary=1;

  // one-of read
  if (argc == 4) {
    offset = std::stoull(std::string(argv[3]));
    do_read(serverArr, offset);
    return 0;
  }
  // one-of write
  if (argc == 5) {
    offset = std::stoull(std::string(argv[3]));
    str = std::string(argv[4]);
    str.resize(4096, ' ');
    do_write(serverArr, offset, str);
    return 0;
  }

  std::cout << "Enter operation: ";
  std::cin >> user_input;    // input = 1 for read, 2 for write, 0 to exit
  while(user_input != 0) {

    std::cout << "Enter offset: " << std::endl;
    std::cin >> offset;

    if(user_input == 1) {
        primary = do_read(serverArr, offset);
    } else {
        
        std::cout << "Enter data to write: " << std::endl;
        std::cin >> str;

        // char* str = (char *) malloc(BLOCK_SIZE/sizeof(char));
        // strcpy(str, data);

        str.resize(4096, ' ');
        std::cout << "Hash of data to write: " << std::hash<std::string>{}(str) << std::endl;

        primary = do_write(serverArr, offset, str);
    }

    std::cout << "Enter operation: ";
    std::cin >> user_input;    // input = 1 for read, 2 for write, 0 to exit
  }

  return 0;
}

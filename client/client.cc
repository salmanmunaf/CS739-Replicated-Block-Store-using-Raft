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
                primary = response.current_leader();
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
                primary = response.current_leader();
                return response.return_code();
            }
            return response.error_code();
        } else {
            return -1;
        }
    }

    int DisplayLog(int id) {
      ClientContext context;

      LogRequest request;
      request.set_id(id);

      Response response;

      Status status = stub_->DisplayLog(&context, request, &response);

      if(status.ok()) {
        return response.return_code();
      } else {
        return -1;
      }

    }

  private:
    std::unique_ptr<RBS::Stub> stub_;
};

int do_read(std::vector<RBSClient> &serverArr, off_t offset) {
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
        result = rbsClient.Read(offset);

        // If we couldn't communicate with the given server, increment the primary
        if (result == -1) {
            primary = (primary + 1) % serverArr.size();
        }
    
        std::cout << primary << ": " << result << std::endl;
    }
    
    return primary;
}

int do_write(std::vector<RBSClient> &serverArr, off_t offset, std::string str) {
    bool first_try = true;
    int result = -1, retry = 1;
    int64_t request_start_time = cur_time();

    while (result != BLOCKSTORE_SUCCESS && cur_time() - request_start_time < TIMEOUT) {
        if (!first_try) {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
        first_try = false;

        RBSClient &rbsClient = serverArr[primary];
        result = rbsClient.Write(offset, std::string(str));

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

  // one-of read
  if (argc == 3) {
    offset = std::stoull(std::string(argv[3]));
    do_read(serverArr, offset);
    return 0;
  }
  // one-of write
  if (argc == 4) {
    offset = std::stoull(std::string(argv[3]));
    str = std::string(argv[4]);
    str.resize(4096, ' ');
    do_write(serverArr, offset, str);
    return 0;
  }

  std::cout << "Enter operation (1 = read, 2 = write, 3 = display log, 0 = exit): ";
  std::cin >> user_input;    // input = 1 for read, 2 for write, 0 to exit
  while(user_input != 0) {
    if(user_input != 3) {
      std::cout << "Enter offset: " << std::endl;
      std::cin >> offset;
    }

    if(user_input == 1) {
        primary = do_read(serverArr, offset);
    } else if (user_input == 2){
        
        std::cout << "Enter data to write: " << std::endl;
        std::cin >> str;

        // char* str = (char *) malloc(BLOCK_SIZE/sizeof(char));
        // strcpy(str, data);

        str.resize(4096, ' ');
        std::cout << "Hash of data to write: " << std::hash<std::string>{}(str) << std::endl;

        primary = do_write(serverArr, offset, str);
    } else {
      std::cout<<"Enter server id: "<<std::endl;
      int server_id;
      std::cin>>server_id;
      RBSClient &rbsClient = serverArr[primary];
      int result = rbsClient.DisplayLog(server_id);
      std::cout << "Server displayed log"<<std::endl;
    }

    std::cout << "Enter operation: ";
    std::cin >> user_input;    // input = 1 for read, 2 for write, 0 to exit
  }

  return 0;
}

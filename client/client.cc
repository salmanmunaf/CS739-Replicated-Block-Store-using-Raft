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
#include "blockstore.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientWriter;
using grpc::ClientReader;
using grpc::Status;
using grpc::StatusCode;
using namespace cs739;

#define KB (1024)
#define BLOCK_SIZE (4*KB)

class RBSClient {
  public:
    RBSClient(std::shared_ptr<Channel> channel)
      : stub_(RBS::NewStub(channel)) {}

    int CheckPrimary() {
        ClientContext context;

        CheckPrimaryRequest request;

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
            if(response.return_code() == 1) {
                std::cout << response.data();
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
            if(response.return_code() == 1) {
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

int main(int argc, char** argv) {

  std::string server1 = argv[1];
  std::string server2 = argv[2];

  RBSClient rbsClient1(
      grpc::CreateChannel(server1, grpc::InsecureChannelCredentials()));
  RBSClient rbsClient2(
      grpc::CreateChannel(server2, grpc::InsecureChannelCredentials()));

  int user_input;
  std::cout << "Enter operation: ";
  std::cin >> user_input;    // input = 1 for read, 2 for write, 0 to exit
  off_t offset;
  char* str;
  int primary=0;
  while(user_input != 0) {

    std::cout << "Enter offset: " << std::endl;
    std::cin >> offset;

    if(user_input == 1) {

        int result = -1, retry = 1;
        while(result == -1 && retry <= 3) {
            if(primary == 0) {
                result = rbsClient1.Read(offset); 
            } else {
                result = rbsClient2.Read(offset);
            }
        }

        if(result == -1) {
            primary = 1-primary;
        } 

        while(result == -1 && retry <= 3) {
            if(primary == 0) {
                result = rbsClient1.Read(offset); 
            } else {
                result = rbsClient2.Read(offset);
            }
        }
    
    } else {
        
        std::cout << "Enter data to write: " << std::endl;
        std::cin >> str;

        // char* str = (char *) malloc(BLOCK_SIZE/sizeof(char));
        // strcpy(str, data);

        std::cout << "Data to write: " << str << std::endl;
        
        int result = -1, retry = 1;
        while(result == -1 && retry <= 3) {
            if(primary == 0) {
                result = rbsClient1.Write(offset, std::string(str)); 
            } else {
                result = rbsClient2.Write(offset, std::string(str));
            }
        }

        if(result == -1) {
            primary = 1-primary;
        }

        while(result == -1 && retry <= 3) {
            if(primary == 0) {
                result = rbsClient1.Write(offset, std::string(str)); 
            } else {
                result = rbsClient2.Write(offset, std::string(str));
            }
        }

        std::cout << result;

    }

    std::cout << "Enter operation: ";
    std::cin >> user_input;    // input = 1 for read, 2 for write, 0 to exit
  }

  return 0;
}

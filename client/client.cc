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

#define KB (1024)
#define BLOCK_SIZE (4*KB)

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
            if(return_code == BLOCKSTORE_SUCCESS || return_code == BLOCKSTORE_NOT_PRIM) {
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

  auto start = std::chrono::steady_clock::now();

  int user_input;
  std::cout << "Enter operation: ";
  //std::cin >> user_input;    // input = 1 for read, 2 for write, 0 to exit
  user_input = 1; //all reads
  off_t offset;
  std::string str;
  uint64_t request_start_time;
  const uint64_t TIMEOUT = 7000;
  bool first_try;
  int primary=0;

  auto read_lat = 0;
  //offset = 0;
  int counter = 0;
  while(counter != 50) {

    std::cout << "Enter offset: " << std::endl;
    //std::cin >> offset;
    offset = 0;
    //offset+=1024;
    first_try = true;
    std::cout << "Offset = " << offset << " and the user_input is " << user_input << " and the counter is " << counter << std::endl;
    if(user_input == 1) {
    	auto startinner = std::chrono::steady_clock::now();
        int result = -1, retry = 1;
        request_start_time = cur_time();

        while (result != BLOCKSTORE_SUCCESS && cur_time() - request_start_time < TIMEOUT) {
            // Wait some time between sending requests
            if (!first_try) {
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
            }
            first_try = false;

            if (primary == 0) {
                result = rbsClient1.Read(offset);
            } else {
                result = rbsClient2.Read(offset);
            }

            std::cout << primary << ": " << result << std::endl;
            if (result != BLOCKSTORE_SUCCESS) {
                primary = 1 - primary;
            }
        }
        auto endinner = std::chrono::steady_clock::now();
        std::cout << " Inner Elapsed time in microiseconds: "
                  << std::chrono::duration_cast<std::chrono::microseconds>(endinner - startinner).count()
                  << " ms" << std::endl;
        read_lat += std::chrono::duration_cast<std::chrono::microseconds>(endinner - startinner).count();
    } else {
    	auto startinner = std::chrono::steady_clock::now();
        std::cout << "Enter data to write: " << std::endl;
        //std::cin >> str;
        str = "Distributed Systems";
        std::cout << "The data being written is " << str << std::endl;

        // char* str = (char *) malloc(BLOCK_SIZE/sizeof(char));
        // strcpy(str, data);

        str.resize(4096, ' ');
        std::cout << "Hash of data to write: " << std::hash<std::string>{}(str) << std::endl;

        int result = -1, retry = 1;
        request_start_time = cur_time();

        while (result != BLOCKSTORE_SUCCESS && cur_time() - request_start_time < TIMEOUT) {
            if (!first_try) {
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
            }
            first_try = false;

            if(primary == 0) {
                result = rbsClient1.Write(offset, std::string(str)); 
            } else {
                result = rbsClient2.Write(offset, std::string(str));
            }

            std::cout << primary << ": " << result << std::endl;
            if (result != BLOCKSTORE_SUCCESS) {
                primary = 1 - primary;
            }
        }

        std::cout << result << std::endl;

        auto endinner = std::chrono::steady_clock::now();
		std::cout << " Inner Elapsed time in milliseconds: "
				  << std::chrono::duration_cast<std::chrono::milliseconds>(endinner - startinner).count()
				  << " ms" << std::endl;

    }

    //std::cout << "Enter operation: ";
    counter++;
    //std::cin >> user_input;    // input = 1 for read, 2 for write, 0 to exit
  }
  auto end = std::chrono::steady_clock::now();
  std::cout << " Total Elapsed time in milliseconds: "
          << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count()
          << " ms" << std::endl;
  read_lat = read_lat/50;
  std::cout << "The mean latency for 50 reads is " << read_lat << std::endl;
  return 0;
}

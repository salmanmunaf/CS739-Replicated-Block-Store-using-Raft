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

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using namespace cs739;
using namespace std;
// using helloworld::Greeter;
// using helloworld::HelloReply;
// using helloworld::HelloRequest;

#define MAX_NUM_BLOCKS (1000)
#define KB (1024)
#define BLOCK_SIZE (4*KB)
std::mutex lockArray [MAX_NUM_BLOCKS];
const std::string FILE_PATH = "blockstore.log";

uint64_t time_since_last_response = 0;
queue<string> data_log; //queue to log data when backup fails
queue<int> block_log; //queue to log block num when backup fails


Status Write(ServerContext* context, const WriteRequest* request,
                  Response* reply) override {
    cout << "I am backup. Request received from primary to write data" << endl;
    cout << "Data to write: " << request->data().c_str() << endl;
    int fd = open(FILE_PATH.c_str(), O_WRONLY);
    if (fd < 0) {
      reply->set_error_code(errno);
      printf("%s : Failed to open file\n", __func__);
      perror(strerror(errno));
      return Status::OK;
    }

    int block_num = request->address()/BLOCK_SIZE;
    if (block_num >= MAX_NUM_BLOCKS) {
      reply->set_error_code(errno);
      printf("%s : Invalid block number\n", __func__);
      perror(strerror(errno));
      return Status::OK;
    }

    lockArray[block_num].lock();
    int res = pwrite(fd, request->data().c_str(), BLOCK_SIZE, request->address());
    fsync(fd);
    lockArray[block_num].unlock();

    if (res == -1) {
      reply->set_error_code(errno);
      printf("%s : Failed to write to file\n", __func__);
      perror(strerror(errno));
      return Status::OK;
    }

    if (close(fd) != 0) {
      reply->set_error_code(errno);
      printf("%s : Failed to close file\n", __func__);
      perror(strerror(errno));
      return Status::OK;
    }

    reply->set_return_code(0);
    reply->set_error_code(0);
    return Status::OK;
  }

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

#ifndef BLOCKSTORE_H
#define BLOCKSTORE_H

#include <chrono>

#define BLOCKSTORE_SUCCESS 1
#define BLOCKSTORE_NOT_PRIM 2
#define BLOCKSTORE_FAIL -2

uint64_t cur_time() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
    std::chrono::system_clock::now().time_since_epoch()
  ).count();
}

#endif // BLOCKSTORE_H

#pragma once

#include <tpcc/stdlike/condition_variable.hpp>

#include <cstddef>
#include <mutex>
#include <condition_variable>
#include <iostream>

namespace tpcc {
namespace solutions {

class CyclicBarrier {
 public:
  explicit CyclicBarrier(const size_t num_threads)
      : num_threads_(num_threads), n_threads_(num_threads) {
    flag = false;
  }

  void PassThrough() {
    std::unique_lock<std::mutex> lock{mutex_};
    bool f = !flag;
    --n_threads_;
    if (n_threads_ == 0) {
      n_threads_ = num_threads_;
      flag = f;
      all_threads_arrived_.notify_all();
    } else {
      all_threads_arrived_.wait(lock, [this, f]() { return f == flag; });
    }
  }

 private:
  std::mutex mutex_;
  tpcc::condition_variable all_threads_arrived_;
  size_t num_threads_;
  bool flag;
  std::size_t n_threads_;
};

}  // namespace solutions
}  // namespace tpcc

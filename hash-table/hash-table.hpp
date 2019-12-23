/*
 * Implementation of striped hash-set
 * Author: Mary Feofanova
 */

#pragma once

#include <tpcc/stdlike/atomic.hpp>
#include <tpcc/stdlike/condition_variable.hpp>
#include <tpcc/stdlike/mutex.hpp>

#include <tpcc/support/compiler.hpp>

#include <algorithm>
#include <forward_list>
#include <functional>
#include <shared_mutex>
#include <vector>
#include <utility>

#include <iostream>
#include <mutex>

namespace tpcc {
namespace solutions {

////////////////////////////////////////////////////////////////////////////////

// implement writer-priority rwlock
class ReaderWriterLock {
 public:
  // reader section / shared ownership

  void lock_shared() {
    std::unique_lock<std::mutex> lock{mutex_};
    wait_for_writer_finish_.wait(
        lock, [this]() { return w_ == false && writer_ == 0; });
    r_++;
  }

  void unlock_shared() {
    std::unique_lock<std::mutex> lock{mutex_};
    r_--;
    wait_for_writer_and_reader_finish_.notify_one();
    wait_for_writer_finish_.notify_one();
  }

  // writer section / exclusive ownership

  void lock() {
    std::unique_lock<std::mutex> lock{mutex_};
    writer_++;
    wait_for_writer_and_reader_finish_.wait(
        lock, [this]() { return w_ == false && r_ == 0; });
    w_ = true;
    writer_--;
  }

  void unlock() {
    std::unique_lock<std::mutex> lock{mutex_};
    w_ = false;
    wait_for_writer_and_reader_finish_.notify_one();
    wait_for_writer_finish_.notify_one();
  }

 private:
  std::mutex mutex_;
  tpcc::condition_variable wait_for_writer_finish_;
  tpcc::condition_variable wait_for_writer_and_reader_finish_;
  bool w_ = false;
  // bool r_ = true;
  size_t writer_{0};
  size_t r_{0};
};

////////////////////////////////////////////////////////////////////////////////

template <typename T, class HashFunction = std::hash<T>>
class StripedHashSet {
 private:
  using RWLock = ReaderWriterLock;  // std::shared_timed_mutex

  using ReaderLocker = std::shared_lock<RWLock>;
  using WriterLocker = std::unique_lock<RWLock>;

  using Bucket = std::forward_list<T>;
  using Buckets = std::vector<Bucket>;

 public:
  explicit StripedHashSet(const size_t concurrency_level = 4,
                          const size_t growth_factor = 2,
                          const double max_load_factor = 0.8)
      : concurrency_level_(concurrency_level),
        growth_factor_(growth_factor),
        max_load_factor_(max_load_factor),
        buckets_(concurrency_level),
        rwlock_(concurrency_level) {
    count_elements = 0;
  }

  bool Insert(T element) {
    size_t new_size = NewSize(element);
    if (new_size != 0) {
      if (MaxLoadFactorExceeded()) {
        TryExpandTable(new_size);
      }
      return true;
    }
    return false;
  }

  bool Remove(const T& element) {
    size_t h = hash_(element);
    auto stripe_lock = LockStripe<WriterLocker>(GetStripeIndex(h));

    auto result = std::find(GetBucket(h).begin(), GetBucket(h).end(), element);
    if (result != GetBucket(h).end()) {
      GetBucket(h).remove(element);
      count_elements.fetch_sub(1);
      return true;
    }
    return false;
  }

  bool Contains(const T& element) const {
    auto h = hash_(element);
    auto stripe_lock = LockStripe<ReaderLocker>(GetStripeIndex(h));
    auto result = std::find(GetBucket(h).begin(), GetBucket(h).end(), element);
    if (result != GetBucket(h).end()) {
      return true;
    }
    return false;
  }

  size_t GetSize() const {
    return count_elements.load();
  }
  size_t GetBucketCount() const {
    auto stripe_lock = LockStripe<ReaderLocker>(0);
    // for testing purposes
    // do not optimize, just acquire arbitrary lock and read bucket count
    return buckets_.size();
  }

 private:
  size_t NewSize(T element) {
    size_t h = hash_(element);
    auto stripe_lock = LockStripe<WriterLocker>(GetStripeIndex(h));

    auto result = std::find(GetBucket(h).begin(), GetBucket(h).end(), element);
    if (result == GetBucket(h).end()) {
      GetBucket(h).push_front(element);
      count_elements.fetch_add(1);
      return buckets_.size() * growth_factor_;
    }
    return 0;
  }

  size_t GetStripeIndex(const size_t hash_value) const {
    auto stripe_lock = LockStripe<ReaderLocker>(0);
    return hash_value % concurrency_level_;
  }

  template <class Locker>
  Locker LockStripe(const size_t stripe_index) const {
    return Locker{rwlock_[stripe_index]};
  }

  size_t GetBucketIndex(const size_t hash_value) const {
    return hash_value % buckets_.size();
  }

  Bucket& GetBucket(const size_t hash_value) const {
    return const_cast<Bucket&>(buckets_[GetBucketIndex(hash_value)]);
  }

  bool MaxLoadFactorExceeded() const {
    auto stripe_lock = LockStripe<ReaderLocker>(0);
    if (count_elements >= max_load_factor_ * buckets_.size()) {
      return true;
    }
    return false;
  }

  void TryExpandTable(const size_t expected_bucket_count) {
    std::vector<WriterLocker> locks(concurrency_level_);
    for (size_t i = 0; i < concurrency_level_; i++) {
      locks[i] = LockStripe<WriterLocker>(i);
    }

    if (buckets_.size() < expected_bucket_count) {
      Buckets new_buckets(expected_bucket_count);
      std::swap(buckets_, new_buckets);
      for (Bucket i : new_buckets) {
        for (T j : i) {
          GetBucket(hash_(j)).push_front(j);
        }
      }
    }
  }

 private:
  size_t concurrency_level_;
  size_t growth_factor_;
  double max_load_factor_;

  HashFunction hash_;
  tpcc::atomic<size_t> count_elements;
  Buckets buckets_;
  mutable std::vector<RWLock> rwlock_;
  // mutable tpcc::mutex mutex_;
};

}  // namespace solutions
}  // namespace tpcc
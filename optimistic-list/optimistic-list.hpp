#pragma once

#include <tpcc/concurrency/backoff.hpp>
#include <tpcc/memory/bump_pointer_allocator.hpp>
#include <tpcc/stdlike/atomic.hpp>
#include <tpcc/support/compiler.hpp>

#include <limits>
#include <mutex>
#include <thread>

namespace tpcc {
namespace solutions {

////////////////////////////////////////////////////////////////////////////////

// implement Test-and-TAS (!) spinlock
class SpinLock {
 public:
  void Lock() {
    Backoff backoff{};
    while (flag_.exchange(true)) {
      while (flag_.load()) {
        backoff();
      }
    }
  }

  void Unlock() {
    flag_.store(false);
  }

  // adapters for BasicLockable concept

  void lock() {
    Lock();
  }

  void unlock() {
    Unlock();
  }

 private:
  tpcc::atomic<bool> flag_{false};
};

////////////////////////////////////////////////////////////////////////////////

// don't touch this
template <typename T>
struct KeyTraits {
  static T LowerBound() {
    return std::numeric_limits<T>::min();
  }

  static T UpperBound() {
    return std::numeric_limits<T>::max();
  }
};

////////////////////////////////////////////////////////////////////////////////

template <typename T, class TTraits = KeyTraits<T>>
class OptimisticLinkedSet {
 private:
  struct Node {
    T key_;
    tpcc::atomic<Node*> next_;
    SpinLock spinlock_;
    tpcc::atomic<bool> marked_{false};

    Node(const T& key, Node* next = nullptr) : key_(key) {
      next_ = next;
    }

    // use: auto node_lock = node->Lock();
    std::unique_lock<SpinLock> Lock() {
      return std::unique_lock<SpinLock>{spinlock_};
    }
  };

  struct EdgeCandidate {
    Node* pred_;
    Node* curr_;

    EdgeCandidate(Node* pred, Node* curr) : pred_(pred), curr_(curr) {
    }
  };

 public:
  explicit OptimisticLinkedSet(BumpPointerAllocator& allocator)
      : allocator_(allocator) {
    count_elements_ = 0;
    CreateEmptyList();
  }

  bool Insert(T key) {
    int flag = MakeInsert(key);
    while (flag == 0) {
      flag = MakeInsert(key);
    }
    if (flag == 1) {
      return true;
    }
    return false;
  }

  int MakeInsert(T key) {
    EdgeCandidate edge = Locate(key);
    auto pred_lock = edge.pred_->Lock();
    auto curr_lock = edge.curr_->Lock();

    if (Validate(edge)) {
      if (edge.curr_->key_ != key) {
        count_elements_.fetch_add(1);
        Node* node = allocator_.New<Node>(key, edge.curr_);
        edge.pred_->next_.store(node);
        return 1;
      }
      return -1;
    }
    return 0;
  }

  bool Remove(const T& key) {
    int flag = MakeRemove(key);
    while (flag == 0) {
      flag = MakeRemove(key);
    }
    if (flag == 1) {
      return true;
    }
    return false;
  }

  int MakeRemove(T key) {
    EdgeCandidate edge = Locate(key);
    auto pred_lock = (edge.pred_)->Lock();
    auto edge_node_lock = (edge.curr_)->Lock();

    if (Validate(edge)) {
      if ((edge.curr_)->key_ == key) {
        count_elements_.fetch_sub(1);
        (edge.pred_)->next_.store((edge.curr_)->next_.load());
        (edge.curr_)->marked_ = true;
        return 1;
      }
      return -1;
    }
    return 0;
  }

  bool Contains(const T& key) const {
    EdgeCandidate edge = Locate(key);
    if (!edge.curr_->marked_ && edge.curr_->key_ == key) {
      return true;
    }
    return false;
  }

  size_t GetSize() const {
    return count_elements_.load();
  }

 private:
  void CreateEmptyList() {
    // create sentinel nodes
    head_ = allocator_.New<Node>(TTraits::LowerBound());
    head_->next_ = allocator_.New<Node>(TTraits::UpperBound());
  }

  EdgeCandidate Locate(const T& key) const {
    EdgeCandidate edge(head_, head_->next_.load());
    while (edge.curr_->key_ < key) {
      edge.pred_ = edge.curr_;
      edge.curr_ = edge.pred_->next_.load();
    }
    return edge;
  }

  bool Validate(const EdgeCandidate& edge) const {
    if (edge.pred_->marked_ || edge.pred_->next_.load() != edge.curr_) {
      return false;
    }
    return true;
  }

 private:
  BumpPointerAllocator& allocator_;
  Node* head_{nullptr};
  tpcc::atomic<size_t> count_elements_;
};

}  // namespace solutions
}  // namespace tpcc

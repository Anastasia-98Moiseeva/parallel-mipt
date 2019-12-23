#pragma once

#include <tpcc/stdlike/atomic.hpp>
#include <tpcc/support/compiler.hpp>

#include <utility>
#include <cstddef>

namespace tpcc {
namespace solutions {

// Michael-Scott lock-free queue

template <typename T>
class LockFreeQueue {
  struct Node {
    T item_;
    tpcc::atomic<Node*> next_{nullptr};

    Node() {
    }

    explicit Node(T item, Node* next = nullptr)
        : item_(std::move(item)), next_(next) {
    }
  };

 public:
  LockFreeQueue() {
    Node* dummy = new Node{};
    head_ = dummy;
    tail_ = dummy;
    garbage_ = dummy;
  }

  ~LockFreeQueue() {
    Node* current_node = garbage_.load();
    while (current_node) {
      Node* next_node = current_node->next_.load();
      delete current_node;
      current_node = next_node;
    }
  }

  void Enqueue(T item) {
    Delete();
    Node* new_tail = new Node(item);
    Node* current_tail = tail_.load();
    while (true) {
      Node* current_tail = tail_.load();
      Node* next_tail = current_tail->next_.load();
      if (!next_tail) {
        if (current_tail->next_.compare_exchange_strong(next_tail, new_tail)) {
          break;
        }
      } else {
        tail_.compare_exchange_strong(current_tail, next_tail);
      }
    }
    tail_.compare_exchange_strong(current_tail, new_tail);
    count_.fetch_sub(1);
  }

  bool Dequeue(T& item) {
    Delete();
    while (true) {
      Node* current_head = head_.load();
      Node* current_tail = tail_.load();
      Node* next_head = current_head->next_.load();
      Node* next_tail = current_tail->next_.load();
      if (current_head == current_tail) {
        if (!next_head) {
          // count_.fetch_sub(1);
          return false;
        } else {
          tail_.compare_exchange_strong(current_tail, next_tail);
        }
      } else {
        if (head_.compare_exchange_strong(current_head, next_head)) {
          item = next_head->item_;
          // count_.fetch_sub(1);
          return true;
        }
      }
    }
  }

  void Delete() {
    Node* current_node = head_.load();
    Node* garbage_node = garbage_.load();
    size_t temp = count_.fetch_add(1);
    if (temp == 0 && garbage_node == garbage_.load()) {
      while (garbage_node != current_node) {
        Node* next_garbage_node = garbage_node->next_.load();
        delete garbage_node;
        garbage_node = next_garbage_node;
      }
      garbage_.store(current_node);
    }
  }

 private:
  tpcc::atomic<Node*> head_{nullptr};
  tpcc::atomic<Node*> tail_{nullptr};
  tpcc::atomic<size_t> count_{0};
  tpcc::atomic<Node*> garbage_{nullptr};
};

}  // namespace solutions
}  // namespace tpcc

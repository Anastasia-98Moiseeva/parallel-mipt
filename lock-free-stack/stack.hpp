#pragma once

#include <tpcc/stdlike/atomic.hpp>
#include <tpcc/support/compiler.hpp>

#include <utility>
#include <atomic>

namespace tpcc {
namespace solutions {

// Treiber lock-free stack

template <typename T>
class LockFreeStack {
  struct Node {
    T item_;
    tpcc::atomic<Node*> next;

    Node(T item) : item_(std::move(item)) {
    }
  };

 public:
  LockFreeStack() {
  }

  ~LockFreeStack() {
    Delete(garbage_top_);
    Delete(top_);
  }

  void Push(T item) {
    Node* new_top = new Node(item);
    Node* current_top = top_.load();
    new_top->next.store(current_top);
    while (!top_.compare_exchange_strong(current_top, new_top)) {
      new_top->next.store(current_top);
    }
  }

  bool Pop(T& item) {
    Node* current_top = top_.load();
    while (true) {
      if (!current_top) {
        return false;
      }
      if (top_.compare_exchange_strong(current_top, current_top->next.load())) {
        item = current_top->item_;
        PushInGarbage(current_top);
        return true;
      }
    }
  }

 private:
  tpcc::atomic<Node*> top_{nullptr};
  tpcc::atomic<Node*> garbage_top_{nullptr};

 private:
  void PushInGarbage(Node* node) {
    Node* current_top = garbage_top_.load();
    node->next.store(current_top);
    while (!garbage_top_.compare_exchange_strong(current_top, node)) {
      node->next.store(current_top);
    }
  }

  void Delete(tpcc::atomic<Node*>& top) {
    while (top) {
      Node* node = top.load();
      Node* next_node = node->next.load();
      delete top.load();
      top.store(next_node);
    }
  }
};

}  // namespace solutions
}  // namespace tpcc
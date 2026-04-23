//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

/**
 * Create a new LRUReplacer.
 * @param num_pages the maximum number of pages the LRUReplacer will be required to store
 */
LRUReplacer::LRUReplacer(size_t num_pages) {}

/**
 * Destroys the LRUReplacer.
 */
LRUReplacer::~LRUReplacer() = default;

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool {
  std::scoped_lock lock(lru_latch_);
  
  // 检查链表是否为空
  if (lru_list_.empty()) {
    return false;
  }
  
  // 取链表头部（最旧的）
  frame_id_t victim = lru_list_.front();
  *frame_id = victim;
  
  // 从链表和哈希表中删除
  lru_list_.pop_front();
  lru_map_.erase(victim);
  
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::scoped_lock lock(lru_latch_);
  auto it = lru_map_.find(frame_id);
  if (it != lru_map_.end()) {
    lru_list_.erase(it->second);
    lru_map_.erase(it);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::scoped_lock lock(lru_latch_);
  if (lru_map_.count(frame_id) > 0) {
    return;
  }
  lru_list_.push_back(frame_id);
  auto it = std::prev(lru_list_.end());
  lru_map_[frame_id] = it;
}

auto LRUReplacer::Size() -> size_t {
  std::scoped_lock lock(lru_latch_);
  return lru_list_.size();
}

}  // namespace bustub

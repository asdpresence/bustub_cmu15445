//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <cstddef>
#include <deque>
#include <limits>
#include <stdexcept>
namespace bustub {

constexpr frame_id_t INVALID_FRAME_ID = -1;

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  current_timestamp_ = 0;
  curr_size_ = 0;
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  size_t max_distance = 0;
  frame_id_t victim = INVALID_FRAME_ID;
  size_t victim_timestamp = std::numeric_limits<size_t>::max();
  for (const auto &frame : evictable_frames_) {
    std::deque<size_t> &history = access_history_[frame];
    size_t distance = 0;
    // 该页面访问次数大于等于k次，找到位置k，计算distance
    if (history.size() >= k_) {
      distance = current_timestamp_ - history[history.size() - k_];
      // for(size_t i=0;i<k_;i++) {it--;}
      // distance=current_timestamp_-*it;

    } else {
      // 小于k次，设为+inf

      distance = std::numeric_limits<size_t>::max();
    }

    // 距离更久，或者距离相同，最近一次的访问时间更早
    // 这包括了所有的情况
    if (distance > max_distance || (distance == max_distance && history.front() < victim_timestamp)) {
      victim = frame;
      victim_timestamp = history.front();
      max_distance = distance;
    }
  }
  if (victim != INVALID_FRAME_ID) {
    // frame_id=&victim;这样写不对
    *frame_id = victim;
    evictable_frames_.erase(victim);
    access_history_.erase(victim);
    curr_size_--;
    return true;
  }

  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  if (static_cast<size_t>(frame_id) >= replacer_size_) {
    // throw std::invalid_argument("Invalid frame_id in RecordAccess");
    return;
  }
  auto it = access_history_.find(frame_id);
  if (it == access_history_.end()) {
    access_history_[frame_id] = {current_timestamp_};
  } else {
    access_history_[frame_id].push_back(current_timestamp_);
  }
  current_timestamp_++;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);
  if (static_cast<size_t>(frame_id) >= replacer_size_) {
    //   throw std::invalid_argument("Invalid frame_id in SetEvictable");
    return;
  }
  auto it = evictable_frames_.find(frame_id);
  if (set_evictable) {
    if (it == evictable_frames_.end()) {
      curr_size_++;
      evictable_frames_.insert(frame_id);
    }
  } else {
    if (it != evictable_frames_.end()) {
      curr_size_--;
      evictable_frames_.erase(it);
    }
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  auto it = evictable_frames_.find(frame_id);
  if (it == evictable_frames_.end()) {
    // throw std::invalid_argument("Attempting to remove a non-evictable frame");
    return;
  }
  evictable_frames_.erase(it);
  access_history_.erase(frame_id);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lock(latch_);
  // return curr_size_;
  return evictable_frames_.size();
}
}  // namespace bustub

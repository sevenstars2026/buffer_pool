// :bustub-keep-private:
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// arc_replacer.cpp
//
// Identification: src/buffer/arc_replacer.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/arc_replacer.h"
#include <algorithm>
#include "common/exception.h"

namespace bustub {

ArcReplacer::ArcReplacer(size_t num_frames) : replacer_size_(num_frames) {}

auto ArcReplacer::EvictFromList(std::list<frame_id_t> &list, ArcStatus list_status, std::list<page_id_t> &ghost_list,
                                ArcStatus ghost_status) -> std::optional<frame_id_t> {
  for (auto it = list.begin(); it != list.end(); ++it) {
    const auto frame_id = *it;
    auto alive_it = alive_map_.find(frame_id);
    if (alive_it == alive_map_.end() || !alive_it->second->evictable_) {
      continue;
    }

    const auto page_id = alive_it->second->page_id_;

    if (list_status == ArcStatus::MRU) {
      mru_pos_.erase(frame_id);
    } else {
      mfu_pos_.erase(frame_id);
    }
    list.erase(it);
    alive_map_.erase(alive_it);
    curr_size_--;

    AddToGhostList(ghost_list, ghost_status == ArcStatus::MRU_GHOST ? mru_ghost_pos_ : mfu_ghost_pos_, page_id,
                   ghost_status);
    TrimGhostLists();
    return frame_id;
  }
  return std::nullopt;
}

void ArcReplacer::AddToGhostList(std::list<page_id_t> &ghost_list,
                                 std::unordered_map<page_id_t, std::list<page_id_t>::iterator> &ghost_pos,
                                 page_id_t page_id, ArcStatus ghost_status) {
  if (auto it = mru_ghost_pos_.find(page_id); it != mru_ghost_pos_.end()) {
    mru_ghost_.erase(it->second);
    mru_ghost_pos_.erase(it);
    ghost_map_.erase(page_id);
  }
  if (auto it = mfu_ghost_pos_.find(page_id); it != mfu_ghost_pos_.end()) {
    mfu_ghost_.erase(it->second);
    mfu_ghost_pos_.erase(it);
    ghost_map_.erase(page_id);
  }

  ghost_list.push_back(page_id);
  auto iter = std::prev(ghost_list.end());
  ghost_pos[page_id] = iter;
  ghost_map_[page_id] = std::make_shared<FrameStatus>(page_id, INVALID_FRAME_ID, false, ghost_status);
}

void ArcReplacer::RemoveAliveFrame(frame_id_t frame_id) {
  auto alive_it = alive_map_.find(frame_id);
  if (alive_it == alive_map_.end()) {
    return;
  }

  if (alive_it->second->arc_status_ == ArcStatus::MRU) {
    auto it = mru_pos_.find(frame_id);
    if (it != mru_pos_.end()) {
      mru_.erase(it->second);
      mru_pos_.erase(it);
    }
  } else if (alive_it->second->arc_status_ == ArcStatus::MFU) {
    auto it = mfu_pos_.find(frame_id);
    if (it != mfu_pos_.end()) {
      mfu_.erase(it->second);
      mfu_pos_.erase(it);
    }
  }

  if (alive_it->second->evictable_ && curr_size_ > 0) {
    curr_size_--;
  }
  alive_map_.erase(alive_it);
}

void ArcReplacer::RemoveGhostPage(page_id_t page_id) {
  if (auto it = mru_ghost_pos_.find(page_id); it != mru_ghost_pos_.end()) {
    mru_ghost_.erase(it->second);
    mru_ghost_pos_.erase(it);
  }
  if (auto it = mfu_ghost_pos_.find(page_id); it != mfu_ghost_pos_.end()) {
    mfu_ghost_.erase(it->second);
    mfu_ghost_pos_.erase(it);
  }
  ghost_map_.erase(page_id);
}

void ArcReplacer::TrimGhostLists() {
  while (mru_ghost_.size() > replacer_size_) {
    auto page_id = mru_ghost_.front();
    mru_ghost_.pop_front();
    mru_ghost_pos_.erase(page_id);
    ghost_map_.erase(page_id);
  }

  while (mfu_ghost_.size() > replacer_size_) {
    auto page_id = mfu_ghost_.front();
    mfu_ghost_.pop_front();
    mfu_ghost_pos_.erase(page_id);
    ghost_map_.erase(page_id);
  }

  while (mru_.size() + mfu_.size() + mru_ghost_.size() + mfu_ghost_.size() > 2 * replacer_size_) {
    if (!mfu_ghost_.empty()) {
      auto page_id = mfu_ghost_.front();
      mfu_ghost_.pop_front();
      mfu_ghost_pos_.erase(page_id);
      ghost_map_.erase(page_id);
      continue;
    }
    if (!mru_ghost_.empty()) {
      auto page_id = mru_ghost_.front();
      mru_ghost_.pop_front();
      mru_ghost_pos_.erase(page_id);
      ghost_map_.erase(page_id);
      continue;
    }
    break;
  }
}

auto ArcReplacer::Evict() -> std::optional<frame_id_t> {
  std::scoped_lock lock(latch_);
  if (curr_size_ == 0) {
    return std::nullopt;
  }

  const bool prefer_mru = mru_.size() >= mru_target_size_;
  if (prefer_mru) {
    if (auto victim = EvictFromList(mru_, ArcStatus::MRU, mru_ghost_, ArcStatus::MRU_GHOST); victim.has_value()) {
      return victim;
    }
    return EvictFromList(mfu_, ArcStatus::MFU, mfu_ghost_, ArcStatus::MFU_GHOST);
  }

  if (auto victim = EvictFromList(mfu_, ArcStatus::MFU, mfu_ghost_, ArcStatus::MFU_GHOST); victim.has_value()) {
    return victim;
  }
  return EvictFromList(mru_, ArcStatus::MRU, mru_ghost_, ArcStatus::MRU_GHOST);
}

void ArcReplacer::RecordAccess(frame_id_t frame_id, page_id_t page_id, [[maybe_unused]] AccessType access_type) {
  std::scoped_lock lock(latch_);
  if (frame_id < 0) {
    throw Exception("frame id is invalid");
  }

  if (auto alive_it = alive_map_.find(frame_id); alive_it != alive_map_.end()) {
    auto &status = alive_it->second;
    status->page_id_ = page_id;

    if (status->arc_status_ == ArcStatus::MRU) {
      auto pos = mru_pos_.find(frame_id);
      if (pos != mru_pos_.end()) {
        mru_.erase(pos->second);
        mru_pos_.erase(pos);
      }
      mfu_.push_back(frame_id);
      mfu_pos_[frame_id] = std::prev(mfu_.end());
      status->arc_status_ = ArcStatus::MFU;
      return;
    }

    if (status->arc_status_ == ArcStatus::MFU) {
      auto pos = mfu_pos_.find(frame_id);
      if (pos != mfu_pos_.end()) {
        mfu_.erase(pos->second);
      }
      mfu_.push_back(frame_id);
      mfu_pos_[frame_id] = std::prev(mfu_.end());
      return;
    }
  }

  if (mru_ghost_pos_.find(page_id) != mru_ghost_pos_.end()) {
    const size_t delta = mfu_ghost_.empty()
                             ? 1
                             : (mfu_ghost_.size() >= mru_ghost_.size() ? 1
                                                                        : std::max<size_t>(1, mru_ghost_.size() / mfu_ghost_.size()));
    mru_target_size_ = std::min(replacer_size_, mru_target_size_ + delta);
    RemoveGhostPage(page_id);

    RemoveAliveFrame(frame_id);
    auto status = std::make_shared<FrameStatus>(page_id, frame_id, false, ArcStatus::MFU);
    alive_map_[frame_id] = status;
    mfu_.push_back(frame_id);
    mfu_pos_[frame_id] = std::prev(mfu_.end());
    TrimGhostLists();
    return;
  }

  if (mfu_ghost_pos_.find(page_id) != mfu_ghost_pos_.end()) {
    const size_t delta = mru_ghost_.empty()
                             ? 1
                             : (mru_ghost_.size() >= mfu_ghost_.size() ? 1
                                                                        : std::max<size_t>(1, mfu_ghost_.size() / mru_ghost_.size()));
    mru_target_size_ = mru_target_size_ > delta ? mru_target_size_ - delta : 0;
    RemoveGhostPage(page_id);

    RemoveAliveFrame(frame_id);
    auto status = std::make_shared<FrameStatus>(page_id, frame_id, false, ArcStatus::MFU);
    alive_map_[frame_id] = status;
    mfu_.push_back(frame_id);
    mfu_pos_[frame_id] = std::prev(mfu_.end());
    TrimGhostLists();
    return;
  }

  if (mru_.size() + mru_ghost_.size() == replacer_size_) {
    if (mru_.size() < replacer_size_) {
      if (!mru_ghost_.empty()) {
        const auto victim_page = mru_ghost_.front();
        mru_ghost_.pop_front();
        mru_ghost_pos_.erase(victim_page);
        ghost_map_.erase(victim_page);
      }
    } else {
      static_cast<void>(EvictFromList(mru_, ArcStatus::MRU, mru_ghost_, ArcStatus::MRU_GHOST));
    }
  } else if (mru_.size() + mru_ghost_.size() < replacer_size_) {
    const auto total_size = mru_.size() + mfu_.size() + mru_ghost_.size() + mfu_ghost_.size();
    if (total_size >= replacer_size_ && total_size >= 2 * replacer_size_ && !mfu_ghost_.empty()) {
      const auto victim_page = mfu_ghost_.front();
      mfu_ghost_.pop_front();
      mfu_ghost_pos_.erase(victim_page);
      ghost_map_.erase(victim_page);
    }
  }

  RemoveAliveFrame(frame_id);
  RemoveGhostPage(page_id);

  auto status = std::make_shared<FrameStatus>(page_id, frame_id, false, ArcStatus::MRU);
  alive_map_[frame_id] = status;
  mru_.push_back(frame_id);
  mru_pos_[frame_id] = std::prev(mru_.end());
  TrimGhostLists();
}

void ArcReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock lock(latch_);
  if (frame_id < 0) {
    throw Exception("frame id is invalid");
  }

  auto it = alive_map_.find(frame_id);
  if (it == alive_map_.end()) {
    return;
  }

  if (it->second->evictable_ == set_evictable) {
    return;
  }

  it->second->evictable_ = set_evictable;
  if (set_evictable) {
    curr_size_++;
  } else {
    curr_size_--;
  }
}

void ArcReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock lock(latch_);
  if (frame_id < 0) {
    throw Exception("frame id is invalid");
  }

  auto it = alive_map_.find(frame_id);
  if (it == alive_map_.end()) {
    return;
  }
  if (!it->second->evictable_) {
    throw Exception("cannot remove a non-evictable frame");
  }

  if (it->second->arc_status_ == ArcStatus::MRU) {
    auto pos = mru_pos_.find(frame_id);
    if (pos != mru_pos_.end()) {
      mru_.erase(pos->second);
      mru_pos_.erase(pos);
    }
  } else {
    auto pos = mfu_pos_.find(frame_id);
    if (pos != mfu_pos_.end()) {
      mfu_.erase(pos->second);
      mfu_pos_.erase(pos);
    }
  }

  alive_map_.erase(it);
  curr_size_--;
}

auto ArcReplacer::Size() -> size_t {
  std::scoped_lock lock(latch_);
  return curr_size_;
}

}  // namespace bustub

//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include <vector>
#include "common/config.h"
#include "common/macros.h"

namespace bustub {

FrameHeader::FrameHeader(frame_id_t frame_id) : frame_id_(frame_id), data_(BUSTUB_PAGE_SIZE, 0) { Reset(); }

auto FrameHeader::GetData() const -> const char * { return data_.data(); }

auto FrameHeader::GetDataMut() -> char * { return data_.data(); }

void FrameHeader::Reset() {
  std::fill(data_.begin(), data_.end(), 0);
  pin_count_.store(0);
  is_dirty_ = false;
  page_id_ = std::nullopt;
}

BufferPoolManager::BufferPoolManager(size_t num_frames, DiskManager *disk_manager, LogManager *log_manager)
    : num_frames_(num_frames),
      next_page_id_(0),
      bpm_latch_(std::make_shared<std::mutex>()),
      replacer_(std::make_shared<ArcReplacer>(num_frames)),
      disk_scheduler_(std::make_shared<DiskScheduler>(disk_manager)),
      log_manager_(log_manager) {
  std::scoped_lock latch(*bpm_latch_);
  next_page_id_.store(0);
  frames_.reserve(num_frames_);
  page_table_.reserve(num_frames_);

  for (size_t i = 0; i < num_frames_; i++) {
    frames_.push_back(std::make_shared<FrameHeader>(static_cast<frame_id_t>(i)));
    free_frames_.push_back(static_cast<frame_id_t>(i));
  }
}

BufferPoolManager::~BufferPoolManager() = default;

auto BufferPoolManager::Size() const -> size_t { return num_frames_; }

auto BufferPoolManager::NewPage() -> page_id_t { return next_page_id_.fetch_add(1); }

auto BufferPoolManager::FlushFrameUnsafe(const std::shared_ptr<FrameHeader> &frame) -> bool {
  if (!frame->page_id_.has_value()) {
    return false;
  }

  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  DiskRequest request{/*is_write=*/true, frame->GetDataMut(), frame->page_id_.value(), std::move(promise)};
  std::vector<DiskRequest> requests;
  requests.push_back(std::move(request));
  disk_scheduler_->Schedule(requests);

  if (!future.get()) {
    return false;
  }

  frame->is_dirty_ = false;
  return true;
}

auto BufferPoolManager::AcquireFrameForPage(page_id_t page_id, AccessType access_type) -> std::shared_ptr<FrameHeader> {
  if (auto it = page_table_.find(page_id); it != page_table_.end()) {
    const auto frame_id = it->second;
    auto frame = frames_[frame_id];
    frame->pin_count_.fetch_add(1);
    replacer_->RecordAccess(frame_id, page_id, access_type);
    replacer_->SetEvictable(frame_id, false);
    return frame;
  }

  frame_id_t frame_id = INVALID_FRAME_ID;
  if (!free_frames_.empty()) {
    frame_id = free_frames_.front();
    free_frames_.pop_front();
  } else {
    auto victim = replacer_->Evict();
    if (!victim.has_value()) {
      return nullptr;
    }
    frame_id = victim.value();
  }

  auto frame = frames_[frame_id];
  if (frame->page_id_.has_value()) {
    if (frame->is_dirty_ && !FlushFrameUnsafe(frame)) {
      return nullptr;
    }
    page_table_.erase(frame->page_id_.value());
  }

  frame->Reset();
  frame->page_id_ = page_id;

  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  DiskRequest request{/*is_write=*/false, frame->GetDataMut(), page_id, std::move(promise)};
  std::vector<DiskRequest> requests;
  requests.push_back(std::move(request));
  disk_scheduler_->Schedule(requests);
  if (!future.get()) {
    frame->Reset();
    free_frames_.push_back(frame_id);
    return nullptr;
  }

  page_table_[page_id] = frame_id;
  frame->pin_count_.store(1);
  replacer_->RecordAccess(frame_id, page_id, access_type);
  replacer_->SetEvictable(frame_id, false);
  return frame;
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::scoped_lock lock(*bpm_latch_);

  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    disk_scheduler_->DeallocatePage(page_id);
    return true;
  }

  const auto frame_id = it->second;
  auto frame = frames_[frame_id];
  if (frame->pin_count_.load() > 0) {
    return false;
  }

  replacer_->Remove(frame_id);
  page_table_.erase(it);
  frame->Reset();
  free_frames_.push_back(frame_id);
  disk_scheduler_->DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::CheckedWritePage(page_id_t page_id, AccessType access_type) -> std::optional<WritePageGuard> {
  std::shared_ptr<FrameHeader> frame;
  {
    std::scoped_lock lock(*bpm_latch_);
    frame = AcquireFrameForPage(page_id, access_type);
    if (frame == nullptr) {
      return std::nullopt;
    }
  }
  return WritePageGuard(page_id, frame, replacer_, bpm_latch_, disk_scheduler_);
}

auto BufferPoolManager::CheckedReadPage(page_id_t page_id, AccessType access_type) -> std::optional<ReadPageGuard> {
  std::shared_ptr<FrameHeader> frame;
  {
    std::scoped_lock lock(*bpm_latch_);
    frame = AcquireFrameForPage(page_id, access_type);
    if (frame == nullptr) {
      return std::nullopt;
    }
  }
  return ReadPageGuard(page_id, frame, replacer_, bpm_latch_, disk_scheduler_);
}

auto BufferPoolManager::WritePage(page_id_t page_id, AccessType access_type) -> WritePageGuard {
  auto guard_opt = CheckedWritePage(page_id, access_type);

  if (!guard_opt.has_value()) {
    fmt::println(stderr, "\n`CheckedWritePage` failed to bring in page {}\n", page_id);
    std::abort();
  }

  return std::move(guard_opt).value();
}

auto BufferPoolManager::ReadPage(page_id_t page_id, AccessType access_type) -> ReadPageGuard {
  auto guard_opt = CheckedReadPage(page_id, access_type);

  if (!guard_opt.has_value()) {
    fmt::println(stderr, "\n`CheckedReadPage` failed to bring in page {}\n", page_id);
    std::abort();
  }

  return std::move(guard_opt).value();
}

auto BufferPoolManager::FlushPageUnsafe(page_id_t page_id) -> bool {
  std::scoped_lock lock(*bpm_latch_);
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }
  return FlushFrameUnsafe(frames_[it->second]);
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::scoped_lock lock(*bpm_latch_);
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }

  auto frame = frames_[it->second];
  std::unique_lock<std::shared_mutex> page_lock(frame->rwlatch_);
  return FlushFrameUnsafe(frame);
}

void BufferPoolManager::FlushAllPagesUnsafe() {
  std::scoped_lock lock(*bpm_latch_);
  for (const auto &[page_id, frame_id] : page_table_) {
    static_cast<void>(page_id);
    static_cast<void>(FlushFrameUnsafe(frames_[frame_id]));
  }
}

void BufferPoolManager::FlushAllPages() {
  std::scoped_lock lock(*bpm_latch_);
  for (const auto &[page_id, frame_id] : page_table_) {
    static_cast<void>(page_id);
    auto frame = frames_[frame_id];
    std::unique_lock<std::shared_mutex> page_lock(frame->rwlatch_);
    static_cast<void>(FlushFrameUnsafe(frame));
  }
}

auto BufferPoolManager::GetPinCount(page_id_t page_id) -> std::optional<size_t> {
  std::scoped_lock lock(*bpm_latch_);

  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return std::nullopt;
  }

  frame_id_t frame_id = it->second;
  return frames_[frame_id]->pin_count_.load();
}

}  // namespace bustub

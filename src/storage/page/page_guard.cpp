//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page_guard.cpp
//
// Identification: src/storage/page/page_guard.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/page_guard.h"
#include <vector>
#include "common/macros.h"

namespace bustub {

ReadPageGuard::ReadPageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame, std::shared_ptr<ArcReplacer> replacer,
                             std::shared_ptr<std::mutex> bpm_latch, std::shared_ptr<DiskScheduler> disk_scheduler)
    : page_id_(page_id),
      frame_(std::move(frame)),
      replacer_(std::move(replacer)),
      bpm_latch_(std::move(bpm_latch)),
      disk_scheduler_(std::move(disk_scheduler)) {
  frame_->rwlatch_.lock_shared();
  is_valid_ = true;
}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept
    : page_id_(that.page_id_),
      frame_(std::move(that.frame_)),
      replacer_(std::move(that.replacer_)),
      bpm_latch_(std::move(that.bpm_latch_)),
      disk_scheduler_(std::move(that.disk_scheduler_)),
      is_valid_(that.is_valid_) {
  that.page_id_ = INVALID_PAGE_ID;
  that.is_valid_ = false;
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this == &that) {
    return *this;
  }
  Drop();
  page_id_ = that.page_id_;
  frame_ = std::move(that.frame_);
  replacer_ = std::move(that.replacer_);
  bpm_latch_ = std::move(that.bpm_latch_);
  disk_scheduler_ = std::move(that.disk_scheduler_);
  is_valid_ = that.is_valid_;

  that.page_id_ = INVALID_PAGE_ID;
  that.is_valid_ = false;
  return *this;
}

auto ReadPageGuard::GetPageId() const -> page_id_t {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return page_id_;
}

auto ReadPageGuard::GetData() const -> const char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return frame_->GetData();
}

auto ReadPageGuard::IsDirty() const -> bool {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return frame_->is_dirty_;
}

void ReadPageGuard::Flush() {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();

  DiskRequest request{/*is_write=*/true, const_cast<char *>(frame_->GetData()), page_id_, std::move(promise)};
  std::vector<DiskRequest> requests;
  requests.push_back(std::move(request));
  disk_scheduler_->Schedule(requests);

  if (future.get()) {
    frame_->is_dirty_ = false;
  }
}

void ReadPageGuard::Drop() {
  if (!is_valid_) {
    return;
  }

  frame_->rwlatch_.unlock_shared();
  {
    std::scoped_lock lock(*bpm_latch_);
    const auto old_pin_count = frame_->pin_count_.fetch_sub(1);
    BUSTUB_ENSURE(old_pin_count > 0, "pin count underflow in ReadPageGuard::Drop");
    if (old_pin_count == 1) {
      replacer_->SetEvictable(frame_->frame_id_, true);
    }
  }

  frame_.reset();
  replacer_.reset();
  bpm_latch_.reset();
  disk_scheduler_.reset();
  page_id_ = INVALID_PAGE_ID;
  is_valid_ = false;
}

ReadPageGuard::~ReadPageGuard() { Drop(); }

WritePageGuard::WritePageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame,
                               std::shared_ptr<ArcReplacer> replacer, std::shared_ptr<std::mutex> bpm_latch,
                               std::shared_ptr<DiskScheduler> disk_scheduler)
    : page_id_(page_id),
      frame_(std::move(frame)),
      replacer_(std::move(replacer)),
      bpm_latch_(std::move(bpm_latch)),
      disk_scheduler_(std::move(disk_scheduler)) {
  frame_->rwlatch_.lock();
  frame_->is_dirty_ = true;
  is_valid_ = true;
}

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept
    : page_id_(that.page_id_),
      frame_(std::move(that.frame_)),
      replacer_(std::move(that.replacer_)),
      bpm_latch_(std::move(that.bpm_latch_)),
      disk_scheduler_(std::move(that.disk_scheduler_)),
      is_valid_(that.is_valid_) {
  that.page_id_ = INVALID_PAGE_ID;
  that.is_valid_ = false;
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this == &that) {
    return *this;
  }
  Drop();
  page_id_ = that.page_id_;
  frame_ = std::move(that.frame_);
  replacer_ = std::move(that.replacer_);
  bpm_latch_ = std::move(that.bpm_latch_);
  disk_scheduler_ = std::move(that.disk_scheduler_);
  is_valid_ = that.is_valid_;

  that.page_id_ = INVALID_PAGE_ID;
  that.is_valid_ = false;
  return *this;
}

auto WritePageGuard::GetPageId() const -> page_id_t {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return page_id_;
}

auto WritePageGuard::GetData() const -> const char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return frame_->GetData();
}

auto WritePageGuard::GetDataMut() -> char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  frame_->is_dirty_ = true;
  return frame_->GetDataMut();
}

auto WritePageGuard::IsDirty() const -> bool {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return frame_->is_dirty_;
}

void WritePageGuard::Flush() {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();

  DiskRequest request{/*is_write=*/true, frame_->GetDataMut(), page_id_, std::move(promise)};
  std::vector<DiskRequest> requests;
  requests.push_back(std::move(request));
  disk_scheduler_->Schedule(requests);

  if (future.get()) {
    frame_->is_dirty_ = false;
  }
}

void WritePageGuard::Drop() {
  if (!is_valid_) {
    return;
  }

  frame_->rwlatch_.unlock();
  {
    std::scoped_lock lock(*bpm_latch_);
    const auto old_pin_count = frame_->pin_count_.fetch_sub(1);
    BUSTUB_ENSURE(old_pin_count > 0, "pin count underflow in WritePageGuard::Drop");
    if (old_pin_count == 1) {
      replacer_->SetEvictable(frame_->frame_id_, true);
    }
  }

  frame_.reset();
  replacer_.reset();
  bpm_latch_.reset();
  disk_scheduler_.reset();
  page_id_ = INVALID_PAGE_ID;
  is_valid_ = false;
}

WritePageGuard::~WritePageGuard() { Drop(); }

}  // namespace bustub

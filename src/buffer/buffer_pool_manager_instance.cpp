//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"
#include <cstddef>
#include <mutex>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager_instance.cpp`.");
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);

  //还有空余帧
  if (!free_list_.empty()) {
    //取一个空余帧
    frame_id_t new_frame_id = free_list_.back();
    free_list_.pop_back();

    Page &page = pages_[new_frame_id];
    //哈希表中删去旧页
    page_table_->Remove(page.GetPageId());
    // reset the memory and metadata for the new page
    page.page_id_ = AllocatePage();
    page.ResetMemory();
    page.is_dirty_ = false;
    page.pin_count_ = 1;
    // pin该frame
    replacer_->SetEvictable(new_frame_id, false);
    //替换池中记录：访问该帧
    replacer_->RecordAccess(new_frame_id);
    //维护哈希表
    page_table_->Insert(page.page_id_, new_frame_id);

    //维护指针及返回值
    *page_id = page.page_id_;
    return &page;
  }

  frame_id_t new_frame_id;
  //来到替换池中寻找是否有可逐出的帧
  if (replacer_->Evict(&new_frame_id)) {
    //取该帧中的旧页
    Page &page = pages_[new_frame_id];
    //旧页是否脏,若脏，写入disk
    if (page.IsDirty()) {
      disk_manager_->WritePage(page.GetPageId(), page.GetData());
    }
    //哈希表中删去旧页
    page_table_->Remove(page.GetPageId());
    // reset the memory and metadata for the new page
    page.page_id_ = AllocatePage();
    page.ResetMemory();
    page.is_dirty_ = false;
    page.pin_count_ = 1;
    // pin该frame
    replacer_->SetEvictable(new_frame_id, false);
    //替换池中记录：访问该帧
    replacer_->RecordAccess(new_frame_id);
    //维护哈希表
    page_table_->Insert(page.page_id_, new_frame_id);
    //维护指针及返回值
    *page_id = page.page_id_;
    return &page;
  }
  //没有空余帧并且没有可逐出的帧
  return nullptr;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);
  frame_id_t frame_id;
  //该页在buffer pool里
  if (page_table_->Find(page_id, frame_id)) {
    //访问
    replacer_->SetEvictable(frame_id, false);
    replacer_->RecordAccess(frame_id);
    Page &page = pages_[frame_id];
    page.pin_count_++;
    return &page;
  }

  //还有空余帧
  if (!free_list_.empty()) {
    frame_id_t new_frame_id = free_list_.back();
    free_list_.pop_back();
    //用disk读取到的新页，代替旧页
    //取旧页
    Page &page = pages_[new_frame_id];

    //旧页是否脏,若脏，写入disk
    if (page.IsDirty()) {
      disk_manager_->WritePage(page.GetPageId(), page.GetData());
    }
    //哈希表中删去旧页
    page_table_->Remove(page.GetPageId());
    //读取新页数据
    disk_manager_->ReadPage(page_id, page.data_);
    //更新页面元数据
    page.page_id_ = page_id;
    page.pin_count_ = 1;
    page.is_dirty_ = false;

    //更新数据结构
    page_table_->Insert(page_id, new_frame_id);
    replacer_->SetEvictable(new_frame_id, false);
    replacer_->RecordAccess(new_frame_id);

    return &page;
  }

  //从缓存池逐出
  frame_id_t new_frame_id;
  if (replacer_->Evict(&new_frame_id)) {
    Page &page = pages_[new_frame_id];
    //旧页是否脏,若脏，写入disk
    if (page.IsDirty()) {
      disk_manager_->WritePage(page.GetPageId(), page.GetData());
    }
    //哈希表中删去旧页
    page_table_->Remove(page.GetPageId());
    //读取新页数据
    disk_manager_->ReadPage(page_id, page.data_);
    //更新页面元数据
    page.page_id_ = page_id;
    page.pin_count_ = 1;
    page.is_dirty_ = false;

    //更新数据结构
    page_table_->Insert(page_id, new_frame_id);
    replacer_->SetEvictable(new_frame_id, false);
    replacer_->RecordAccess(new_frame_id);

    return &page;
  }

  return nullptr;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }

  Page &page = pages_[frame_id];
  if (page.GetPinCount() <= 0) {
    return false;
  }

  page.pin_count_--;
  if (page.GetPinCount() == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  if (is_dirty) {
    page.is_dirty_ = true;
  }

  return true;
}
auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }

  Page &page = pages_[frame_id];
  disk_manager_->WritePage(page_id, page.GetData());

  page.is_dirty_ = false;

  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::lock_guard<std::mutex> lock(latch_);
  for (size_t i = 0; i < pool_size_; i++) {
    page_id_t page_id = pages_[i].GetPageId();
    FlushPgImp(page_id);
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return true;
  }

  //取该帧存的page
  Page &page = pages_[frame_id];

  if (page.GetPinCount() > 0) {
    return false;
  }
  //如果是脏页，需要将数据写入磁盘
  // if(page.IsDirty()){
  //   disk_manager_->WritePage(page_id, page.GetData());
  // }

  //将（页编号，帧编号）键值对从哈希表移除
  page_table_->Remove(page_id);
  //将对应帧从替换池移除,注意这里应是完全移除，因为该页已从内存池中释放
  replacer_->Remove(frame_id);
  //将该帧放入空闲帧列表
  free_list_.push_back(frame_id);

  //重置页的内存和数据
  page.ResetMemory();
  page.page_id_ = INVALID_PAGE_ID;
  page.pin_count_ = 0;
  page.is_dirty_ = false;

  //硬盘上释放该页
  DeallocatePage(page_id);
  return true;

  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub

/*
pages_：线程池的帧数组
replacer_：线程池的帧的替换池
*/
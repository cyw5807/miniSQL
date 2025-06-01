#include "buffer/buffer_pool_manager.h"

#include "glog/logging.h"
#include "page/bitmap_page.h"

static const char EMPTY_PAGE_DATA[PAGE_SIZE] = {0};

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page : page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::FetchPage(page_id_t page_id) {
    std::lock_guard<std::recursive_mutex> guard(latch_); 
    auto page_table_iter = page_table_.find(page_id);

    if (page_table_iter != page_table_.end()) {
        frame_id_t frame_id_in_pool = page_table_iter->second; 
        Page* page_in_frame = &pages_[frame_id_in_pool];     
        page_in_frame->pin_count_++;
        replacer_->Pin(frame_id_in_pool); 
        return page_in_frame;
    }


    frame_id_t frame_id_to_use; 

    if (!free_list_.empty()) {
        frame_id_to_use = free_list_.front(); 
        free_list_.pop_front();              
    } else { 
        if (!replacer_->Victim(&frame_id_to_use)) { 
            //LOG(WARNING) << "BufferPoolManager::FetchPage: No victim available from replacer.";
            return nullptr; 
        }
        Page* victim_page_ptr = &pages_[frame_id_to_use];
        if (victim_page_ptr->IsDirty()) {
            disk_manager_->WritePage(victim_page_ptr->GetPageId(), victim_page_ptr->GetData());
            victim_page_ptr->is_dirty_ = false; 
        }
        page_table_.erase(victim_page_ptr->GetPageId()); 

    }

    Page* new_page_in_frame = &pages_[frame_id_to_use];

    new_page_in_frame->page_id_ = page_id;        
    new_page_in_frame->pin_count_ = 1;             
    new_page_in_frame->is_dirty_ = false;      
    new_page_in_frame->ResetMemory();              
    
    page_table_[page_id] = frame_id_to_use;

    disk_manager_->ReadPage(page_id, new_page_in_frame->GetData()); 

    replacer_->Pin(frame_id_to_use);      

    return new_page_in_frame; 

}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::NewPage(page_id_t &page_id) {

      std::lock_guard<std::recursive_mutex> guard(latch_);

    frame_id_t frame_id_to_use;

    if (free_list_.empty() && replacer_->Size() == 0) {
       page_id = INVALID_PAGE_ID;
        return nullptr;
    }

    if (!free_list_.empty()) {
        frame_id_to_use = free_list_.front(); 
        free_list_.pop_front();             
        } else { 
        if (!replacer_->Victim(&frame_id_to_use)) {
            page_id = INVALID_PAGE_ID;
            return nullptr; 
        }
        Page* victim_page_ptr = &pages_[frame_id_to_use];
       
        if (victim_page_ptr->IsDirty()) {
            disk_manager_->WritePage(victim_page_ptr->GetPageId(), victim_page_ptr->GetData());
            victim_page_ptr->is_dirty_ = false;
        }
      
        page_table_.erase(victim_page_ptr->GetPageId());
          }

    page_id_t new_on_disk_page_id = disk_manager_->AllocatePage();
    if (new_on_disk_page_id == INVALID_PAGE_ID) {
        LOG(ERROR) << "BufferPoolManager::NewPage (Optimized): DiskManager failed to allocate a new page on disk.";
        pages_[frame_id_to_use].page_id_ = INVALID_PAGE_ID; 
        pages_[frame_id_to_use].pin_count_ = 0;
        pages_[frame_id_to_use].is_dirty_ = false;

        free_list_.push_front(frame_id_to_use);

        page_id = INVALID_PAGE_ID;
        return nullptr;
    }

    Page* new_page_in_frame = &pages_[frame_id_to_use];
    
    new_page_in_frame->page_id_ = new_on_disk_page_id; 
    
    new_page_in_frame->pin_count_ = 1;
 
    new_page_in_frame->is_dirty_ = false; 
    new_page_in_frame->ResetMemory(); 

    page_table_[new_on_disk_page_id] = frame_id_to_use;

    replacer_->Pin(frame_id_to_use); 

    page_id = new_on_disk_page_id;

    return new_page_in_frame;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) {
  std::lock_guard<std::recursive_mutex> guard(latch_);
    auto page_table_iter = page_table_.find(page_id);
    if (page_table_iter == page_table_.end()) {
        disk_manager_->DeAllocatePage(page_id);
        return true; // 操作视为完成，因为缓冲池无事可做
    }

    frame_id_t frame_id_of_page = page_table_iter->second;
    Page* page_to_delete_ptr = &pages_[frame_id_of_page];
    if (page_to_delete_ptr->GetPinCount() != 0) {
       
        return false; 
    }

    page_table_.erase(page_table_iter);

    page_to_delete_ptr->ResetMemory();
    page_to_delete_ptr->page_id_ = INVALID_PAGE_ID;
    page_to_delete_ptr->pin_count_ = 0;
    page_to_delete_ptr->is_dirty_ = false; 
    free_list_.push_back(frame_id_of_page);

    replacer_->Pin(frame_id_of_page);

    disk_manager_->DeAllocatePage(page_id);
    return true;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {

    std::lock_guard<std::recursive_mutex> guard(latch_); // 保护共享数据结构

    auto page_table_iter = page_table_.find(page_id);
    if (page_table_iter == page_table_.end()) {
      
        return false;
    }
    frame_id_t frame_id_to_unpin = page_table_iter->second;
    Page* page_to_unpin_ptr = &pages_[frame_id_to_unpin];
    if (page_to_unpin_ptr->GetPinCount() <= 0) {

        return false; 
    }
    page_to_unpin_ptr->pin_count_--; 
    if (is_dirty) {
        page_to_unpin_ptr->is_dirty_ = true;
  
    }

    if (page_to_unpin_ptr->GetPinCount() == 0) {
        replacer_->Unpin(frame_id_to_unpin);
       
    }

    return true;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) {
  std::lock_guard<std::recursive_mutex> guard(latch_);

    auto page_table_iter = page_table_.find(page_id);
    if (page_table_iter == page_table_.end()) {
    
        return false;
    }

    frame_id_t frame_id_to_flush = page_table_iter->second;
    Page* page_to_flush_ptr = &pages_[frame_id_to_flush];

    if (page_to_flush_ptr->IsDirty()) {
      
        disk_manager_->WritePage(page_id, page_to_flush_ptr->GetData());
    
        page_to_flush_ptr->is_dirty_ = false;
      
    } 
    return true;
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(__attribute__((unused)) page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}
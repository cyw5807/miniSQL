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
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
    // 获取互斥锁，保护缓冲池的内部数据结构
    std::lock_guard<std::recursive_mutex> guard(latch_); 

    // 不过我们已将其替换为直接调用 disk_manager_->WritePage()。

    ///LOG(INFO) << "BufferPoolManager::FetchPage: Attempting to fetch page_id " << page_id;

    // 1. 搜索页面表查找请求的页面(P)
    auto page_table_iter = page_table_.find(page_id);

    //  1.1 如果 P 存在，固定它并立即返回
    if (page_table_iter != page_table_.end()) {
        frame_id_t frame_id_in_pool = page_table_iter->second; // 获取帧ID
        Page* page_in_frame = &pages_[frame_id_in_pool];     // 获取Page指针

        // pages[frame_id].pin_count += 1
        page_in_frame->pin_count_++;
        // replacer.Pin(frame_id)
        replacer_->Pin(frame_id_in_pool); // 通知替换器此帧被固定

        // LOG(INFO) << "BufferPoolManager::FetchPage: Page " << page_id << " found in buffer pool (frame " 
        //           << frame_id_in_pool << "). Pin count incremented to " << page_in_frame->pin_count_;
        //return pages[frame_id]
        return page_in_frame;
    }

    //  P 不存在于缓冲池中
    //LOG(INFO) << "BufferPoolManager::FetchPage: Page " << page_id << " not found in buffer pool. Attempting to find/evict a frame.";

    //  1.2 从空闲列表或替换器中找到一个替换页面(R)
    frame_id_t frame_id_to_use; // 将用于新页的帧ID

    // if not free_list.isEmpty():
    if (!free_list_.empty()) {
        frame_id_to_use = free_list_.front(); 
        free_list_.pop_front();              
        // LOG(INFO) << "BufferPoolManager::FetchPage: Using free frame " << frame_id_to_use 
        //           << " from free_list for page " << page_id << ".";
    } else { //  else (free_list_ is empty)
        //  从替换器中获取
        if (!replacer_->Victim(&frame_id_to_use)) { 
            LOG(WARNING) << "BufferPoolManager::FetchPage: No free frames and no victim available from replacer "
                         << "(all evictable pages might be pinned). Cannot fetch page " << page_id << ".";
            return nullptr; 
        }
        // 成功从替换器获取牺牲帧 frame_id_to_use
        Page* victim_page_ptr = &pages_[frame_id_to_use];
        // LOG(INFO) << "BufferPoolManager::FetchPage: No free frames. Evicting content of frame " 
        //           << frame_id_to_use << " (which held page " << victim_page_ptr->GetPageId() 
        //           << ") to make space for page " << page_id << ".";

        //  2. 如果 R (牺牲页) 是脏的，写回磁盘
        if (victim_page_ptr->IsDirty()) {
            // LOG(INFO) << "BufferPoolManager::FetchPage: Victim page " << victim_page_ptr->GetPageId() 
            //           << " in frame " << frame_id_to_use << " is dirty. Flushing to disk.";
            // 直接调用 disk_manager_ 写盘，而不是 this->FlushPage()，以避免潜在的锁问题
            disk_manager_->WritePage(victim_page_ptr->GetPageId(), victim_page_ptr->GetData());
            victim_page_ptr->is_dirty_ = false; // 写回后标记为不脏
        }

        // 3. 从页面表中删除 R (牺牲页的映射)
        page_table_.erase(victim_page_ptr->GetPageId()); 
        // LOG(INFO) << "BufferPoolManager::FetchPage: Removed mapping for old page " 
        //           << victim_page_ptr->GetPageId() << " from frame " << frame_id_to_use << ".";
    }

    // 到这里，frame_id_to_use 是一个可用的帧 (来自free_list_或通过Victim得到)

    //  4. 更新 P 的元数据，从磁盘读取页面内容，然后返回指向 P 的指针
    Page* new_page_in_frame = &pages_[frame_id_to_use];

    // 设置新页的元数据
    new_page_in_frame->page_id_ = page_id;        
    new_page_in_frame->pin_count_ = 1;             //  新获取的页，pin count为1
    new_page_in_frame->is_dirty_ = false;          // 刚从磁盘加载，不是脏页
    new_page_in_frame->ResetMemory();              // 清理帧的旧内容
    
    // 在页表中为新的 page_id 和选定的 frame_id_to_use 建立映射
    // (page_table.erase 在上面，page_table[page_id] = frame_id 在这里)
    page_table_[page_id] = frame_id_to_use;
    // LOG(INFO) << "BufferPoolManager::FetchPage: Added mapping for new page " << page_id 
    //           << " to frame " << frame_id_to_use << ".";

    // 从磁盘读取请求的页内容到该帧的数据区
    // LOG(INFO) << "BufferPoolManager::FetchPage: Reading page " << page_id 
    //           << " from disk into frame " << frame_id_to_use << ".";
    disk_manager_->ReadPage(page_id, new_page_in_frame->GetData()); 
    
    // 通知替换器该帧已被固定
    replacer_->Pin(frame_id_to_use);      

    // LOG(INFO) << "BufferPoolManager::FetchPage: Page " << page_id << " successfully fetched and loaded into frame " 
    //           << frame_id_to_use << ". Pin count is 1.";
    return new_page_in_frame; 

}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
      std::lock_guard<std::recursive_mutex> guard(latch_);

    //LOG(INFO) << "BufferPoolManager::NewPage (Optimized): Attempting to create a new page.";

    frame_id_t frame_id_to_use;

    // 1. 首先尝试从缓冲池获取一个可用帧 
    //   "如果缓冲池中的所有页面都被固定，返回 null"
    if (free_list_.empty() && replacer_->Size() == 0) {
        //LOG(WARNING) << "BufferPoolManager::NewPage (Optimized): No available frames in buffer pool (all pinned and no free list).";
        page_id = INVALID_PAGE_ID;
        return nullptr; // 对应图片第7行
    }

    //  "从空闲列表或替换器中选择一个受害者页面 P，优先从空闲列表中选择"
    if (!free_list_.empty()) {
        frame_id_to_use = free_list_.front(); 
        free_list_.pop_front();             
        //LOG(INFO) << "BufferPoolManager::NewPage (Optimized): Using free frame " << frame_id_to_use << " from free_list.";
    } else { 
     
        if (!replacer_->Victim(&frame_id_to_use)) {
            //LOG(WARNING) << "BufferPoolManager::NewPage (Optimized): No victim available from replacer.";
            page_id = INVALID_PAGE_ID;
            return nullptr; 
        }
        
        Page* victim_page_ptr = &pages_[frame_id_to_use];
        // LOG(INFO) << "BufferPoolManager::NewPage (Optimized): Evicting content of frame " 
        //           << frame_id_to_use << " (which held page " << victim_page_ptr->GetPageId() << ").";

        // 处理牺牲的脏页 
        if (victim_page_ptr->IsDirty()) {
            // LOG(INFO) << "BufferPoolManager::NewPage (Optimized): Victim page " << victim_page_ptr->GetPageId() 
            //           << " in frame " << frame_id_to_use << " is dirty. Flushing to disk.";
            disk_manager_->WritePage(victim_page_ptr->GetPageId(), victim_page_ptr->GetData());
            victim_page_ptr->is_dirty_ = false;
        }
        
        // 从页表中移除旧页的映射
        page_table_.erase(victim_page_ptr->GetPageId());
        // LOG(INFO) << "BufferPoolManager::NewPage (Optimized): Removed mapping for old page " 
        //           << victim_page_ptr->GetPageId() << " from frame " << frame_id_to_use << ".";
    }

    // 2. 成功获取缓冲池帧后，再从磁盘分配新页 
    page_id_t new_on_disk_page_id = disk_manager_->AllocatePage();
    if (new_on_disk_page_id == INVALID_PAGE_ID) {
        LOG(ERROR) << "BufferPoolManager::NewPage (Optimized): DiskManager failed to allocate a new page on disk.";
        // 归还之前获取的帧
        pages_[frame_id_to_use].page_id_ = INVALID_PAGE_ID; 
        pages_[frame_id_to_use].pin_count_ = 0;
        pages_[frame_id_to_use].is_dirty_ = false;
        // 注意：如果 frame_id_to_use 来自 replacer_->Victim()，它已经被从 replacer 中移除了。
        // 将其放回 free_list_ 是一个安全的回退操作。
        free_list_.push_front(frame_id_to_use);

        page_id = INVALID_PAGE_ID;
        return nullptr;
    }
    // LOG(INFO) << "BufferPoolManager::NewPage (Optimized): DiskManager allocated new page_id " 
    //           << new_on_disk_page_id << " on disk.";

    // 3. 更新帧的元数据，并将其添加到页表 
    Page* new_page_in_frame = &pages_[frame_id_to_use];
    
    // (隐式将 new_on_disk_page_id 赋值给帧的 page_id)
    //  (pages[frame_id].page_id = page_id)
    new_page_in_frame->page_id_ = new_on_disk_page_id; 
    
    new_page_in_frame->pin_count_ = 1;
 
    new_page_in_frame->is_dirty_ = false; // 新创建的空页通常不是脏的
                                          // 如果创建后立即被写入，调用者负责通过 UnpinPage 标记为脏
 
    new_page_in_frame->ResetMemory(); // 清空帧数据

    //  (page_table[page_id] = frame_id) -> page_id 是 new_on_disk_page_id
    page_table_[new_on_disk_page_id] = frame_id_to_use;
    LOG(INFO) << "BufferPoolManager::NewPage (Optimized): Added mapping for new page " << new_on_disk_page_id 
              << " to frame " << frame_id_to_use << ".";
    
 
    replacer_->Pin(frame_id_to_use); // 固定新页，不让替换器立即换出

    //  (设置输出参数)
    page_id = new_on_disk_page_id;

    // LOG(INFO) << "BufferPoolManager::NewPage (Optimized): Successfully created and buffered new page " << page_id 
    //           << " in frame " << frame_id_to_use << ". Pin count is 1.";
  
    return new_page_in_frame;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::lock_guard<std::recursive_mutex> guard(latch_);

    //LOG(INFO) << "BufferPoolManager::DeletePage (Optimized): Attempting to delete page_id " << page_id;

    auto page_table_iter = page_table_.find(page_id);

    if (page_table_iter == page_table_.end()) {
        // 页不在缓冲池中，但仍需通知DiskManager释放（如果它之前存在于磁盘）
        LOG(INFO) << "BufferPoolManager::DeletePage (Optimized): Page " << page_id 
                  << " not found in buffer pool. Calling DiskManager::DeAllocatePage.";
        disk_manager_->DeAllocatePage(page_id);
        return true; // 操作视为完成，因为缓冲池无事可做
    }

    // 页在缓冲池中
    frame_id_t frame_id_of_page = page_table_iter->second;
    Page* page_to_delete_ptr = &pages_[frame_id_of_page];

    if (page_to_delete_ptr->GetPinCount() != 0) {
        LOG(WARNING) << "BufferPoolManager::DeletePage (Optimized): Page " << page_id << " in frame " << frame_id_of_page
                     << " is pinned (pin_count=" << page_to_delete_ptr->GetPinCount() 
                     << "). Cannot delete from buffer pool.";
        return false; 
    }

    // 页在缓冲池中且未被固定，可以安全地从缓冲池移除
    // LOG(INFO) << "BufferPoolManager::DeletePage (Optimized): Page " << page_id << " (frame " << frame_id_of_page
    //           << ") is unpinned. Removing from buffer pool and deallocating from disk.";

    // 1. 从页表中移除
    page_table_.erase(page_table_iter);

    // 2. 重置帧的元数据
    page_to_delete_ptr->ResetMemory();
    page_to_delete_ptr->page_id_ = INVALID_PAGE_ID;
    page_to_delete_ptr->pin_count_ = 0;
    page_to_delete_ptr->is_dirty_ = false; // 脏数据不写回

    // 3. 将帧加入空闲列表
    free_list_.push_back(frame_id_of_page);

    // 4. 通知替换器该帧不再可替换 (因为它现在是空闲的)
    replacer_->Pin(frame_id_of_page);

    
    disk_manager_->DeAllocatePage(page_id);

    // LOG(INFO) << "BufferPoolManager::DeletePage (Optimized): Page " << page_id 
    //           << " successfully deleted from buffer and disk. Frame " << frame_id_of_page << " added to free list.";
    return true;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {

    std::lock_guard<std::recursive_mutex> guard(latch_); // 保护共享数据结构

    // LOG(INFO) << "BufferPoolManager::UnpinPage: Attempting to unpin page_id " << page_id 
    //           << " with is_dirty = " << (is_dirty ? "true" : "false");


    auto page_table_iter = page_table_.find(page_id);
    if (page_table_iter == page_table_.end()) {
        LOG(WARNING) << "BufferPoolManager::UnpinPage: Page " << page_id 
                     << " not found in buffer pool. Cannot unpin.";
        return false;
    }


    frame_id_t frame_id_to_unpin = page_table_iter->second;
    Page* page_to_unpin_ptr = &pages_[frame_id_to_unpin];


    // 首先检查 pin_count 是否已经是0，不应该unpin一个已经是0的页
    if (page_to_unpin_ptr->GetPinCount() <= 0) {
        LOG(WARNING) << "BufferPoolManager::UnpinPage: Page " << page_id << " (frame " << frame_id_to_unpin
                     << ") has non-positive pin_count (" << page_to_unpin_ptr->GetPinCount() 
                     << ") before unpin. This is unusual.";
        // 根据策略，这里可以直接返回 false，或者允许操作但记录更严重的错误。
        // 如果严格按图片 "if pin_count > 0"，那么 pin_count <= 0 时不执行减操作。
        // 但函数仍然会继续更新脏位并可能错误地调用 replacer_->Unpin()。
        // 更安全的做法是如果 pin_count <= 0，则认为 unpin 操作无效或失败。
        return false; // 增加此返回，使逻辑更健壮
    }

    // pin_count > 0，可以安全地减1
    page_to_unpin_ptr->pin_count_--; // 对应图片第9行
    // LOG(INFO) << "BufferPoolManager::UnpinPage: Page " << page_id << " (frame " << frame_id_to_unpin
    //           << ") pin_count decremented to " << page_to_unpin_ptr->GetPinCount();

    // 更新页面的脏状态
    if (is_dirty) {
        page_to_unpin_ptr->is_dirty_ = true;
        // LOG(INFO) << "BufferPoolManager::UnpinPage: Page " << page_id << " (frame " << frame_id_to_unpin
        //           << ") marked as dirty due to UnpinPage call.";
    }


    //如果 pin_count 变为0，通知替换器**
    if (page_to_unpin_ptr->GetPinCount() == 0) {
        replacer_->Unpin(frame_id_to_unpin);
        LOG(INFO) << "BufferPoolManager::UnpinPage: Page " << page_id << " (frame " << frame_id_to_unpin
                  << ") pin_count is now 0. Frame unpinned in replacer.";
    }

    return true;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) {
  std::lock_guard<std::recursive_mutex> guard(latch_);

    //LOG(INFO) << "BufferPoolManager::FlushPage (Optimized): Attempting to flush page_id " << page_id;

    auto page_table_iter = page_table_.find(page_id);
    if (page_table_iter == page_table_.end()) {
        LOG(WARNING) << "BufferPoolManager::FlushPage (Optimized): Page " << page_id 
                     << " not found in buffer pool. Cannot flush.";
        return false;
    }

    frame_id_t frame_id_to_flush = page_table_iter->second;
    Page* page_to_flush_ptr = &pages_[frame_id_to_flush];

    // 优化：仅当页面是脏的时候才执行写磁盘操作
    if (page_to_flush_ptr->IsDirty()) {
        // LOG(INFO) << "BufferPoolManager::FlushPage (Optimized): Page " << page_id 
        //           << " (frame " << frame_id_to_flush << ") is dirty. Writing to disk.";
        disk_manager_->WritePage(page_id, page_to_flush_ptr->GetData());
        // 假设 WritePage 成功
        page_to_flush_ptr->is_dirty_ = false;
        // LOG(INFO) << "BufferPoolManager::FlushPage (Optimized): Page " << page_id 
        //           << " successfully flushed and marked as not dirty.";
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
#include "storage/disk_manager.h"

#include <sys/stat.h>

#include <filesystem>
#include <stdexcept>

#include "glog/logging.h"
#include "page/bitmap_page.h"

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    std::filesystem::path p = db_file;
    if (p.has_parent_path()) std::filesystem::create_directories(p.parent_path());
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  WritePhysicalPage(META_PAGE_ID, meta_data_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
} 

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::AllocatePage() {
  std::lock_guard<std::recursive_mutex> guard(db_io_latch_); // 保护对文件和元数据的访问

    DiskFileMetaPage* meta_page_ = reinterpret_cast<DiskFileMetaPage*>(meta_data_); 

    //  检查是否已分配所有可能的页面
    if (meta_page_->num_allocated_pages_ >= MAX_VALID_PAGE_ID) {
        LOG(WARNING) << "DiskManager::AllocatePage: All possible pages (" << MAX_VALID_PAGE_ID
                     << ") have been allocated. Cannot allocate more.";
        return INVALID_PAGE_ID; 
    }

    // 在现有的扩展中找到第一个空闲页面
    for (uint32_t i = 0; i < meta_page_->num_extents_; ++i) {
        // GetExtentUsedPage(i) 是指该extent内的bitmap管理的page有多少被用了
        // BITMAP_SIZE 是一个bitmap能管理的总数据页数
        if (meta_page_->extent_used_page_[i] < DiskManager::BITMAP_SIZE) { 
            LOG(INFO) << "DiskManager::AllocatePage: Found space in existing extent " << i
                      << " (used: " << meta_page_->extent_used_page_[i] << "/" << DiskManager::BITMAP_SIZE << ").";

            //  创建一个新的 BitmapPage 对象 (在栈上创建以避免 new/delete)
            BitmapPage<PAGE_SIZE> bitmap_page_obj; // 对象在栈上
            char* bitmap_buffer = reinterpret_cast<char*>(&bitmap_page_obj);

            // 计算该 extent 对应的位图页的物理页号
            // 物理页0是 meta_page, 第一个 extent (i=0) 的 bitmap 在物理页1
            // 第 i 个 extent 的 bitmap 在物理页 1 + i * (1个bitmap页 + BITMAP_SIZE个数据页)
            // 这里 BITMAP_SIZE 来自 DiskManager 的静态成员，是数据页的数量。
            // extent 大小是 1 (bitmap) + BITMAP_SIZE (data pages)
            page_id_t physical_bitmap_page_id = 1 + i * (1 + DiskManager::BITMAP_SIZE); 

            // 读取物理页面到 bitmap_page_obj
            ReadPhysicalPage(physical_bitmap_page_id, bitmap_buffer);

            uint32_t page_offset_in_bitmap = 0; 
            // 在 bitmap_page 中分配一个页面
            bool res = bitmap_page_obj.AllocatePage(page_offset_in_bitmap);
            
            // 图片第20行: assert(res, "Allocate page failed")
            if (!res) {
                 LOG(ERROR) << "DiskManager::AllocatePage: BitmapPage::AllocatePage failed for extent " << i
                           << " (physical_bitmap_page_id: " << physical_bitmap_page_id 
                           << ") even though extent_used_page suggested space. This indicates inconsistency.";
                 // 理论上不应发生，如果发生了，可能需要更复杂的恢复或错误处理
                 continue; // 尝试下一个 extent 
            }
            LOG(INFO) << "DiskManager::AllocatePage: Allocated offset " << page_offset_in_bitmap 
                      << " within bitmap of extent " << i;

            // 更新元数据
            meta_page_->num_allocated_pages_++;
            meta_page_->extent_used_page_[i]++;
            
            // 将更新后的 bitmap_page 写回磁盘
            WritePhysicalPage(physical_bitmap_page_id, reinterpret_cast<const char*>(bitmap_buffer));
            
            // 更新并写回 DiskFileMetaPage (meta_data_)
            WritePhysicalPage(0, meta_data_); // 物理页0是元数据页
            LOG(INFO) << "DiskManager::AllocatePage: Updated meta_page on disk. num_allocated_pages: " 
                      << meta_page_->num_allocated_pages_ << ", extent " << i << " used: " << meta_page_->extent_used_page_[i];


            // 返回分配的页面 ID (逻辑页号)
            page_id_t logical_page_id = i * DiskManager::BITMAP_SIZE + page_offset_in_bitmap;
            LOG(INFO) << "DiskManager::AllocatePage: Successfully allocated logical page_id: " << logical_page_id;
            return logical_page_id;
        }
    }

    LOG(INFO) << "DiskManager::AllocatePage: No space in existing extents. Attempting to create a new extent. "
              << "Current num_extents: " << meta_page_->num_extents_;

    //  如果没有找到空闲页面，创建新的扩展

    BitmapPage<PAGE_SIZE> new_bitmap_obj; // 在栈上创建
    char* new_bitmap_buffer = reinterpret_cast<char*>(&new_bitmap_obj);
    // 对于全新的 BitmapPage，page_allocated_ 和 next_free_page_ 应为0 (由构造函数保证)

    uint32_t new_page_offset_in_bitmap = 0; 
    // 在新的 bitmap_page 中分配一个页面
    bool alloc_res = new_bitmap_obj.AllocatePage(new_page_offset_in_bitmap); 
    
    if (!alloc_res || new_page_offset_in_bitmap != 0) {
        LOG(FATAL) << "DiskManager::AllocatePage: Failed to allocate the first page (offset 0) in a brand new bitmap. "
                   << "alloc_res: " << alloc_res << ", new_page_offset_in_bitmap: " << new_page_offset_in_bitmap;
    
        return INVALID_PAGE_ID;
    }
    LOG(INFO) << "DiskManager::AllocatePage: Allocated offset " << new_page_offset_in_bitmap 
              << " in new bitmap for new extent.";

    uint32_t new_extent_index = meta_page_->num_extents_; // 新的extent的索引
    page_id_t new_physical_bitmap_page_id = 1 + new_extent_index * (1 + DiskManager::BITMAP_SIZE); 
    // 将新的 bitmap_page 写回磁盘
    WritePhysicalPage(new_physical_bitmap_page_id, reinterpret_cast<const char*>(new_bitmap_buffer));
    LOG(INFO) << "DiskManager::AllocatePage: Wrote new bitmap for extent " << new_extent_index 
              << " to physical page " << new_physical_bitmap_page_id;

    // 更新元数据
    meta_page_->num_allocated_pages_++;
    meta_page_->extent_used_page_[new_extent_index] = 1; // 新 extent 中已用1页
    meta_page_->num_extents_++;
    
    // 更新并写回 DiskFileMetaPage (meta_data_)
    WritePhysicalPage(0, meta_data_);
    LOG(INFO) << "DiskManager::AllocatePage: Updated meta_page on disk for new extent. "
              << "num_allocated_pages: " << meta_page_->num_allocated_pages_
              << ", num_extents: " << meta_page_->num_extents_
              << ", extent " << new_extent_index << " used: " << meta_page_->extent_used_page_[new_extent_index];

    //  返回新分配的页面 ID (逻辑页号)
    page_id_t logical_page_id = new_extent_index * DiskManager::BITMAP_SIZE + new_page_offset_in_bitmap;
    LOG(INFO) << "DiskManager::AllocatePage: Successfully allocated logical page_id from new extent: " << logical_page_id;
    ASSERT(false, "Not implemented yet.");
    return logical_page_id;
  
}

/**
 * TODO: Student Implement
 */
void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  std::lock_guard<std::recursive_mutex> guard(db_io_latch_); // 保护对文件和元数据的访问

    // 0. 增加对 logical_page_id 的基本有效性检查
    if (logical_page_id < 0) { // 逻辑页号通常是非负的
        LOG(ERROR) << "DiskManager::DeAllocatePage: Attempted to deallocate an invalid logical_page_id: "
                   << logical_page_id;
        return;
    }

    // meta_page_ 已经在构造函数中通过 reinterpret_cast 初始化指向 meta_data_
    DiskFileMetaPage* meta_page_ = reinterpret_cast<DiskFileMetaPage*>(meta_data_); 


    uint32_t extent_id = static_cast<uint32_t>(logical_page_id) / DiskManager::BITMAP_SIZE;

    // 检查 extent_id 是否有效 (是否在当前已创建的 extents 范围内)
    if (extent_id >= meta_page_->num_extents_) {
        LOG(ERROR) << "DiskManager::DeAllocatePage: Extent ID " << extent_id 
                   << " calculated for logical_page_id " << logical_page_id
                   << " is out of bounds. Current num_extents: " << meta_page_->num_extents_
                   << ". Page likely not allocated or invalid ID.";
        return;
    }


    // 物理页0是 meta_page, 第一个 extent (i=0) 的 bitmap 在物理页1
    // 第 extent_id 个 extent 的 bitmap 在物理页 1 + extent_id * (1个bitmap页 + BITMAP_SIZE个数据页)
    page_id_t physical_bitmap_page_id = 1 + extent_id * (1 + DiskManager::BITMAP_SIZE);

    // 创建 BitmapPage 对象 
    BitmapPage<PAGE_SIZE> bitmap_page_obj;
    char* bitmap_buffer = reinterpret_cast<char*>(&bitmap_page_obj);

    //读取 bitmap_page
    ReadPhysicalPage(physical_bitmap_page_id, bitmap_buffer);

    // 计算页面在 bitmap_page 中的偏移量
    uint32_t page_offset_in_bitmap = static_cast<uint32_t>(logical_page_id) % DiskManager::BITMAP_SIZE;

    // 在 bitmap_page 中释放页面
    bool res = bitmap_page_obj.DeAllocatePage(page_offset_in_bitmap);

    // if res:
    if (res) {
        LOG(INFO) << "DiskManager::DeAllocatePage: Successfully deallocated page " << page_offset_in_bitmap
                  << " within bitmap of extent " << extent_id 
                  << " (logical_page_id: " << logical_page_id << ").";
        //  更新元数据
        if (meta_page_->num_allocated_pages_ > 0) { // 防止下溢
            meta_page_->num_allocated_pages_--;
        } else {
            LOG(WARNING) << "DiskManager::DeAllocatePage: num_allocated_pages_ was already 0 before decrementing for logical_page_id " << logical_page_id;
        }
        if (meta_page_->extent_used_page_[extent_id] > 0) { // 防止下溢
             meta_page_->extent_used_page_[extent_id]--;
        } else {
            LOG(WARNING) << "DiskManager::DeAllocatePage: extent_used_page_[" << extent_id << "] was already 0 before decrementing for logical_page_id " << logical_page_id;
        }
        
        //  将更新后的 bitmap_page 写回磁盘
        WritePhysicalPage(physical_bitmap_page_id, reinterpret_cast<const char*>(bitmap_buffer));
        
        // 更新并写回 DiskFileMetaPage (meta_data_) 到物理页0
        WritePhysicalPage(0, meta_data_); 
        LOG(INFO) << "DiskManager::DeAllocatePage: Updated meta_page on disk. num_allocated_pages: " 
                  << meta_page_->num_allocated_pages_ << ", extent " << extent_id << " used: " << meta_page_->extent_used_page_[extent_id];

    } else {
        // 如果释放失败，记录错误日志

        LOG(WARNING) << "DiskManager::DeAllocatePage: BitmapPage::DeAllocatePage failed for logical_page_id "
                     << logical_page_id << " (offset " << page_offset_in_bitmap << " in extent " << extent_id 
                     << "). The page might have already been free.";
    }
}

/**
 * TODO: Student Implement
 */
bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  // 加锁以保护对 meta_data_ (如果在此处访问) 和文件IO的访问
    
    std::lock_guard<std::recursive_mutex> guard(db_io_latch_);

    // 增加对 logical_page_id 的基本有效性检查 (例如，非负)
    if (logical_page_id < 0) {
        LOG(WARNING) << "DiskManager::IsPageFree: Called with invalid (negative) logical_page_id: " 
                     << logical_page_id;
        return false; // 或 true，取决于如何定义无效ID的“空闲”状态，但false更安全
    }

    // 创建一个新的 BitmapPage 对象

    BitmapPage<PAGE_SIZE>* bitmap_page_ptr = nullptr;
    try {
        bitmap_page_ptr = new BitmapPage<PAGE_SIZE>();
    } catch (const std::bad_alloc& e) {
        LOG(FATAL) << "DiskManager::IsPageFree: Failed to allocate memory for BitmapPage: " << e.what();
        return false; // 无法继续
    }
    char* bitmap_buffer = reinterpret_cast<char*>(bitmap_page_ptr);

    // 计算页面所在的扩展 ID
    uint32_t extent_id = static_cast<uint32_t>(logical_page_id) / DiskManager::BITMAP_SIZE;
    //计算页面在 bitmap_page 中的偏移量
    uint32_t page_offset_in_bitmap = static_cast<uint32_t>(logical_page_id) % DiskManager::BITMAP_SIZE;
    //计算 bitmap_page 的物理页面 ID
    page_id_t physical_bitmap_page_id = 1 + extent_id * (1 + DiskManager::BITMAP_SIZE);

    // 获取元数据指针 (每次需要时进行转换，因为头文件中没有 meta_page_ 成员)
    DiskFileMetaPage* meta_page_ptr = reinterpret_cast<DiskFileMetaPage*>(meta_data_);

    // 检查扩展 ID 是否超出范围
    if (extent_id >= meta_page_ptr->num_extents_) {
        LOG(INFO) << "DiskManager::IsPageFree: logical_page_id " << logical_page_id 
                  << " belongs to extent " << extent_id << " which does not exist (num_extents: " 
                  << meta_page_ptr->num_extents_ << "). Considered free.";
        delete bitmap_page_ptr; // 清理已分配的内存
        return true; // 图片第13行
    }

    //读取 bitmap_page
  
    ReadPhysicalPage(physical_bitmap_page_id, bitmap_buffer);
    LOG(INFO) << "DiskManager::IsPageFree: Read bitmap for extent " << extent_id 
              << " from physical page " << physical_bitmap_page_id;

    //检查页面是否空闲
    bool is_free = bitmap_page_ptr->IsPageFree(page_offset_in_bitmap);
    LOG(INFO) << "DiskManager::IsPageFree: logical_page_id " << logical_page_id 
              << " (offset " << page_offset_in_bitmap << " in bitmap for extent " << extent_id 
              << ") is_free result: " << is_free;

    //  释放 bitmap_page 对象
    delete bitmap_page_ptr;

    //返回页面是否空闲
    return is_free;

}

/**如
 * TODO: Student Implement
 */
page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {

    if (logical_page_id < 0) {
      LOG(WARNING) << "MapPageId_V1: Received invalid (negative) logical_page_id: " << logical_page_id;
      return INVALID_PAGE_ID;
  }

  //  计算页面所在的扩展 ID
  uint32_t extent_id = static_cast<uint32_t>(logical_page_id) / DiskManager::BITMAP_SIZE;
  //  计算页面在 bitmap_page 中的偏移量
  uint32_t page_offset_in_extent_data_area = static_cast<uint32_t>(logical_page_id) % DiskManager::BITMAP_SIZE;

  //  计算 bitmap_page 的物理页面 ID
  // 物理页0是 MetaPage。每个 extent 包含 1 个 BitmapPage 和 BITMAP_SIZE 个数据页。
  page_id_t physical_bitmap_page_id = 1 + extent_id * (1 + DiskManager::BITMAP_SIZE);

  //  返回物理页面 ID
  page_id_t physical_page_id_as_per_image = physical_bitmap_page_id + page_offset_in_extent_data_area + 1;

  // LOG(INFO) << "MapPageId_V1 (Image Logic): logical_id=" << logical_page_id
  //           << " -> extent_id=" << extent_id 
  //           << ", page_offset_in_extent=" << page_offset_in_extent_data_area
  //           << ", physical_bitmap_page_id=" << physical_bitmap_page_id
  //           << " -> mapped_physical_id=" << physical_page_id_as_per_image;
            
  return physical_page_id_as_per_image;

}

int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}
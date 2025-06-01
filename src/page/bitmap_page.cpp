#include "page/bitmap_page.h"

#include "glog/logging.h"

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
   if (page_allocated_ >= GetMaxSupportedSize() && GetMaxSupportedSize() > 0) { // 只有在有可管理页时才判断是否已满
       
        return false;
    }
    if (GetMaxSupportedSize() == 0) { // 如果根本没有可管理的页
        LOG(WARNING) << "BitmapPage::AllocatePage (ImageLogic): GetMaxSupportedSize() is 0. Cannot allocate.";
        return false;
    }

    if (IsPageFree(next_free_page_)) {

        uint32_t byte_to_modify = next_free_page_ / 8;
        uint8_t bit_to_modify = next_free_page_ % 8;

        if (byte_to_modify >= MAX_CHARS) { // 防御性检查
             LOG(ERROR) << "BitmapPage::AllocatePage (ImageLogic): Calculated byte_index " << byte_to_modify
                       << " is out of bounds (MAX_CHARS: " << MAX_CHARS
                       << ") for next_free_page_ " << next_free_page_ << ". Inconsistency detected.";
            return false;
        }
        bytes[byte_to_modify] |= (1U << bit_to_modify);

        page_offset = next_free_page_;

        page_allocated_++;

        uint32_t scanned_free_index = 0; 
        
        if (GetMaxSupportedSize() > 0) { // 仅当有可管理的页时才扫描
            while (!IsPageFree(scanned_free_index) && (scanned_free_index < GetMaxSupportedSize() - 1) ) {
                scanned_free_index++; 
            }
 
        } else {

        }

        next_free_page_ = scanned_free_index; 
       // LOG(INFO) << "BitmapPage::AllocatePage (ImageLogic): Updated next_free_page_ hint to " << next_free_page_;

        return true;
    } else {
        LOG(WARNING) << "BitmapPage::AllocatePage (ImageLogic): Initial hint next_free_page_ (" << next_free_page_
                     << ") is not free or invalid. Allocation failed based on this hint.";
        return false;
    }
}



/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  // 1. 边界检查
  if (page_offset >= GetMaxSupportedSize()) {
    // LOG(WARNING) << "DeAllocatePage_Optimized: page_offset " << page_offset 
    //              << " is out of bounds (GetMaxSupportedSize() = " << GetMaxSupportedSize() << ").";
    return false;
}

// 2. 检查页面是否已被分配 (如果已经是空闲的，则无法“再”释放)
uint32_t byte_to_modify = page_offset / 8;
uint8_t bit_to_modify = page_offset % 8;

if (byte_to_modify >= MAX_CHARS) { // 安全检查
    LOG(ERROR) << "DeAllocatePage_Optimized: Calculated byte_index " << byte_to_modify
               << " is out of bounds (MAX_CHARS: " << MAX_CHARS
               << ") for page_offset " << page_offset << ".";
    return false;
}

if ((bytes[byte_to_modify] & (1U << bit_to_modify)) == 0) { // 即 IsPageFree(page_offset)
    //LOG(WARNING) << "DeAllocatePage_Optimized: Page " << page_offset << " is already free. Cannot deallocate.";
    return false;
}

// 3. 执行释放操作：位图对应位清零
bytes[byte_to_modify] &= ~(1U << bit_to_modify);

// 4. 更新元数据：已分配页数减少
if (page_allocated_ > 0) {
    page_allocated_--;
} else {
    // 这通常表示元数据不一致
    LOG(ERROR) << "DeAllocatePage_Optimized: page_allocated_ was 0, but page " << page_offset 
               << " was marked as allocated in bitmap and is now deallocated. Metadata inconsistency.";
}

if (page_offset < next_free_page_ || next_free_page_ == GetMaxSupportedSize()) {
    next_free_page_ = page_offset;
    // LOG(INFO) << "DeAllocatePage_Optimized: Updated next_free_page_ hint to " << next_free_page_
    //           << " due to deallocation of page " << page_offset << ".";
}

return true;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
    // GetMaxSupportedSize() 是 BitmapPage 头文件中定义的，表示此位图能管理的最大页数。
    if (page_offset >= GetMaxSupportedSize()) {
     
      LOG(WARNING) << "IsPageFree: page_offset " << page_offset 
                   << " is out of bounds for GetMaxSupportedSize() " << GetMaxSupportedSize() << ".";
     
      return false;
  }

  // 计算对应的字节索引和位索引。
  uint32_t byte_index = page_offset / 8;
  uint8_t bit_index = page_offset % 8;

  return IsPageFreeLow(byte_index, bit_index);

}
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
   // 增加对 byte_index 和 bit_index 的严格检查，尽管通常由 IsPageFree 保证其有效性。
    // MAX_CHARS 是 bytes 数组的大小，在 BitmapPage 头文件中定义。
    if (byte_index >= MAX_CHARS) {
      LOG(ERROR) << "IsPageFreeLow: byte_index " << byte_index 
                 << " is out of bounds (MAX_CHARS: " << MAX_CHARS << ").";
      return false; // 字节索引超出范围，视为不可用/不空闲。
  }
  if (bit_index >= 8) {
      LOG(ERROR) << "IsPageFreeLow: bit_index " << bit_index 
                 << " is out of bounds (must be 0-7).";
      return false; // 位索引必须是0到7。
  }

  return (bytes[byte_index] & (1U << bit_index)) == 0;
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;
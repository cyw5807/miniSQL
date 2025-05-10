#include "page/bitmap_page.h"

#include "glog/logging.h"

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
   // 1. 容量检查: 是否所有页都已分配完毕

// 2. 查找空闲页: 从 next_free_page_ 开始进行环形搜索
//    确保起始搜索点有效
uint32_t current_scan_start = next_free_page_;
if (current_scan_start >= GetMaxSupportedSize()) {
    // LOG(INFO) << "AllocatePage_Optimized: next_free_page_ (" << next_free_page_ 
    //           << ") was out of bounds. Resetting scan start to 0.";
    current_scan_start = 0;
}

for (uint32_t i = 0; i < GetMaxSupportedSize(); ++i) {
    uint32_t candidate_offset = (current_scan_start + i) % GetMaxSupportedSize();

    if (IsPageFree(candidate_offset)) {
        // 找到了空闲页
        uint32_t byte_to_modify = candidate_offset / 8;
        uint8_t bit_to_modify = candidate_offset % 8;

        if (byte_to_modify >= MAX_CHARS) { // 额外的安全检查
             LOG(ERROR) << "AllocatePage_Optimized: Calculated byte_index " << byte_to_modify
                   << " is out of bounds (MAX_CHARS: " << MAX_CHARS
                   << ") for candidate_offset " << candidate_offset << ". This indicates an issue with GetMaxSupportedSize or IsPageFree.";
            return false; // 或者尝试下一个，但这通常表明逻辑错误
        }

        bytes[byte_to_modify] |= (1U << bit_to_modify); // 标记为已分配

        page_allocated_++;
        page_offset = candidate_offset; // 设置输出参数

        // LOG(INFO) << "AllocatePage_Optimized: Successfully allocated page " << page_offset
        //           << ". Total allocated: " << page_allocated_;

        // 3. 更新 next_free_page_ 指向下一个 实际 的空闲页
        //    从刚分配的页的下一个位置开始搜索
        bool found_next_hint = false;
        if (page_allocated_ < GetMaxSupportedSize()) { // 只有在还有空闲页时才搜索
            for (uint32_t j = 0; j < GetMaxSupportedSize(); ++j) {
                uint32_t next_hint_candidate = (page_offset + 1 + j) % GetMaxSupportedSize();
                if (IsPageFree(next_hint_candidate)) {
                    next_free_page_ = next_hint_candidate;
                    found_next_hint = true;
                    break;
                }
            }
        }
        if (!found_next_hint) { // 如果没有找到下一个空闲页 (可能所有页都满了)
            next_free_page_ = GetMaxSupportedSize(); // 指向末尾之后，表示没有已知空闲页或全满
        }
        // LOG(INFO) << "AllocatePage_Optimized: Updated next_free_page_ hint to " << next_free_page_;
        
        return true; // 分配成功
    }
}

// 如果循环结束仍未找到空闲页 (理论上，如果 page_allocated_ < GetMaxSupportedSize()，则不应发生)
LOG(ERROR) << "AllocatePage_Optimized: No free page found even though page_allocated_ (" 
           << page_allocated_ << ") < GetMaxSupportedSize() (" << GetMaxSupportedSize() 
           << "). This might indicate a corrupted bitmap or metadata.";
return false;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  // 1. 边界检查
  if (page_offset >= GetMaxSupportedSize()) {
    LOG(WARNING) << "DeAllocatePage_Optimized: page_offset " << page_offset 
                 << " is out of bounds (GetMaxSupportedSize() = " << GetMaxSupportedSize() << ").";
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
    LOG(WARNING) << "DeAllocatePage_Optimized: Page " << page_offset << " is already free. Cannot deallocate.";
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
// LOG(INFO) << "DeAllocatePage_Optimized: Deallocated page " << page_offset
          // << ". Total allocated now: " << page_allocated_;

// 5. 更新 next_free_page_ 提示
//    如果释放的页的偏移量小于当前的 next_free_page_，
//    或者如果 next_free_page_ 原本指向一个无效位置 (例如 GetMaxSupportedSize()，表示之前已满)，
//    那么这个刚释放的页就成了新的（或更优的）next_free_page_ 候选。
if (page_offset < next_free_page_ || next_free_page_ == GetMaxSupportedSize()) {
    next_free_page_ = page_offset;
    LOG(INFO) << "DeAllocatePage_Optimized: Updated next_free_page_ hint to " << next_free_page_
              << " due to deallocation of page " << page_offset << ".";
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

  // (1U << bit_index) 创建一个位掩码，其中只有 bit_index 指定的位是1。
  // 如果 bytes[byte_index] 中对应的位是0，则按位与的结果为0。
  return (bytes[byte_index] & (1U << bit_index)) == 0;


}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;
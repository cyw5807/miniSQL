#include "buffer/lru_replacer.h"
#include "glog/logging.h"

LRUReplacer::LRUReplacer(size_t num_pages):capacity_(num_pages){}

LRUReplacer::~LRUReplacer() = default;

/**
 * TODO: Student Implement
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> guard(latch_);

    if (lru_list_.empty()) {
        
        if (frame_id != nullptr) {
            // *frame_id = INVALID_FRAME_ID; // 可选
        }
        return false;
    }

    // 获取LRU元素 (链表尾部)
    frame_id_t victim_id = lru_list_.back();

    // 从链表中移除
    lru_list_.pop_back(); // O(1)
    
    // 从set中也移除
    size_t erased_from_set = lru_set_tracker_.erase(victim_id); 


    if (frame_id != nullptr) {
        *frame_id = victim_id;
    }

    return true;
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);
    if (lru_set_tracker_.count(frame_id)) { 

        lru_set_tracker_.erase(frame_id);

        lru_list_.remove(frame_id); 

    } else {

    }
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
   std::lock_guard<std::mutex> guard(latch_);

    if (lru_set_tracker_.find(frame_id) == lru_set_tracker_.end()) { // 不在集合中
        
        if (lru_list_.size() >= capacity_) {
            if (capacity_ == 0) {
                 
                return;
            }

        }
        lru_list_.push_front(frame_id); // 加到MRU端 (list头部)
        lru_set_tracker_.insert(frame_id); // 加入set
       
    } else {
        
    }
}

/**
 * TODO: Student Implement
 */
size_t LRUReplacer::Size() {
  std::lock_guard<std::mutex> guard(latch_);
    // 返回当前在可替换列表中的元素数量
    size_t current_size = lru_list_.size();
    // 验证与 set 大小的一致性 (用于调试)
    if (current_size != lru_set_tracker_.size()) {
        LOG(ERROR) << "LRUReplacer::Size: Mismatch between lru_list_ size (" << current_size
                   << ") and lru_set_tracker_ size (" << lru_set_tracker_.size() << "). Data inconsistency!";
    }
    // LOG(INFO) << "LRUReplacer::Size: Current number of evictable frames is " << current_size;
    return current_size;

}
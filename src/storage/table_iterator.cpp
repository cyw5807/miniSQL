#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
TableIterator::TableIterator(TableHeap *table_heap, RowId rid, Txn *txn) 
:table_heap_(table_heap), rid_(rid), txn_(txn){
  if (table_heap_ != nullptr && rid_.GetPageId() != INVALID_PAGE_ID) {
        row_ = Row(rid_); 
        if (!table_heap_->GetTuple(&row_, txn_)) {
            rid_.Set(INVALID_PAGE_ID, 0);
        }
    }
}

TableIterator::TableIterator(const TableIterator &other) {
  table_heap_ = other.table_heap_;
    rid_ = other.rid_;
    txn_ = other.txn_;
    row_ = other.row_;
}

TableIterator::~TableIterator() = default;

bool TableIterator::operator==(const TableIterator &itr) const {
  return table_heap_ == itr.table_heap_ && rid_ == itr.rid_;
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return !(*this == itr);
}

const Row &TableIterator::operator*() {
  ASSERT(rid_.GetPageId() != INVALID_PAGE_ID, "Dereferencing end or invalid iterator.");
    return row_;
}

Row *TableIterator::operator->() {
  ASSERT(rid_.GetPageId() != INVALID_PAGE_ID, "Dereferencing end or invalid iterator.");
    return &row_;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  if (this != &itr) {
        table_heap_ = itr.table_heap_;
        rid_ = itr.rid_;
        txn_ = itr.txn_;
        row_ = itr.row_;
    }
    return *this;
}

// ++iter
TableIterator &TableIterator::operator++() {
  // 如果已经是 End() 迭代器，或 TableHeap 为空，则不再移动
    if (table_heap_ == nullptr || rid_.GetPageId() == INVALID_PAGE_ID) {
        return *this;
    }

    BufferPoolManager *bpm = table_heap_->buffer_pool_manager_;
    page_id_t current_page_id = rid_.GetPageId();
    RowId current_rid = rid_;
    RowId next_rid;

    // 获取当前页面
    auto page = reinterpret_cast<TablePage *>(bpm->FetchPage(current_page_id));
    if (page == nullptr) {
        // 无法获取当前页面，这是一个异常情况，设置为 End()
        rid_.Set(INVALID_PAGE_ID, 0);
        return *this;
    }

    // 1. 尝试在当前页面查找下一个元组
    if (page->GetNextTupleRid(current_rid, &next_rid)) {
        bpm->UnpinPage(current_page_id, false);
        rid_ = next_rid;
        row_ = Row(rid_);
        table_heap_->GetTuple(&row_, txn_);
        return *this;
    }

    // 2. 如果当前页面没有下一个了，则开始查找后续页面
    while (true) {
        page_id_t next_page_id = page->GetNextPageId();
        bpm->UnpinPage(current_page_id, false); // Unpin 当前页

        if (next_page_id == INVALID_PAGE_ID) {
            // 没有下一页了，到达末尾
            rid_.Set(INVALID_PAGE_ID, 0);
            return *this;
        }

        // 移动到下一页
        current_page_id = next_page_id;
        page = reinterpret_cast<TablePage *>(bpm->FetchPage(current_page_id));
        if (page == nullptr) {
            // 无法获取下一页，设为 End()
            rid_.Set(INVALID_PAGE_ID, 0);
            return *this;
        }

        // 尝试从新页面的开头获取第一个元组
        if (page->GetFirstTupleRid(&next_rid)) {
            bpm->UnpinPage(current_page_id, false);
            rid_ = next_rid;
            row_ = Row(rid_);
            table_heap_->GetTuple(&row_, txn_);
            return *this;
        }
        // 如果这个新页面也是空的，循环会继续，获取它的下一页 (在下一次循环开始时 Unpin)
    }
}

// iter++
TableIterator TableIterator::operator++(int) { 
  TableIterator temp = *this; // 创建当前迭代器的副本
    ++(*this);                  // 调用前缀 ++ 来移动当前迭代器
    return temp;                // 返回副本 
    }

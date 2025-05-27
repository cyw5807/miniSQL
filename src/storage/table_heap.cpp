#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
bool TableHeap::InsertTuple(Row &row, Txn *txn) { 
  if (row.GetSerializedSize(schema_) > TablePage::SIZE_MAX_ROW) {
      return false; // Tuple too large even for an empty page
  }

  page_id_t current_page_id = first_page_id_;
  TablePage *table_page = nullptr;

  // 1. Try to find an existing page with enough space.
  while (current_page_id != INVALID_PAGE_ID) {
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(current_page_id));
    if (page == nullptr) { return false; }
    table_page = page;

    table_page->WLatch();
    if (table_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)) {
      table_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(current_page_id, true);
      return true;
    }
    table_page->WUnlatch();

    page_id_t next_page_id = table_page->GetNextPageId();
    buffer_pool_manager_->UnpinPage(current_page_id, false);
    current_page_id = next_page_id;
  }

  // 2. If no existing page works, create a new one.
  page_id_t new_page_id;
  auto new_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(new_page_id));
  if (new_page == nullptr) { return false; }
  table_page = new_page;

  // Find the last page ID to link.
  page_id_t last_page_id = first_page_id_;
  page_id_t prev_page_id_for_new = INVALID_PAGE_ID;

  if (last_page_id == new_page_id) { // This means the heap was empty
      prev_page_id_for_new = INVALID_PAGE_ID;
  } else {
      page_id_t current_iter_id = last_page_id;
      while(true) {
          auto iter_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(current_iter_id));
          if (iter_page == nullptr) { // Error
              buffer_pool_manager_->UnpinPage(new_page_id, false);
              buffer_pool_manager_->DeletePage(new_page_id);
              return false;
          }
          page_id_t next_id = iter_page->GetNextPageId();
          buffer_pool_manager_->UnpinPage(current_iter_id, false);
          if (next_id == INVALID_PAGE_ID) {
              prev_page_id_for_new = current_iter_id; // Found the last one
              break;
          }
          current_iter_id = next_id;
      }
      // Link the last page to the new page.
      auto last_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(prev_page_id_for_new));
      if (last_page == nullptr) { // Error
            buffer_pool_manager_->UnpinPage(new_page_id, false);
            buffer_pool_manager_->DeletePage(new_page_id);
            return false;
      }
      last_page->WLatch();
      last_page->SetNextPageId(new_page_id);
      last_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(prev_page_id_for_new, true);
  }

  // Initialize and insert into the new page.
  table_page->WLatch();
  table_page->Init(new_page_id, prev_page_id_for_new, log_manager_, txn);
  bool success = table_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
  table_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(new_page_id, true);

  return success;

}

bool TableHeap::MarkDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the recovery.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

/**
 * TODO: Student Implement
 */
bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Txn *txn) { 

  if (rid.GetPageId() == INVALID_PAGE_ID) {
    LOG(WARNING) << "UpdateTuple called with invalid RowId.";
    return false;
  }
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr) {
    LOG(WARNING) << "UpdateTuple failed to fetch page " << rid.GetPageId();
    return false;
  }

  // 创建 old_row (如果 TablePage::UpdateTuple 需要)
  Row old_row(rid); 

  //  调用 TablePage::UpdateTuple
  page->WLatch();

  int update_res = page->UpdateTuple(row, &old_row, schema_, txn, lock_manager_, log_manager_);
  page->WUnlatch();

  // 处理返回值
  if (update_res == 0) {
    //  原地更新成功
    row.SetRowId(rid); // 确保 RowId 是旧的 RowId
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true); // 页面变脏
    return true;
  } else if (update_res == 3) {
  
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false); // 原页面未修改，Unpin

    //  尝试插入新元组 (它会修改 row 的 RowId)
    if (InsertTuple(row, txn)) {
        //  插入成功，尝试删除旧元组
        if (MarkDelete(rid, txn)) {
            //  插入和删除都成功
            return true;
        } else {
            LOG(ERROR) << "CRITICAL: InsertTuple succeeded but MarkDelete failed during update. ";
            return false;
        }
    } else {
        // 插入失败
        LOG(WARNING) << "UpdateTuple failed: InsertTuple failed during Insert-then-Delete.";
        return false;
    }
  } else {
    //  其他错误 (update_res = 1 or 2 etc.)
    LOG(WARNING) << "UpdateTuple failed " << " with error code " << update_res;
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false); // 原页面未修改，Unpin
    return false;
  }
}

/**
 * TODO: Student Implement
 */
void TableHeap::ApplyDelete(const RowId &rid, Txn *txn) {
  // Step1: Find the page which contains the tuple.
  if (rid.GetPageId() == INVALID_PAGE_ID) { // 额外检查 rid 有效性
    return;
  }
  auto page_obj = buffer_pool_manager_->FetchPage(rid.GetPageId()); 
  if (page_obj == nullptr) { 
    return;
  }
  auto table_page = reinterpret_cast<TablePage *>(page_obj); 

  // Step2: Delete the tuple from the page.
  table_page->WLatch(); // 增加了并发控制的闩锁
  table_page->ApplyDelete(rid, txn, log_manager_); 
  table_page->WUnlatch(); // 释放闩锁

  buffer_pool_manager_->UnpinPage(table_page->GetTablePageId(), true); //  (true 表示已修改)
}

void TableHeap::RollbackDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback to delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

/**
 * TODO: Student Implement
 */
bool TableHeap::GetTuple(Row *row, Txn *txn) { 
   // 增加了对 row 指针和 RowId 有效性的检查
  if (row == nullptr || row->GetRowId().GetPageId() == INVALID_PAGE_ID) {
    return false;
  }
  RowId rid = row->GetRowId();
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId())); // 对应伪代码步骤 4
  if (page == nullptr) { 
    return false;
  }

  bool success = page->GetTuple(row, schema_, txn, lock_manager_);

  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);

  return success;
}

void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
      DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::Begin(Txn *txn) { 
  page_id_t current_page_id = first_page_id_;
  RowId first_rid;

  // 循环遍历页面，直到找到第一个包含有效元组的页面
  while (current_page_id != INVALID_PAGE_ID) {
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(current_page_id));
    if (page == nullptr) {
      return End(); // 如果无法获取页面，则返回 End 迭代器
    }
    page_id_t next_page_id = page->GetNextPageId(); // 先获取下一页 ID

    if (page->GetFirstTupleRid(&first_rid)) { // 尝试从当前页获取第一个元组的 RID
      buffer_pool_manager_->UnpinPage(current_page_id, false);
      return TableIterator(this, first_rid, txn); // 找到，返回指向该元组的迭代器
    }

    // 当前页面没有有效元组，Unpin 并继续下一页
    buffer_pool_manager_->UnpinPage(current_page_id, false);
    current_page_id = next_page_id;
  }

  return End(); // 遍历完所有页面都没有找到元组 
}
/**
 * TODO: Student Implement
 */
TableIterator TableHeap::End() { return TableIterator(this, RowId(INVALID_PAGE_ID, 0), nullptr); }

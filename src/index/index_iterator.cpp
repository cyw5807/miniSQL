#include "index/index_iterator.h"

#include "index/basic_comparator.h"
#include "index/generic_key.h"

IndexIterator::IndexIterator() = default;

IndexIterator::IndexIterator(page_id_t page_id, BufferPoolManager *bpm, int index)
    : current_page_id(page_id), item_index(index), buffer_pool_manager(bpm) {
  page = reinterpret_cast<LeafPage *>(buffer_pool_manager->FetchPage(current_page_id)->GetData());
}

IndexIterator::~IndexIterator() {
  if (current_page_id != INVALID_PAGE_ID)
    buffer_pool_manager->UnpinPage(current_page_id, false);
}

/**
 * TODO: Student Implement - DONE
 */
std::pair<GenericKey *, RowId> IndexIterator::operator*() {
  if (page == nullptr || item_index < 0 || item_index >= page->GetSize()) {
    throw std::out_of_range("Iterator dereference out of range");
  }
  GenericKey *key = page->KeyAt(item_index);
  RowId value = page->ValueAt(item_index);
  return {key, value};
}

/**
 * TODO: Student Implement - DONE
 */
IndexIterator &IndexIterator::operator++() {
  ++item_index;
  if (item_index >= page->GetSize()) {
    page_id_t next_page_id = page->GetNextPageId();
    buffer_pool_manager->UnpinPage(current_page_id, false);
    current_page_id = next_page_id;
    if (current_page_id != INVALID_PAGE_ID) {
      page = reinterpret_cast<LeafPage *>(buffer_pool_manager->FetchPage(current_page_id)->GetData());
      item_index = 0;
    } else {
      // 到达 end()
      page = nullptr;
      item_index = -1;
    }
  }
  return *this;
}

bool IndexIterator::operator==(const IndexIterator &itr) const {
  return current_page_id == itr.current_page_id && item_index == itr.item_index;
}

bool IndexIterator::operator!=(const IndexIterator &itr) const {
  return !(*this == itr);
}
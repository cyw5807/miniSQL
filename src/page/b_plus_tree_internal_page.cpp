#include "page/b_plus_tree_internal_page.h"

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(page_id_t))
#define key_off 0
#define val_off GetKeySize()

/**
 * TODO: Student Implement - Done
 */
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
void InternalPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE); // This should be defined in BPlusTreePage or a common header
  SetKeySize(key_size);
  SetMaxSize(max_size); // max_size is likely the number of (key,pointer) PAIRS this page can hold
  SetParentPageId(parent_id);
  SetPageId(page_id);
  SetSize(0);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *InternalPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void InternalPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

page_id_t InternalPage::ValueAt(int index) const {
  return *reinterpret_cast<const page_id_t *>(pairs_off + index * pair_size + val_off);
}

void InternalPage::SetValueAt(int index, page_id_t value) {
  *reinterpret_cast<page_id_t *>(pairs_off + index * pair_size + val_off) = value;
}

int InternalPage::ValueIndex(const page_id_t &value) const {
  for (int i = 0; i < GetSize(); ++i) {
    if (ValueAt(i) == value)
      return i;
  }
  return -1;
}

// UNDO
void *InternalPage::PairPtrAt(int index) {
  return reinterpret_cast<void*>(pairs_off + index * pair_size);
}

void InternalPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(page_id_t)));
}
/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 * 用了二分查找
 */
page_id_t InternalPage::Lookup(const GenericKey *key, const KeyManager &KM) {
  int size = GetSize();
  if (size <= 1 || KM.CompareKeys(key, KeyAt(1)) < 0) {
      return ValueAt(0);
  }

  int left = 1, right = size - 1;

  while(left < right) {
    int mid = (left + right + 1) / 2;
    if (KM.CompareKeys(key, KeyAt(mid)) < 0) { // key < KeyAt(mid)
      right = mid - 1;
    } else { // key >= KeyAt(mid)
      left = mid;
    }
  }
  return ValueAt(left);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
void InternalPage::PopulateNewRoot(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  SetValueAt(0, old_value);
  SetKeyAt(1, new_key);
  SetValueAt(1, new_value);
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
int InternalPage::InsertNodeAfter(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  int size = GetSize();
  IncreaseSize(1);
  for(int i = size; i > 0; --i){
    if(ValueAt(i - 1) == old_value){
      SetValueAt(i, new_value);
      SetKeyAt(i, new_key);
      return GetSize();
    }
    SetValueAt(i, ValueAt(i - 1));
    SetKeyAt(i, KeyAt(i - 1));
  }
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 * buffer_pool_manager 是干嘛的？传给CopyNFrom()用于Fetch数据页
 */
void InternalPage::MoveHalfTo(InternalPage *recipient, BufferPoolManager *buffer_pool_manager) {
  int size = GetSize();
  int half_size = size / 2;
  recipient->CopyNFrom(pairs_off + half_size * pair_size, size - half_size, buffer_pool_manager);
  IncreaseSize( - (size - half_size));
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 *
 */
void InternalPage::CopyNFrom(void *src, int size, BufferPoolManager *buffer_pool_manager) {
  PairCopy(PairPtrAt(GetSize()), src, size);
  for (int i = 0; i < size; ++i) {
    page_id_t child_page_id = ValueAt(GetSize() + i);
    Page *child_page = buffer_pool_manager->FetchPage(child_page_id);
    if (child_page == nullptr) {
      throw std::runtime_error("Failed to fetch child page during CopyNFrom");
    }

    BPlusTreePage *bpt_child_page = reinterpret_cast<BPlusTreePage *>(child_page);
    bpt_child_page->SetParentPageId(GetPageId());

    buffer_pool_manager->UnpinPage(child_page_id, true);
  }
  IncreaseSize(size);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
void InternalPage::Remove(int index) {
  int size = GetSize();
  for(int i = index; i + 1 < size; i++){
    SetKeyAt(i, KeyAt(i + 1));
    SetValueAt(i, ValueAt(i + 1));
  }
  IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
page_id_t InternalPage::RemoveAndReturnOnlyChild() {
  page_id_t child_page_id = ValueAt(0);
  SetSize(0);
  SetValueAt(0, INVALID_PAGE_ID);
  return child_page_id;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveAllTo(InternalPage *recipient, GenericKey *middle_key, BufferPoolManager *buffer_pool_manager) {
  recipient->CopyLastFrom(middle_key, ValueAt(0), buffer_pool_manager);
  for (int i = 1; i < GetSize(); ++i) {
    recipient->CopyLastFrom(KeyAt(i), ValueAt(i), buffer_pool_manager);
  }
  // SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveFirstToEndOf(InternalPage *recipient, GenericKey *middle_key,
                                    BufferPoolManager *buffer_pool_manager) {
  page_id_t first_pointer_to_move = ValueAt(0);
  recipient->CopyLastFrom(middle_key, first_pointer_to_move, buffer_pool_manager);
  for (int i = 0; i + 1 < GetSize(); ++i) {
    SetKeyAt(i, KeyAt(i + 1));
    SetValueAt(i, ValueAt(i + 1));
  }
  IncreaseSize(-1);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManager
 */
// 将给定的键和值（子页面ID）追加到当前内部节点的末尾。
// 同时更新被追加的子页面的父节点ID为当前节点。
void InternalPage::CopyLastFrom(GenericKey *key, const page_id_t value, BufferPoolManager *buffer_pool_manager) {
  int size = GetSize();
  SetKeyAt(size, key);
  SetValueAt(size, value);
  IncreaseSize(1);

  Page *child_page = buffer_pool_manager->FetchPage(value);
  if (child_page != nullptr) {
    BPlusTreePage *bpt_child_page = reinterpret_cast<BPlusTreePage *>(child_page);
    bpt_child_page->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(value, true);
  }
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
void InternalPage::MoveLastToFrontOf(InternalPage *recipient, GenericKey *middle_key,
                                     BufferPoolManager *buffer_pool_manager) {
  int size = GetSize();
  page_id_t last_pointer_from_this = ValueAt(size - 1);
  recipient->CopyFirstFrom(last_pointer_from_this, buffer_pool_manager);
  recipient->SetKeyAt(0, middle_key);
  if(size > 1) middle_key = KeyAt(size - 1);
  IncreaseSize(-1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyFirstFrom(const page_id_t value, BufferPoolManager *buffer_pool_manager) {
   int size = GetSize();
    for (int i = size; i > 0; --i) {
        SetKeyAt(i, KeyAt(i - 1));     
        SetValueAt(i, ValueAt(i - 1));
    }
    SetValueAt(0, value);
    IncreaseSize(1);

    Page *child_page = buffer_pool_manager->FetchPage(value);
    if (child_page != nullptr) {
        BPlusTreePage *bpt_child_page = reinterpret_cast<BPlusTreePage *>(child_page);
        bpt_child_page->SetParentPageId(GetPageId());
        buffer_pool_manager->UnpinPage(value, true); // Unpin and mark as dirty
    }
}
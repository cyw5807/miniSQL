#include "index/b_plus_tree.h"

#include <string>

#include "glog/logging.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

/**
 * TODO: Student Implement
 */
BPlusTree::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyManager &KM,
                     int leaf_max_size, int internal_max_size)
    : index_id_(index_id),
      buffer_pool_manager_(buffer_pool_manager),
      processor_(KM),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
}

void BPlusTree::Destroy(page_id_t current_page_id) {
  if (current_page_id == INVALID_PAGE_ID) {
    Destroy(root_page_id_);
    return;
  }

  Page *page = buffer_pool_manager_->FetchPage(current_page_id);
  if (page == nullptr) return;

  BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(page);

  if (node->IsLeafPage()) {
    buffer_pool_manager_->UnpinPage(current_page_id, true);
  } else {
    BPlusTreeInternalPage *internal_node =
        reinterpret_cast<BPlusTreeInternalPage *>(node);
    for (int i = 0; i < internal_node->GetSize(); ++i) {
      Destroy(internal_node->ValueAt(i));
    }
    buffer_pool_manager_->UnpinPage(current_page_id, true);
  }

  // 真正删除页面
  buffer_pool_manager_->DeletePage(current_page_id);

  // 如果销毁的是根节点，更新 root_page_id_
  if (current_page_id == root_page_id_) {
    root_page_id_ = INVALID_PAGE_ID;
  }
}

/*
 * Helper function to decide whether current b+tree is empty
 */
bool BPlusTree::IsEmpty() const {
  return root_page_id_ == INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
bool BPlusTree::GetValue(const GenericKey *key, std::vector<RowId> &result, Txn *transaction) {
  if (IsEmpty()) {
    return false;
  }

  page_id_t current_page_id = root_page_id_;
  Page *current_page = buffer_pool_manager_->FetchPage(current_page_id);
  BPlusTreePage *current_node = reinterpret_cast<BPlusTreePage *>(current_page);

  while (!current_node->IsLeafPage()) {
    BPlusTreeInternalPage *internal_page =
        reinterpret_cast<BPlusTreeInternalPage *>(current_node);
    page_id_t next_page_id = internal_page->Lookup(key, processor_);

    buffer_pool_manager_->UnpinPage(current_page_id, false); // 释放当前页
    current_page_id = next_page_id;
    current_page = buffer_pool_manager_->FetchPage(current_page_id);
    current_node = reinterpret_cast<BPlusTreePage *>(current_page);
  }

  BPlusTreeLeafPage *leaf_page = reinterpret_cast<BPlusTreeLeafPage *>(current_node);
  RowId value;
  bool found = leaf_page->Lookup(key, value, processor_);
  if (found) {
    result.push_back(value);
  }

  buffer_pool_manager_->UnpinPage(current_page_id, false); // 释放叶子页
  return found;
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::Insert(GenericKey *key, const RowId &value, Txn *transaction) {
  if(IsEmpty()){
    StartNewTree(key, value);
    return true;
  }
  return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
void BPlusTree::StartNewTree(GenericKey *key, const RowId &value) {
  auto *page = buffer_pool_manager_->NewPage(root_page_id_);
  if(page == nullptr){
    throw std::runtime_error("out of memory");
  }
  auto *node = reinterpret_cast<BPlusTreeLeafPage *>(page);
  node->Init(root_page_id_, INVALID_PAGE_ID, processor_.GetKeySize());
  node->Insert(key, value, processor_);
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
  UpdateRootPageId();
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::InsertIntoLeaf(GenericKey *key, const RowId &value, Txn *transaction) {
  // 查找正确的叶子节点
    Page *page = FindLeafPage(key);
    auto *node = reinterpret_cast<BPlusTreeLeafPage *>(page);

    // 检查键是否已存在
    RowId tmp_value;
    bool key_exist = node->Lookup(key, tmp_value, processor_);
    if (key_exist) {
        buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
        return false;
    }

    // 如果当前页未满，直接插入
    if (node->GetSize() < node->GetMaxSize()) {
        node->Insert(key, value, processor_);
        buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
        return true;
    }

    // 否则需要分裂
    LeafPage *new_node = Split(node, transaction);

    // 根据比较决定插入到哪个节点
    if (processor_.CompareKeys(key, new_node->KeyAt(0)) < 0) {
        node->Insert(key, value, processor_);
    } else {
        new_node->Insert(key, value, processor_);
    }

    // 设置兄弟链表关系
    new_node->SetNextPageId(node->GetNextPageId());
    node->SetNextPageId(new_node->GetPageId());

    // 设置父节点（注意：Split函数中已经设置过，但可以再确认）
    new_node->SetParentPageId(node->GetParentPageId());

    // 更新父节点
    InsertIntoParent(node, new_node->KeyAt(0), new_node, transaction);

    // 释放页面
    buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);

    return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
BPlusTreeInternalPage *BPlusTree::Split(InternalPage *node, Txn *transaction) {
  // 获取新页面
  Page *page = buffer_pool_manager_->NewPage(root_page_id_);
  if (page == nullptr) {
    throw std::runtime_error("out of memory");
  }

  // 初始化新节点
  BPlusTreeInternalPage *new_node = reinterpret_cast<BPlusTreeInternalPage *>(page);
  new_node->Init(page->GetPageId(), node->GetParentPageId(), node->GetKeySize(), node->GetMaxSize());

  // 移动一半数据到新节点
  node->MoveHalfTo(new_node, buffer_pool_manager_);

  // 注意：不需要在这里 Unpin，因为 caller 会处理
  return new_node;
}

BPlusTreeLeafPage *BPlusTree::Split(LeafPage *node, Txn *transaction) {
   // 获取新页面
    Page *page = buffer_pool_manager_->NewPage(root_page_id_);
    if (page == nullptr) {
        throw std::runtime_error("out of memory");
    }

    // 初始化新节点
    BPlusTreeLeafPage *new_node = reinterpret_cast<BPlusTreeLeafPage *>(page);
    new_node->Init(page->GetPageId(), node->GetParentPageId(), node->GetKeySize(), node->GetMaxSize());

    // 移动一半数据到新节点
    node->MoveHalfTo(new_node);

    // 设置链表关系
    new_node->SetNextPageId(node->GetNextPageId());
    node->SetNextPageId(new_node->GetPageId());

    // 注意：不需要在这里 Unpin，因为 caller 会处理

    return new_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
void BPlusTree::InsertIntoParent(BPlusTreePage *old_node, GenericKey *key, BPlusTreePage *new_node, Txn *transaction) {
  if (old_node->IsRootPage()) {
    // 创建新根
    Page *new_root_page = buffer_pool_manager_->NewPage(root_page_id_);
    if (new_root_page == nullptr) {
      throw std::runtime_error("out of memory");
    }

    auto *new_root = reinterpret_cast<BPlusTreeInternalPage *>(new_root_page);
    new_root->Init(new_root_page->GetPageId(), INVALID_PAGE_ID, processor_.GetKeySize(), internal_max_size_);

    // 设置两个子节点
    new_root->SetValueAt(0, old_node->GetPageId());
    new_root->SetValueAt(1, new_node->GetPageId());

    // 设置 key（注意：key 是分隔两者的最小 key）
    new_root->SetKeyAt(1, key);

    // 更新父子关系
    old_node->SetParentPageId(new_root->GetPageId());
    new_node->SetParentPageId(new_root->GetPageId());

    // 更新根页 ID
    root_page_id_ = new_root->GetPageId();
    UpdateRootPageId();

    // 释放页面
    buffer_pool_manager_->UnpinPage(new_root->GetPageId(), true);
    return;
  }

  // 获取父节点
  Page *parent_page = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());
  auto *parent_node = reinterpret_cast<BPlusTreeInternalPage *>(parent_page);

  // 在父节点中插入新子节点及其分隔 key
  page_id_t old_value = parent_node->Lookup(key, processor_);
  int index = parent_node->ValueIndex(old_value);
  parent_node->InsertNodeAfter(old_value, key, new_node->GetPageId());

  // 检查是否需要分裂
  if (parent_node->GetSize() > parent_node->GetMaxSize()) {
    InternalPage *new_parent_node = Split(parent_node, transaction);
    InsertIntoParent(parent_node, new_parent_node->KeyAt(0), new_parent_node, transaction);
  }

  // 释放父节点
  buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
void BPlusTree::Remove(const GenericKey *key, Txn *transaction) {
  if (IsEmpty()) {
    return;
  }

  // 查找正确的叶子页
  Page *page = FindLeafPage(key);
  BPlusTreeLeafPage *leaf_page = reinterpret_cast<BPlusTreeLeafPage *>(page);

  RowId dummy;
  // 真正执行删除
  bool removed = leaf_page->RemoveAndDeleteRecord(key, processor_);
  if (!removed) {
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false); // 没有删除内容，无需写入
    return;
  }

  // 检查是否需要合并或重新分配
  if (leaf_page->GetSize() < leaf_page->GetMinSize()) {
    // 需要进行合并或重新分配
    CoalesceOrRedistribute(leaf_page, transaction);
  } else {
    // 删除后仍合法，只需释放页面
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true); // 标记为已修改
  }

  // 若根页调整后为空，则更新根页
  if (root_page_id_ == INVALID_PAGE_ID) {
    StartNewTree(nullptr, RowId()); // 假设这是一个创建空树的方法
  }
}

/* todo
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
template <typename N>
bool BPlusTree::CoalesceOrRedistribute(N *&node, Txn *transaction) {
  // 获取父节点
  page_id_t parent_page_id = node->GetParentPageId();
  if (parent_page_id == INVALID_PAGE_ID) {
    // 如果没有父节点，说明是根节点，无需处理兄弟节点
    return false;
  }
  if(node->GetSize() > node->GetMinSize()){
    return false;
  }

  Page *parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
  BPlusTreeInternalPage *parent_node = reinterpret_cast<BPlusTreeInternalPage *>(parent_page);

  // 查找当前节点在父节点中的索引
  int index = -1;
  for (int i = 0; i < parent_node->GetSize(); ++i) {
    if (parent_node->ValueAt(i) == node->GetPageId()) {
      index = i;
      break;
    }
  }

  if (index == -1) {
    // 当前节点不在父节点中，可能是异常情况
    buffer_pool_manager_->UnpinPage(parent_page_id, false);
    return false;
  }

  // 获取兄弟节点页 ID
  page_id_t sibling_page_id = INVALID_PAGE_ID;
  if (index > 0) {
    // 左兄弟
    sibling_page_id = parent_node->ValueAt(index - 1);
  } else if (index < parent_node->GetSize() - 1) {
    // 右兄弟
    sibling_page_id = parent_node->ValueAt(index + 1);
  }

  buffer_pool_manager_->UnpinPage(parent_page_id, false);

  if (sibling_page_id == INVALID_PAGE_ID) {
    // 没有兄弟节点
    return false;
  }

  // 加载兄弟节点
  Page *sibling_page = buffer_pool_manager_->FetchPage(sibling_page_id);
  N *sibling_node = reinterpret_cast<N *>(sibling_page);

  // 判断是否需要合并或重新分配
  if (node->GetSize() + sibling_node->GetSize() <= node->GetMaxSize()) {
    // 合并操作
    return Coalesce(sibling_node, node, parent_node, index, transaction);
  } else {
    // 重新分配操作
    Redistribute(sibling_node, node, index);
    buffer_pool_manager_->UnpinPage(sibling_page_id, true);
    return false;
  }
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
bool BPlusTree::Coalesce(LeafPage *&neighbor_node, LeafPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
  // 将 node 的内容合并到 neighbor_node 中
  for (int i = 0; i < node->GetSize(); ++i) {
    neighbor_node->Insert(node->KeyAt(i), node->ValueAt(i), processor_);
  }

  // 调整叶子链表指针
  neighbor_node->SetNextPageId(node->GetNextPageId());

  // 从父节点中删除 key 和 child pointer
  parent->Remove(index);

  bool should_delete_parent = false;
  // 如果父节点不满足最小 size，继续处理
  if (parent->GetSize() < parent->GetMinSize()) {
    should_delete_parent = CoalesceOrRedistribute(parent, transaction);
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    if (should_delete_parent) {
      // 处理根节点更新
      if (parent->IsRootPage()) {
        root_page_id_ = neighbor_node->GetPageId();
        UpdateRootPageId();
      }
    }
  }

  // 删除 node 页面
  buffer_pool_manager_->DeletePage(node->GetPageId());
  return should_delete_parent;
}

bool BPlusTree::Coalesce(InternalPage *&neighbor_node, InternalPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
  GenericKey *middle_key = parent->KeyAt(index);
  node->MoveAllTo(neighbor_node, middle_key, buffer_pool_manager_);

  // 从父节点中移除 key 和 child pointer
  parent->Remove(index);

  bool should_delete_parent = false;
  // 如果父节点不满足最小 size，继续处理
  if (parent->GetSize() < parent->GetMinSize()) {
    should_delete_parent = CoalesceOrRedistribute(parent, transaction);
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    if (should_delete_parent) {
      // 处理根节点更新
      if (parent->IsRootPage()) {
        root_page_id_ = neighbor_node->GetPageId();
        UpdateRootPageId();
      }
    }
  }

  // 删除 node 页面
  buffer_pool_manager_->DeletePage(node->GetPageId());
  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
void BPlusTree::Redistribute(LeafPage *neighbor_node, LeafPage *node, int index) {
  if (index == 0) {
    // 从右兄弟拿第一个条目插入到当前节点末尾
    GenericKey *first_key = neighbor_node->KeyAt(0);
    RowId first_value = neighbor_node->ValueAt(0);

    node->Insert(first_key, first_value, processor_);

    // 删除 neighbor_node 中的第一个条目
    neighbor_node->RemoveAndDeleteRecord(first_key, processor_);
  } else {
    // 从左兄弟拿最后一个条目插入到当前节点开头
    int last_index = neighbor_node->GetSize() - 1;
    GenericKey *last_key = neighbor_node->KeyAt(last_index);
    RowId last_value = neighbor_node->ValueAt(last_index);

    node->Insert(last_key, last_value, processor_);

    // 删除 neighbor_node 中的最后一个条目
    neighbor_node->RemoveAndDeleteRecord(last_key, processor_);
  }
}
void BPlusTree::Redistribute(InternalPage *neighbor_node, InternalPage *node, int index) {
  if (index == 0) {
    // 当前节点是第一个子节点，从右兄弟借一个条目插入到当前节点末尾
    GenericKey *first_key = neighbor_node->KeyAt(0);
    page_id_t first_child = neighbor_node->ValueAt(0);

    // 插入到最后
    node->SetValueAt(node->GetSize(), first_child);
    node->SetKeyAt(node->GetSize(), first_key);

    // 更新父节点 key
    auto parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
    auto parent_node = reinterpret_cast<BPlusTreeInternalPage *>(parent_page);
    parent_node->SetKeyAt(index, neighbor_node->KeyAt(1));  // 新的分隔 key
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);

    // 删除 neighbor_node 第一个条目
    neighbor_node->Remove(0);

    // 更新被移动子节点的父指针
    auto child_page = buffer_pool_manager_->FetchPage(first_child);
    auto child_node = reinterpret_cast<BPlusTreePage *>(child_page);
    child_node->SetParentPageId(node->GetPageId());
    buffer_pool_manager_->UnpinPage(first_child, true);

    // 更新 size
    node->IncreaseSize(1);
    neighbor_node->IncreaseSize(-1);
  } else {
    // 当前节点不是第一个子节点，从左兄弟借一个条目插入到当前节点开头
    int last_index = neighbor_node->GetSize() - 1;
    GenericKey *last_key = neighbor_node->KeyAt(last_index);
    page_id_t last_child = neighbor_node->ValueAt(last_index);

    // 插入到最前面
    for (int i = node->GetSize(); i > 0; --i) {
      node->SetKeyAt(i, node->KeyAt(i - 1));
      node->SetValueAt(i, node->ValueAt(i - 1));
    }

    node->SetKeyAt(0, last_key);
    node->SetValueAt(0, last_child);

    // 更新父节点 key
    auto parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
    auto parent_node = reinterpret_cast<BPlusTreeInternalPage *>(parent_page);
    parent_node->SetKeyAt(index, last_key);  // 或根据具体情况调整
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);

    // 更新被移动子节点的父指针
    auto child_page = buffer_pool_manager_->FetchPage(last_child);
    auto child_node = reinterpret_cast<BPlusTreePage *>(child_page);
    child_node->SetParentPageId(node->GetPageId());
    buffer_pool_manager_->UnpinPage(last_child, true);

    // 删除 neighbor_node 最后一个条目
    neighbor_node->IncreaseSize(-1);

    // 更新 size
    node->IncreaseSize(1);
  }
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
bool BPlusTree::AdjustRoot(BPlusTreePage *old_root_node) {
  // 如果根节点大小仍符合最小要求，无需调整
  if (old_root_node->GetSize() > old_root_node->GetMinSize()) {
    return false;
  }

  // 情况 1: 根节点是 InternalPage 且只剩一个子节点
  if (!old_root_node->IsLeafPage()) {
    auto internal_root = reinterpret_cast<BPlusTreeInternalPage *>(old_root_node);

    if (internal_root->GetSize() == 1) {
      page_id_t new_root_id = internal_root->ValueAt(0);
      auto new_root_page = buffer_pool_manager_->FetchPage(new_root_id);
      auto new_root_node = reinterpret_cast<BPlusTreePage *>(new_root_page);

      // 更新新根节点的父节点为 INVALID_PAGE_ID
      new_root_node->SetParentPageId(INVALID_PAGE_ID);

      buffer_pool_manager_->UnpinPage(new_root_id, true);
      buffer_pool_manager_->DeletePage(old_root_node->GetPageId());

      root_page_id_ = new_root_id;
      UpdateRootPageId();

      return true; // 旧根节点被删除
    }
  }
  // 情况 2: 根节点是 LeafPage 且没有任何数据
  else {
    auto leaf_root = reinterpret_cast<BPlusTreeLeafPage *>(old_root_node);
    if (leaf_root->GetSize() == 0) {
      buffer_pool_manager_->DeletePage(leaf_root->GetPageId());
      root_page_id_ = INVALID_PAGE_ID;
      return true; // 整棵树已清空
    }
  }

  return false; // 不需要删除根节点
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin() {
  if (IsEmpty()) {
      return IndexIterator(); // 空树返回空迭代器
  }

  page_id_t current_page_id = root_page_id_;
  BPlusTreePage *current_page = nullptr;

  // 向下查找最左侧的叶子节点
  while (true) {
      Page *page = buffer_pool_manager_->FetchPage(current_page_id);
      current_page = reinterpret_cast<BPlusTreePage *>(page);

      if (current_page->IsLeafPage()) {
          break;
      } else {
          auto *internal_page = reinterpret_cast<BPlusTreeInternalPage *>(current_page);
          page_id_t next_page_id = internal_page->ValueAt(0);
          buffer_pool_manager_->UnpinPage(current_page_id, false); // 当前页不是叶子，不需要写入
          current_page_id = next_page_id;
      }
  }

  // 构造迭代器：指向最左边叶子页的第一个 key
  LeafPage *leftmost_leaf = reinterpret_cast<LeafPage *>(current_page);
  RowId dummy;
  GenericKey *first_key = leftmost_leaf->KeyAt(0);

  // 返回指向第一个有效元素的迭代器
  return IndexIterator(leftmost_leaf->GetPageId(),  buffer_pool_manager_, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin(const GenericKey *key) {
   if (IsEmpty()) {
        return IndexIterator(); // 空树返回空迭代器
    }

    // 查找包含 key 的叶子页
    Page *page = FindLeafPage(key);
    LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page);

    RowId value;
    int index;

    // 在叶子页中查找 key 所在的槽位
    bool found = leaf_page->Lookup(key, value, processor_);

    if (!found) {
        // 如果没找到 key，返回 end() 迭代器
        buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
        return End();
    }

    // 返回指向该 key 的迭代器
    return IndexIterator(leaf_page->GetPageId(), buffer_pool_manager_, index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
IndexIterator BPlusTree::End() {
  if (IsEmpty()) {
    return IndexIterator(); // 空树直接返回空迭代器
  }

  page_id_t current_page_id = root_page_id_;

  while (true) {
    Page *page = buffer_pool_manager_->FetchPage(current_page_id);
    BPlusTreePage *current_page = reinterpret_cast<BPlusTreePage *>(page);

    if (current_page->IsLeafPage()) {
      LeafPage *leaf_page = reinterpret_cast<LeafPage *>(current_page);
      // 找到最后一个 key 的下一个位置（即 end）
      int index = leaf_page->GetSize(); // 超出范围，表示 end
      return IndexIterator(leaf_page->GetPageId(), buffer_pool_manager_, index);
    } else {
      auto *internal_page = reinterpret_cast<BPlusTreeInternalPage *>(current_page);
      page_id_t next_page_id = internal_page->ValueAt(internal_page->GetSize() - 1);
      buffer_pool_manager_->UnpinPage(current_page_id, false);
      current_page_id = next_page_id;
    }
  }
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
Page *BPlusTree::FindLeafPage(const GenericKey *key, page_id_t page_id, bool leftMost) {
  page_id_t current_page_id = page_id;
  BPlusTreePage *current_page = nullptr;

  while (true) {
    Page *page = buffer_pool_manager_->FetchPage(current_page_id);
    current_page = reinterpret_cast<BPlusTreePage *>(page);

    if (current_page->IsLeafPage()) {
      break;
    }

    if (leftMost) {
      // 如果只需要最左页，一直往第一个子节点走
      auto *internal_page = reinterpret_cast<BPlusTreeInternalPage *>(current_page);
      page_id_t next_page_id = internal_page->ValueAt(0);
      buffer_pool_manager_->UnpinPage(current_page_id, false); // 当前页非叶子页，不需要写入
      current_page_id = next_page_id;
    } else {
      // 正常查找路径
      auto *internal_page = reinterpret_cast<BPlusTreeInternalPage *>(current_page);
      page_id_t next_page_id = internal_page->Lookup(key, processor_); // 返回应继续查找的 child page id
      buffer_pool_manager_->UnpinPage(current_page_id, false);
      current_page_id = next_page_id;
    }
  }

  return reinterpret_cast<Page *>(current_page);
}

/*
 * Update/Insert root page id in header page(where page_id = INDEX_ROOTS_PAGE_ID,
 * header_page isdefined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, current_page_id> into header page instead of
 * updating it.
 */
void BPlusTree::UpdateRootPageId(int insert_record) {
  Page *header_page = buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID);
  if (header_page == nullptr) {
    throw std::runtime_error("header page not found");
  }

  IndexRootsPage *header = reinterpret_cast<IndexRootsPage *>(header_page);

  if (insert_record) {
    header->Insert(index_id_, root_page_id_);
  } else {
    header->Update(index_id_, root_page_id_);
  }

  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
}

/**
 * This method is used for debug only, You don't need to modify
 */
void BPlusTree::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out, Schema *schema) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      Row ans;
      processor_.DeserializeToKey(leaf->KeyAt(i), ans, schema);
      out << "<TD>" << ans.GetField(0)->toString() << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        Row ans;
        processor_.DeserializeToKey(inner->KeyAt(i), ans, schema);
        out << ans.GetField(0)->toString();
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out, schema);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
void BPlusTree::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

bool BPlusTree::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}
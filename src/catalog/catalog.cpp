#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
  ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
  MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
  buf += 4;
  MACH_WRITE_UINT32(buf, table_meta_pages_.size());
  buf += 4;
  MACH_WRITE_UINT32(buf, index_meta_pages_.size());
  buf += 4;
  for (auto iter : table_meta_pages_) {
    MACH_WRITE_TO(table_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
  for (auto iter : index_meta_pages_) {
    MACH_WRITE_TO(index_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf) {
  // check valid
  uint32_t magic_num = MACH_READ_UINT32(buf);
  buf += 4;
  ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
  // get table and index nums
  uint32_t table_nums = MACH_READ_UINT32(buf);
  buf += 4;
  uint32_t index_nums = MACH_READ_UINT32(buf);
  buf += 4;
  // create metadata and read value
  CatalogMeta *meta = new CatalogMeta();
  for (uint32_t i = 0; i < table_nums; i++) {
    auto table_id = MACH_READ_FROM(table_id_t, buf);
    buf += 4;
    auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
  }
  for (uint32_t i = 0; i < index_nums; i++) {
    auto index_id = MACH_READ_FROM(index_id_t, buf);
    buf += 4;
    auto index_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->index_meta_pages_.emplace(index_id, index_page_id);
  }
  return meta;
}

/**
 * TODO: Student Implement - Done
 */
uint32_t CatalogMeta::GetSerializedSize() const {
  //ASSERT(false, "Not Implemented yet");
  //magic_num + table_count + index_count
  uint32_t size = sizeof(uint32_t) * 3; // MAGIC_NUM, table_meta_pages_.size(), index_meta_pages_.size()
  // table_meta_pages_ (table_id_t + page_id_t)
  size += table_meta_pages_.size() * (sizeof(table_id_t) + sizeof(page_id_t));
  // index_meta_pages_ (index_id_t + page_id_t)
  size += index_meta_pages_.size() * (sizeof(index_id_t) + sizeof(page_id_t));
  return size;
}

CatalogMeta::CatalogMeta() {}

/**
 * TODO: Student Implement - Done
 */
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {
      if (init) {
        // 为新数据库初始化目录，或覆盖现有目录
        catalog_meta_ = CatalogMeta::NewInstance(); // 创建一个新的、空的 CatalogMeta
        FlushCatalogMetaPage();
        next_table_id_.store(0);                    // 设置初始的 next_table_id
        next_index_id_.store(0);                    // 设置初始的 next_index_id
      } else {
        // 从现有数据库初始化目录
        Page *meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
        if (meta_page == nullptr) {
          LOG(ERROR) << "Failed to fetch catalog meta page " << CATALOG_META_PAGE_ID
                      << " for existing catalog. Database might be new, corrupted, or BPM failed.";
          throw std::runtime_error("Failed to load catalog: could not fetch meta page.");
          return;
        }

        // 从页面数据反序列化 CatalogMeta
        catalog_meta_ = CatalogMeta::DeserializeFrom(meta_page->GetData());
        // 页面仅被读取，未被修改，所以 is_dirty 为 false
        buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, false);

        if (catalog_meta_ == nullptr) {
          LOG(ERROR) << "Failed to deserialize catalog metadata from page " << CATALOG_META_PAGE_ID
                      << ". Catalog integrity compromised.";
          // delete catalog_meta_; // 确保在抛出前清理
          // catalog_meta_ = nullptr;
          throw std::runtime_error("Failed to load catalog: deserialization failed.");
          return;
        }

        // 从加载的元数据初始化 next_table_id_ 和 next_index_id_
        next_table_id_.store(catalog_meta_->GetNextTableId());
        next_index_id_.store(catalog_meta_->GetNextIndexId());

        // 加载所有表的定义信息
        // 注意: GetTableMetaPages() 在 catalog.h 中被标记为 "Used only for testing"
        // 这里假设我们可以使用它来读取元数据信息，或者 CatalogMeta 提供了其他迭代方式
        if (catalog_meta_->GetTableMetaPages() != nullptr) {
          for (const auto &pair : (*catalog_meta_->GetTableMetaPages())) {
            table_id_t table_id = pair.first;
            page_id_t table_meta_page_id = pair.second;
            // 调用 LoadTable
            // LoadTable 会获取页面，反序列化 TableMetadata, 创建 TableInfo,
            // 并填充 CatalogManager 的 tables_ 和 table_names_ 映射
            if (LoadTable(table_id, table_meta_page_id) != DB_SUCCESS) {
              LOG(WARNING) << "Failed to load metadata for table id: " << table_id
                            << " from page id: " << table_meta_page_id;
            }
          }
        }

        // 加载所有索引的定义信息
        if (catalog_meta_->GetIndexMetaPages() != nullptr) {
          for (const auto &pair : (*catalog_meta_->GetIndexMetaPages())) {
            index_id_t index_id = pair.first;
            page_id_t index_meta_page_id = pair.second;
            // 调用 LoadIndex
            // LoadIndex 会获取页面，反序列化 IndexMetadata, 找到对应的 TableInfo,
            // 创建并初始化 IndexInfo, 并填充 CatalogManager 的 indexes_ 和 index_names_ 映射
            if (LoadIndex(index_id, index_meta_page_id) != DB_SUCCESS) {
              LOG(WARNING) << "Failed to load metadata for index id: " << index_id
                            << " from page id: " << index_meta_page_id;
              // 错误处理策略
            }
          }
        }
      }
//    ASSERT(false, "Not Implemented yet");
}

CatalogManager::~CatalogManager() {
  FlushCatalogMetaPage();
  delete catalog_meta_;
  for (auto iter : tables_) {
    delete iter.second;
  }
  for (auto iter : indexes_) {
    delete iter.second;
  }
}

/**
 * TODO: Student Implement - Done
 */
dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema, Txn *txn, TableInfo *&table_info) {
  // 检查表名是否已存在
  if (table_names_.count(table_name)) {
    return DB_TABLE_ALREADY_EXIST;
  }
  // 生成新的表ID
  table_id_t new_table_id = next_table_id_.fetch_add(1);
  // Declare page_id and table_heap_root_id
  page_id_t table_heap_root_id;
  page_id_t meta_page_id; // page_id for table metadata

  // Create the table heap root page
  Page *generic_heap_root_page = buffer_pool_manager_->NewPage(table_heap_root_id);
  if (generic_heap_root_page == nullptr) {
    return DB_FAILED;
  }

  // Initialize table_heap_root_page
  // Cast the generic Page* to TablePage* to call its Init method.
  TablePage *table_heap_root_page_obj = static_cast<TablePage *>(generic_heap_root_page);
  table_heap_root_page_obj->Init(table_heap_root_id, INVALID_PAGE_ID, log_manager_, txn);
  // It will be unpinned at the end as per pseudocode.

  // Deep copy the schema
  TableSchema *tmp_schema = Schema::DeepCopySchema(schema);
  if (tmp_schema == nullptr) {
    // Cleanup: Unpin and delete the allocated table heap root page
    buffer_pool_manager_->UnpinPage(table_heap_root_id, false); // Not dirty from a failed schema copy perspective
    buffer_pool_manager_->DeletePage(table_heap_root_id);
    return DB_FAILED; // Schema copy failed
  }

  // Create table metadata
  TableMetadata *table_meta = TableMetadata::Create(new_table_id, table_name, table_heap_root_id, tmp_schema);
  if (table_meta == nullptr) {
    delete tmp_schema;
    buffer_pool_manager_->UnpinPage(table_heap_root_id, false); // Page was inited but op failed before table fully formed
    buffer_pool_manager_->DeletePage(table_heap_root_id);
    return DB_FAILED; // TableMetadata creation failed
  }

  // Create a new page for the table metadata
  Page *page_for_meta = buffer_pool_manager_->NewPage(meta_page_id);
  // If page is null
  if (page_for_meta == nullptr) {
    delete table_meta; // table_meta owns tmp_schema
    buffer_pool_manager_->UnpinPage(table_heap_root_id, false); // As above
    buffer_pool_manager_->DeletePage(table_heap_root_id);
    return DB_FAILED; // Line 18
  }

  // Serialize table_meta to the data of page (page_for_meta)
  table_meta->SerializeTo(page_for_meta->GetData());
  // page_for_meta is now pinned and dirty. It will be unpinned at the end.

  // Create the table heap
  TableHeap *table_heap_obj = nullptr; // 'table' in pseudocode
  try {
    table_heap_obj = TableHeap::Create(buffer_pool_manager_, table_heap_root_id, table_meta->GetSchema(), log_manager_, lock_manager_);
  } catch (const std::bad_alloc &) {
    buffer_pool_manager_->UnpinPage(meta_page_id, false); // Not dirty from this failure's perspective
    buffer_pool_manager_->DeletePage(meta_page_id);
    delete table_meta; // owns tmp_schema
    buffer_pool_manager_->UnpinPage(table_heap_root_id, true); 
    buffer_pool_manager_->DeletePage(table_heap_root_id);
    return DB_FAILED;
  }

  // Create TableInfo
  TableInfo *t_info = nullptr;
  try {
    t_info = TableInfo::Create();
    if (!t_info) throw std::bad_alloc();
    // Initialize t_info with table_meta and table_heap_obj
    t_info->Init(table_meta, table_heap_obj); // TableInfo takes ownership
  } catch (const std::bad_alloc &) {
    delete table_heap_obj;
    buffer_pool_manager_->UnpinPage(meta_page_id, false);
    buffer_pool_manager_->DeletePage(meta_page_id);
    delete table_meta; // owns tmp_schema
    buffer_pool_manager_->UnpinPage(table_heap_root_id, true); // Dirty from Init
    buffer_pool_manager_->DeletePage(table_heap_root_id);
    return DB_FAILED;
  }

  // Add table_name and table_id to table_names_
  table_names_.emplace(table_name, new_table_id);
  // Add table_id and t_info to tables_
  tables_.emplace(new_table_id, t_info);

  // Add table_id and meta_page_id to catalog_meta_.table_meta_pages_
  if (catalog_meta_ != nullptr && catalog_meta_->GetTableMetaPages() != nullptr) {
    catalog_meta_->GetTableMetaPages()->emplace(new_table_id, meta_page_id);
  } else {
    LOG(ERROR) << "Catalog_meta_ is not initialized properly. Cannot record table metadata page.";
    table_names_.erase(table_name);
    auto it_tables = tables_.find(new_table_id);
    if (it_tables != tables_.end()) { delete it_tables->second; tables_.erase(it_tables); } else { delete t_info; }
    buffer_pool_manager_->UnpinPage(meta_page_id, false); // Not dirty if this step failed before flush
    buffer_pool_manager_->DeletePage(meta_page_id);
    buffer_pool_manager_->UnpinPage(table_heap_root_id, true); // Dirty from Init
    buffer_pool_manager_->DeletePage(table_heap_root_id);
    return DB_FAILED;
  }

  // Set the output parameter table_info to t_info
  table_info = t_info;

  // Unpin page with meta_page_id and modifications
  // This page (page_for_meta) was dirtied by SerializeTo.
  buffer_pool_manager_->UnpinPage(meta_page_id, true /*is_dirty*/);

  // Unpin table_heap_root_page with table_heap_root_id and modifications
  // This page (generic_heap_root_page / table_heap_root_page_obj) was dirtied by TablePage::Init.
  buffer_pool_manager_->UnpinPage(table_heap_root_id, true /*is_dirty*/);

  // Flush catalog metadata to disk
  dberr_t flush_status = FlushCatalogMetaPage();
  if (flush_status != DB_SUCCESS) {
    LOG(ERROR) << "Failed to flush catalog meta page after creating table " << table_name
               << ". Attempting to roll back.";
    table_names_.erase(table_name);
    auto it_tables = tables_.find(new_table_id);
    if (it_tables != tables_.end()) { delete it_tables->second; tables_.erase(it_tables); }
    if (catalog_meta_ != nullptr && catalog_meta_->GetTableMetaPages() != nullptr) {
      catalog_meta_->GetTableMetaPages()->erase(new_table_id);
    }
    // Pages were already unpinned. A more robust rollback might try to delete them
    table_info = nullptr;
    return flush_status;
  }

  return DB_SUCCESS;
  // ASSERT(false, "Not Implemented yet");
}

/**
 * TODO: Student Implement - Done
 */
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  auto it_name = table_names_.find(table_name);
  if (it_name == table_names_.end()) {
    table_info = nullptr;
    return DB_TABLE_NOT_EXIST;
  } else {
    table_id_t table_id = it_name->second;
    auto it_table_info = tables_.find(table_id);
    if (it_table_info == tables_.end()) {
      table_info = nullptr;
      LOG(ERROR) << "Catalog inconsistency: Table name '" << table_name << "' with id " << table_id
                 << " found in table_names_ map, but no corresponding TableInfo in tables_ map.";
      return DB_FAILED;
    }
    table_info = it_table_info->second;
    return DB_SUCCESS;
  }
  // ASSERT(false, "Not Implemented yet");
}

/**
 * TODO: Student Implement - Done
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  // Check if there are any tables in tables_
  // If tables_ is empty
  if (this->tables_.empty()) {
    // Return DB_TABLE_NOT_EXIST
    return DB_TABLE_NOT_EXIST;
  }
  // Iterate through all entries in tables_
  // For each iter in tables_
  for (const auto &pair : this->tables_) {
    // Add iter.second to tables
    tables.push_back(pair.second);
  }
  return DB_SUCCESS;
  // ASSERT(false, "Not Implemented yet");
}

/**
 * TODO: Student Implement - Done
 */
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Txn *txn, IndexInfo *&index_info,
                                    const string &index_type) {
  // 初始化输出参数
  index_info = nullptr;

  // 获取表信息并检查表是否存在
  TableInfo *local_table_info = nullptr;
  if (GetTable(table_name, local_table_info) != DB_SUCCESS) {
    return DB_TABLE_NOT_EXIST;
  }
  ASSERT(local_table_info != nullptr, "GetTable returned DB_SUCCESS but local_table_info is null.");

  // 检查索引是否已在该表上存在
  auto it_table_map_for_indexes = index_names_.find(table_name);
  if (it_table_map_for_indexes != index_names_.end()) {
    const auto &name_to_id_map = it_table_map_for_indexes->second;
    if (name_to_id_map.count(index_name)) {
      return DB_INDEX_ALREADY_EXIST;
    }
  }

  // 从 TableInfo 获取 table_id
  table_id_t table_id = local_table_info->GetTableId();

  // --- 调试建议 1 开始 ---
  LOG(INFO) << "[CatalogManager::CreateIndex] For index '" << index_name << "' on table '" << table_name << "':";
  std::string index_keys_str = "  Index Keys (names): ";
  for (const auto& key_name : index_keys) {
    index_keys_str += key_name + " ";
  }
  LOG(INFO) << index_keys_str;

  std::vector<uint32_t> key_map; // 这个 key_map 将被用于创建 IndexMetadata
  key_map.reserve(index_keys.size());
  TableSchema *table_schema = local_table_info->GetSchema();
  if (table_schema == nullptr) {
      LOG(ERROR) << "[CatalogManager::CreateIndex] Table " << table_name << " has no schema.";
      return DB_FAILED;
  }
  for (const std::string &key_column_name : index_keys) {
    uint32_t column_index;
    dberr_t col_idx_res = table_schema->GetColumnIndex(key_column_name, column_index); // 获取列索引
    if (col_idx_res != DB_SUCCESS) {
      LOG(ERROR) << "[CatalogManager::CreateIndex] Column '" << key_column_name << "' not found in table '" << table_name << "'.";
      return DB_COLUMN_NAME_NOT_EXIST; // 或者 col_idx_res
    }
    key_map.push_back(column_index);
  }
  if (key_map.empty()) {
      LOG(WARNING) << "[CatalogManager::CreateIndex] Attempted to create index '" << index_name << "' on table '" << table_name << "' with no key columns.";
      return DB_FAILED; // 或 DB_INDEX_KEYS_EMPTY
  }

  std::string key_map_str = "  Generated key_map (column indices in table schema): ";
  for (uint32_t col_idx : key_map) {
    key_map_str += std::to_string(col_idx) + " ";
  }
  LOG(INFO) << key_map_str;
  // --- 调试建议 1 结束 (key_map 生成部分的日志) ---

  index_id_t new_index_id = next_index_id_.fetch_add(1);

  page_id_t index_meta_page_id;
  Page *index_meta_disk_page = buffer_pool_manager_->NewPage(index_meta_page_id);
  if (index_meta_disk_page == nullptr) {
    return DB_FAILED;
  }

  IndexMetadata *index_meta = IndexMetadata::Create(new_index_id, index_name, table_id, key_map);
  if (index_meta == nullptr) {
    buffer_pool_manager_->UnpinPage(index_meta_page_id, false);
    buffer_pool_manager_->DeletePage(index_meta_page_id);
    return DB_FAILED;
  }
  index_meta->SerializeTo(index_meta_disk_page->GetData());

  IndexInfo *new_index_info_obj = nullptr;
  bool init_succeeded_and_took_ownership = false;
  try {
    new_index_info_obj = IndexInfo::Create();
    if (!new_index_info_obj) throw std::bad_alloc();
    
    new_index_info_obj->Init(index_meta, local_table_info, buffer_pool_manager_); // index_type 参数被移除，因为 IndexInfo::Init 不接受
    init_succeeded_and_took_ownership = true; // 假设 Init 成功则获取所有权

    // --- 调试 2 ---
    // 在 IndexInfo::Init 调用之后检查其结果
    LOG(INFO) << "[CatalogManager::CreateIndex] After IndexInfo::Init for index '" << new_index_info_obj->GetIndexName() << "':";
    IndexSchema* resulting_key_schema = new_index_info_obj->GetIndexKeySchema();
    if (resulting_key_schema == nullptr) {
        LOG(ERROR) << "  IndexInfo::Init resulted in a null key_schema_!";
        // 如果 key_schema_ 为空，后续操作很可能会失败，这里应该处理这个错误
        // 例如，清理已创建的资源并返回失败
        // delete new_index_info_obj; // 它会删除 index_meta (如果 Init 中已赋值)
        // buffer_pool_manager_->UnpinPage(index_meta_page_id, true);
        // buffer_pool_manager_->DeletePage(index_meta_page_id);
        // catalog_meta_->GetIndexMetaPages()->erase(new_index_id); // 如果已经添加了
        // return DB_FAILED; // 或者更具体的错误码
        // 为了不改变原函数的结构，这里只打日志，让后续的 ASSERT 或操作失败
    } else {
        LOG(INFO) << "  Resulting key_schema_ column count: " << resulting_key_schema->GetColumnCount();
        LOG(INFO) << "  Original key_map size used for IndexMetadata: " << key_map.size();
        if (resulting_key_schema->GetColumnCount() != key_map.size()) {
            LOG(ERROR) << "  MISMATCH! key_schema_ column count (" << resulting_key_schema->GetColumnCount()
                       << ") does not match original key_map size (" << key_map.size() << ").";
        }
    }
    if (new_index_info_obj->GetIndex() == nullptr && resulting_key_schema != nullptr) {
        LOG(ERROR) << "  IndexInfo::Init resulted in a null underlying Index object, even though key_schema might be valid.";
    }
    // --- 调试 2 结束 ---

  } catch (const std::bad_alloc &e) {
    if (!init_succeeded_and_took_ownership) delete index_meta;
    delete new_index_info_obj; 
    buffer_pool_manager_->UnpinPage(index_meta_page_id, true);
    buffer_pool_manager_->DeletePage(index_meta_page_id);
    LOG(ERROR) << "[CatalogManager::CreateIndex] Memory allocation failed: " << e.what();
    return DB_FAILED;
  } catch (const std::exception &e) { // 捕获 Init 可能抛出的其他标准异常
    LOG(ERROR) << "[CatalogManager::CreateIndex] Exception during IndexInfo::Init: " << e.what();
    if (new_index_info_obj) { delete new_index_info_obj; } 
    else if (!init_succeeded_and_took_ownership) { delete index_meta; }
    buffer_pool_manager_->UnpinPage(index_meta_page_id, true);
    buffer_pool_manager_->DeletePage(index_meta_page_id);
    return DB_FAILED;
  }

  // 更新索引名称映射
  index_names_[table_name].emplace(index_name, new_index_id);

  // 更新索引映射
  indexes_.emplace(new_index_id, new_index_info_obj);

  // 将索引元数据页信息添加到 catalog_meta_
  if (catalog_meta_ != nullptr && catalog_meta_->GetIndexMetaPages() != nullptr) {
    catalog_meta_->GetIndexMetaPages()->emplace(new_index_id, index_meta_page_id);
  } else {
    LOG(FATAL) << "Catalog_meta_ is not initialized properly. Cannot record new index meta page.";
    return DB_FAILED; 
  }

  // 解除索引元数据页面的固定
  buffer_pool_manager_->UnpinPage(index_meta_page_id, true);

  // 设置输出参数
  index_info = new_index_info_obj;

  // 14. 持久化目录元数据 (伪代码 Line 57-58)
  dberr_t flush_status = FlushCatalogMetaPage();
  if (flush_status != DB_SUCCESS) {
    LOG(ERROR) << "Failed to flush catalog meta page after creating index " << index_name << " on table " << table_name
               << ". Attempting to roll back changes.";
    // 尝试回滚内存中的更改
    index_names_[table_name].erase(index_name);
    if (index_names_[table_name].empty()) {
      index_names_.erase(table_name);
    }
    auto it_indexes = indexes_.find(new_index_id);
    if (it_indexes != indexes_.end()) { delete it_indexes->second; indexes_.erase(it_indexes); }
    if (catalog_meta_ != nullptr && catalog_meta_->GetIndexMetaPages() != nullptr) {
      catalog_meta_->GetIndexMetaPages()->erase(new_index_id);
    }
    // buffer_pool_manager_->DeletePage(index_meta_page_id); 
    index_info = nullptr;
    return flush_status;
  }

  return DB_SUCCESS;
  // ASSERT(false, "Not Implemented yet");
}

/**
 * TODO: Student Implement - Done
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  // 初始化输出参数
  index_info = nullptr;

  // 检查表本身是否存在于目录中 (通过 table_names_ 映射)
  if (table_names_.count(table_name) == 0) {
    return DB_TABLE_NOT_EXIST;
  }

  // 在 index_names_ 映射中查找表名 (外层map: table_name -> map<index_name, index_id>)
  auto it_table_indexes = index_names_.find(table_name);
  if (it_table_indexes == index_names_.end()) {
    return DB_INDEX_NOT_FOUND; // 符合描述：索引名不存在
  }

  // 获取指定表对应的内部映射 (index_name -> index_id)
  const auto &name_to_id_map = it_table_indexes->second;

  // 在内部映射中查找索引名
  auto it_index_name = name_to_id_map.find(index_name);
  if (it_index_name == name_to_id_map.end()) {
    // 表有索引记录，但没有名为 index_name 的这个特定索引。
    return DB_INDEX_NOT_FOUND;
  }

  // 获取 index_id
  index_id_t index_id = it_index_name->second;

  // 使用 index_id 在 indexes_ 映射中查找 IndexInfo*
  auto it_index_info = indexes_.find(index_id);
  if (it_index_info == indexes_.end()) {
    // 这是一个内部不一致的状态：index_names_ 中有记录，但 indexes_ 中没有对应的 IndexInfo 对象。
    LOG(ERROR) << "Catalog inconsistency: Index '" << index_name << "' on table '" << table_name
               << "' (id: " << index_id << ") found in index_names_ map, but no corresponding IndexInfo in indexes_ map.";
    return DB_FAILED; // 或者一个特定的目录不一致错误码
  }

  // 找到索引，设置输出参数
  index_info = it_index_info->second;
  return DB_SUCCESS;
  // ASSERT(false, "Not Implemented yet");
}

/**
 * TODO: Student Implement - Done
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  // 检查表本身是否存在
  if (table_names_.count(table_name) == 0) {
    return DB_TABLE_NOT_EXIST;
  }

  // 在 index_names_ 中查找表名对应的索引名->索引ID映射
  auto it_table_indexes = index_names_.find(table_name);
  if (it_table_indexes == index_names_.end()) {
    // 表存在 (已通过上面的检查)，但在 index_names_ 中没有条目，
    // 意味着该表当前没有任何已记录的索引。
    return DB_INDEX_NOT_FOUND;
  }

  // 获取该表的所有 (索引名 -> 索引ID) 映射
  const auto &index_name_to_id_map = it_table_indexes->second;
  if (index_name_to_id_map.empty()) {
    // 表在 index_names_ 中有条目，但其索引映射为空，也意味着没有索引。
    return DB_INDEX_NOT_FOUND;
  }

  // 遍历该表的所有索引ID，并从 indexes_ 映射中获取 IndexInfo*
  for (const auto &name_id_pair : index_name_to_id_map) {
    index_id_t current_index_id = name_id_pair.second; // 获取索引ID
    auto it_index_info = indexes_.find(current_index_id);
    if (it_index_info == indexes_.end()) {
      // 严重错误：目录数据不一致。
      // index_names_ 中记录的 index_id 在 indexes_ 映射中找不到对应的 IndexInfo。
      LOG(ERROR) << "Catalog inconsistency: Index ID " << current_index_id
                 << " for an index on table '" << table_name << "' (index name: '" << name_id_pair.first
                 << "') found in index_names_ but no corresponding IndexInfo in indexes_ map.";
      indexes.clear(); 
      return DB_FAILED; // 或者一个特定的目录损坏错误码
    }
    // 找到 IndexInfo，添加到输出向量
    indexes.push_back(it_index_info->second);
  }

  return DB_SUCCESS;
  // ASSERT(false, "Not Implemented yet");
}

/**
 * TODO: Student Implement - Done
 */
dberr_t CatalogManager::DropTable(const string &table_name) {
  // 初始化 local_table_info
  TableInfo *local_table_info = nullptr;

  // 检查表是否存在
  if (GetTable(table_name, local_table_info) != DB_SUCCESS) {
    return DB_TABLE_NOT_EXIST;
  }
  ASSERT(local_table_info != nullptr, "GetTable succeeded but local_table_info is null.");

  table_id_t table_id = local_table_info->GetTableId(); // 获取 table_id

  // 删除与该表关联的所有索引
  std::vector<std::string> index_names_to_drop;
  if (auto it_table_indexes_map_entry = index_names_.find(table_name);
      it_table_indexes_map_entry != index_names_.end()) {
    for (const auto &name_id_pair : it_table_indexes_map_entry->second) {
      index_names_to_drop.push_back(name_id_pair.first);
    }
  }

  for (const std::string &idx_name_to_drop : index_names_to_drop) {
    dberr_t drop_idx_res = DropIndex(table_name, idx_name_to_drop); // 这个 DropIndex 会调用 FlushCatalogMetaPage
    if (drop_idx_res != DB_SUCCESS) {
      LOG(ERROR) << "Failed to drop index '" << idx_name_to_drop << "' for table '" << table_name
                 << "' during DropTable. Aborting DropTable partially completed.";
      // 此处原子性难以保证，除非有DDL事务管理器。
      // 表现在处于不一致状态，因为某些索引可能已删除，但并非全部。
      return drop_idx_res;
    }
  }
  // 成功删除所有索引后, index_names_.find(table_name) 应该返回 .end()
  // 因为 DropIndex 会从 index_names_ 中移除条目，并可能移除 table_name 键本身。

  // 获取表堆根页面ID
  // page_id_t table_heap_root_page_id = local_table_info->GetRootPageId();

  // 删除表堆管理的所有数据页
  TableHeap *table_heap = local_table_info->GetTableHeap();
  if (table_heap != nullptr) {
    table_heap->FreeTableHeap(); // FreeTableHeap 会遍历并删除所有相关页面
  }

  // 删除表元数据页面并从 catalog_meta_ 中移除记录
  page_id_t table_meta_page_id = INVALID_PAGE_ID;
  bool meta_page_entry_found_in_catalog = false;
  if (catalog_meta_ != nullptr && catalog_meta_->GetTableMetaPages() != nullptr) {
    auto it_meta = catalog_meta_->GetTableMetaPages()->find(table_id);
    if (it_meta != catalog_meta_->GetTableMetaPages()->end()) {
      table_meta_page_id = it_meta->second;
      // 先从映射中移除，再删除页面，以防删除页面失败但映射已改
      catalog_meta_->GetTableMetaPages()->erase(it_meta);
      meta_page_entry_found_in_catalog = true;
    } else {
      LOG(WARNING) << "Table ID " << table_id << " (name: " << table_name
                   << ") not found in catalog_meta_->table_meta_pages_ during DropTable.";
    }
  }
  if (meta_page_entry_found_in_catalog && table_meta_page_id != INVALID_PAGE_ID) {
    buffer_pool_manager_->DeletePage(table_meta_page_id);
  }


  // 从 CatalogManager 的内存映射中移除表
  table_names_.erase(table_name);
  tables_.erase(table_id); // 从 tables_ 映射中移除指针

  // 释放 TableInfo 对象及其拥有的资源
  // local_table_info 指向的对象现在不再被 tables_ 映射管理
  delete local_table_info;
  local_table_info = nullptr; // 避免悬空指针

  // 将目录元数据刷新到磁盘
  dberr_t flush_res = FlushCatalogMetaPage();
  if (flush_res != DB_SUCCESS) {
    LOG(ERROR) << "Failed to flush catalog meta page after dropping table " << table_name;
    // 此时，内存状态已更新，但磁盘上的 catalog_meta_ 可能未更新，导致不一致
    return flush_res;
  }

  return DB_SUCCESS;
  // ASSERT(false, "Not Implemented yet");
}

/**
 * TODO: Student Implement - Done
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  // 检查表是否存在，并从 index_names_ 中查找 index_id
  if (table_names_.count(table_name) == 0) {
    return DB_TABLE_NOT_EXIST;
  }

  index_id_t index_id_to_drop = -1;
  bool index_entry_found_in_names = false;

  auto it_table_map_for_indexes = index_names_.find(table_name);
  if (it_table_map_for_indexes != index_names_.end()) {
    auto &name_to_id_map = it_table_map_for_indexes->second;
    auto it_index_name = name_to_id_map.find(index_name);
    if (it_index_name != name_to_id_map.end()) {
      index_id_to_drop = it_index_name->second;
      index_entry_found_in_names = true;
    }
  }

  if (!index_entry_found_in_names) {
    return DB_INDEX_NOT_FOUND;
  }
  ASSERT(index_id_to_drop != -1, "index_id_to_drop should be valid here.");

  // 获取将要删除的 IndexInfo* (其析构函数将运行)
  IndexInfo *index_info_to_delete = nullptr;
  auto it_indexes_map = indexes_.find(index_id_to_drop);
  if (it_indexes_map != indexes_.end()) {
    index_info_to_delete = it_indexes_map->second;
  } else {
    LOG(ERROR) << "Catalog inconsistency: Index ID " << index_id_to_drop << " for index '" << index_name
               << "' on table '" << table_name << "' not found in indexes_ map, but was in index_names_.";
    if (it_table_map_for_indexes != index_names_.end()) { // 清理 index_names_
        it_table_map_for_indexes->second.erase(index_name);
        if (it_table_map_for_indexes->second.empty()) {
            index_names_.erase(it_table_map_for_indexes);
        }
    }
    catalog_meta_->DeleteIndexMetaPage(buffer_pool_manager_, index_id_to_drop); // 尝试清理 catalog_meta_
    FlushCatalogMetaPage(); // 尝试持久化清理
    return DB_FAILED;
  }

  // 从磁盘删除索引元数据页面并从 catalog_meta_ 中移除记录
  // CatalogMeta::DeleteIndexMetaPage 会处理 BPM->DeletePage 和从其内部映射中擦除
  if (catalog_meta_ == nullptr || !catalog_meta_->DeleteIndexMetaPage(buffer_pool_manager_, index_id_to_drop)) {
    LOG(WARNING) << "Failed to delete index metadata page or entry not found in catalog_meta_ for Index ID "
                 << index_id_to_drop << " (table: " << table_name << ", index: " << index_name << ").";
    // 继续进行内存清理，但如果 FlushCatalogMetaPage 稍后失败，目录可能在磁盘上不一致。
  }

  // 从 CatalogManager 的内存映射中移除索引
  // 从 index_names_ (table_name -> map<index_name, index_id>) 中移除
  if (it_table_map_for_indexes != index_names_.end()) { // 迭代器可能已失效，重新查找以确保安全
      auto current_it_table_map = index_names_.find(table_name);
      if(current_it_table_map != index_names_.end()){
        current_it_table_map->second.erase(index_name);
        if (current_it_table_map->second.empty()) {
          index_names_.erase(current_it_table_map); // 如果此表没有其他索引，则移除表条目
        }
      }
  }
  
  // 从 indexes_ (index_id -> IndexInfo*) 映射中移除指针
  indexes_.erase(index_id_to_drop);

  // 删除 IndexInfo 对象。
  // 其析构函数将删除 IndexMetadata 和 Index 对象 (例如 BPlusTreeIndex)。
  // BPlusTreeIndex 的析构函数必须负责通过 BPM 释放其所有数据页。
  if (index_info_to_delete != nullptr) {
      delete index_info_to_delete;
  }

  // 将 catalog_meta_ 刷新到磁盘
  dberr_t flush_res = FlushCatalogMetaPage();
  if (flush_res != DB_SUCCESS) {
    LOG(ERROR) << "Failed to flush catalog meta page after dropping index " << index_name << " on table " << table_name;
    return flush_res;
  }

  return DB_SUCCESS;
  // ASSERT(false, "Not Implemented yet");
}

/**
 * TODO: Student Implement - Done
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  // 检查 catalog_meta_ 是否已初始化
  if (catalog_meta_ == nullptr) {
    LOG(WARNING) << "FlushCatalogMetaPage called, but catalog_meta_ is null. Catalog might not be initialized.";
    return DB_FAILED; // 或者一个更具体的错误码，如 DB_CATALOG_NOT_INITIALIZED
  }
  // 获取目录元数据页
  Page *meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  // 将目录元数据序列化到页面数据中
  catalog_meta_->SerializeTo(meta_page->GetData());
  // 解除目录元数据页面的锁定，并标记为已修改（脏页）
  // 这样 BufferPoolManager 知道在适当时机需要将其写回磁盘
  bool unpin_success = buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  if (!unpin_success) {
    LOG(ERROR) << "Failed to unpin catalog meta page " << CATALOG_META_PAGE_ID
               << " after marking it dirty. The page might remain pinned, potentially causing issues.";
    // 这是一个比较严重的问题，因为页面是脏的，但可能没有被正确解除锁定。
    // BufferPoolManager 最终仍可能将其写回（例如，如果被替换出去），但这并非理想状态。
    return DB_FAILED; // 或者特定的 Unpin 错误码
  }
  return DB_SUCCESS;
  // ASSERT(false, "Not Implemented yet");
}

/**
 * TODO: Student Implement - Done
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  // 检查表是否已在内存中的 tables_ 映射中存在
  if (tables_.count(table_id)) {
    // 表已经被加载，可能是重复调用或逻辑错误
    LOG(WARNING) << "Table with id " << table_id << " already loaded.";
    return DB_TABLE_ALREADY_EXIST; // 或者根据具体策略，也可以返回 DB_SUCCESS
  }

  // 获取包含表元数据的页面
  Page *meta_disk_page = buffer_pool_manager_->FetchPage(page_id);

  // 从页面数据反序列化表元数据
  TableMetadata *table_meta = nullptr;
  // TableMetadata::DeserializeFrom 会在内部 new TableMetadata
  uint32_t bytes_read = TableMetadata::DeserializeFrom(meta_disk_page->GetData(), table_meta);
  
  // 反序列化后立即解除页面锁定，标记为非脏页（因为我们只是读取）
  buffer_pool_manager_->UnpinPage(page_id, false /*is_dirty*/);

  if (table_meta == nullptr || bytes_read == 0) { // 假设 bytes_read == 0 也表示失败
    LOG(ERROR) << "Failed to deserialize TableMetadata from page " << page_id << " for table_id " << table_id;
    if (table_meta) delete table_meta; // 如果 DeserializeFrom 部分成功但仍需清理
    return DB_FAILED; // 假设有此错误码
  }
  // 验证反序列化得到的 table_id 是否与传入的 table_id 一致
  if (table_meta->GetTableId() != table_id) {
      LOG(ERROR) << "Table ID mismatch after deserializing metadata for table_id " << table_id
                 << ". Expected " << table_id << ", got " << table_meta->GetTableId() << ".";
      delete table_meta;
      return DB_FAILED; // 或目录损坏错误
  }


  // 直接使用 table_meta 中的 schema
  TableSchema *table_schema = table_meta->GetSchema();
  if (table_schema == nullptr) {
    LOG(ERROR) << "Deserialized TableMetadata for table_id " << table_id << " has null schema.";
    delete table_meta;
    return DB_FAILED;
  }

  // 创建表堆实例
  // TableHeap::Create 需要 buffer_pool_manager, 表的根页面ID, schema, 以及可选的 log/lock 管理器
  page_id_t table_heap_root_page_id = table_meta->GetFirstPageId(); // GetFirstPageId() 即 root_page_id_
  TableHeap *table_heap = nullptr;
  try {
    table_heap = TableHeap::Create(buffer_pool_manager_, table_heap_root_page_id, table_schema, log_manager_, lock_manager_);
  } catch (const std::bad_alloc &e) {
    LOG(ERROR) << "Failed to allocate TableHeap for table_id " << table_id << ": " << e.what();
    delete table_meta; // table_meta 尚未被 TableInfo 接管
    return DB_FAILED; // 内存分配失败
  }
  if (table_heap == nullptr) { // 如果 Create 返回 nullptr 而不是抛异常
      LOG(ERROR) << "TableHeap::Create returned null for table_id " << table_id;
      delete table_meta;
      return DB_FAILED;
  }


  // 创建 TableInfo 实例并初始化
  TableInfo *new_table_info = nullptr;
  try {
    new_table_info = TableInfo::Create(); // 通常是 new TableInfo()
    if (!new_table_info) throw std::bad_alloc();
    // Init 方法会使 TableInfo 接管 table_meta 和 table_heap 的所有权
    new_table_info->Init(table_meta, table_heap);
  } catch (const std::bad_alloc &e) {
    LOG(ERROR) << "Failed to allocate TableInfo for table_id " << table_id << ": " << e.what();
    delete table_heap; // table_heap 已创建但未被 TableInfo 接管
    delete table_meta; // table_meta 也未被 TableInfo 接管
    return DB_FAILED;
  }


  // 将表名和表ID添加到 table_names_ 映射
  table_names_.emplace(table_meta->GetTableName(), table_id);

  // 将表ID和 TableInfo 添加到 tables_ 映射
  tables_.emplace(table_id, new_table_info);

  // 返回成功
  return DB_SUCCESS;
  // ASSERT(false, "Not Implemented yet");
}

/**
 * TODO: Student Implement - Done
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  // 检查索引是否已在内存中的 indexes_ 映射中存在
  if (indexes_.count(index_id)) {
    LOG(WARNING) << "Index with id " << index_id << " already loaded.";
    return DB_INDEX_ALREADY_EXIST; // 或根据策略返回 DB_SUCCESS
  }

  // 获取包含索引元数据的页面
  Page *meta_disk_page = buffer_pool_manager_->FetchPage(page_id);

  // 从页面数据反序列化索引元数据
  IndexMetadata *index_meta = nullptr;
  uint32_t bytes_read = IndexMetadata::DeserializeFrom(meta_disk_page->GetData(), index_meta);
  
  buffer_pool_manager_->UnpinPage(page_id, false /*is_dirty*/);

  if (index_meta == nullptr || bytes_read == 0) {
    LOG(ERROR) << "Failed to deserialize IndexMetadata from page " << page_id << " for index_id " << index_id;
    if (index_meta) delete index_meta;
    return DB_FAILED;
  }
  // 验证反序列化得到的 index_id 是否与传入的 index_id 一致
  if (index_meta->GetIndexId() != index_id) {
      LOG(ERROR) << "Index ID mismatch after deserializing metadata for index_id " << index_id
                 << ". Expected " << index_id << ", got " << index_meta->GetIndexId() << ".";
      delete index_meta;
      return DB_FAILED; // 或目录损坏错误
  }

  // 获取该索引所属的表的 TableInfo
  table_id_t table_id_for_index = index_meta->GetTableId();
  TableInfo *table_info_for_index = nullptr;
  if (GetTable(table_id_for_index, table_info_for_index) != DB_SUCCESS || table_info_for_index == nullptr) {
    LOG(ERROR) << "Failed to get TableInfo for table_id " << table_id_for_index 
               << " (referenced by index_id " << index_id << "). Table might not be loaded or catalog is inconsistent.";
    delete index_meta;
    return DB_TABLE_NOT_EXIST; // 或 DB_FAILED
  }

  // 创建 IndexInfo 实例并初始化
  IndexInfo *new_index_info = nullptr;
  try {
    new_index_info = IndexInfo::Create();
    if (!new_index_info) throw std::bad_alloc();
    // IndexInfo::Init 会接管 index_meta 的所有权
    // 并使用 TableInfo 来构建 key_schema，以及 buffer_pool_manager_ 来创建实际的索引结构
    // 注意：IndexInfo::Init 内部会硬编码 "bptree" 作为索引类型
    new_index_info->Init(index_meta, table_info_for_index, buffer_pool_manager_);
  } catch (const std::bad_alloc &e) {
    LOG(ERROR) << "Failed to allocate IndexInfo for index_id " << index_id << ": " << e.what();
    delete index_meta; // index_meta 未被 IndexInfo 接管
    return DB_FAILED;
  } catch (...) { // IndexInfo::Init 可能因其他原因失败 (例如，key_schema 创建失败)
    LOG(ERROR) << "IndexInfo::Init failed for index_id " << index_id;
    if (new_index_info) delete new_index_info; // 如果 new_index_info 已分配，其析构函数会处理 index_meta (如果已赋值)
    else delete index_meta; // 否则，手动删除 index_meta
    return DB_FAILED;
  }


  // 将索引名和索引ID添加到 index_names_ 映射
  // index_names_ 是 table_name -> map<index_name, index_id>
  std::string table_name_for_index = table_info_for_index->GetTableName();
  index_names_[table_name_for_index].emplace(index_meta->GetIndexName(), index_id);

  // 将索引ID和 IndexInfo 添加到 indexes_ 映射
  indexes_.emplace(index_id, new_index_info);

  return DB_SUCCESS;
  // ASSERT(false, "Not Implemented yet");
}

/**
 * TODO: Student Implement - Done
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  if (auto it = tables_.find(table_id); it != tables_.end()) {
    table_info = it->second; // 找到表，设置输出参数
    return DB_SUCCESS;
  }
  // 未找到具有给定 table_id 的表
  table_info = nullptr; // 确保输出参数在失败时为 nullptr
  return DB_TABLE_NOT_EXIST;
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}
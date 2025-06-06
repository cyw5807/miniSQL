#ifndef MINISQL_INDEXES_H
#define MINISQL_INDEXES_H

#include <memory>

#include "catalog/table.h"
#include "common/macros.h"
#include "common/rowid.h"
#include "index/b_plus_tree_index.h"
#include "index/generic_key.h"
#include "record/schema.h"

class IndexMetadata {
  friend class IndexInfo;

 public:
  static IndexMetadata *Create(const index_id_t index_id, const std::string &index_name, const table_id_t table_id,
                               const std::vector<uint32_t> &key_map);

  uint32_t SerializeTo(char *buf) const;

  uint32_t GetSerializedSize() const;

  static uint32_t DeserializeFrom(char *buf, IndexMetadata *&index_meta);

  inline std::string GetIndexName() const { return index_name_; }

  inline table_id_t GetTableId() const { return table_id_; }

  uint32_t GetIndexColumnCount() const { return key_map_.size(); }

  inline const std::vector<uint32_t> &GetKeyMapping() const { return key_map_; }

  inline index_id_t GetIndexId() const { return index_id_; }

 private:
  IndexMetadata() = delete;

  explicit IndexMetadata(const index_id_t index_id, const std::string &index_name, const table_id_t table_id,
                         const std::vector<uint32_t> &key_map);

 private:
  static constexpr uint32_t INDEX_METADATA_MAGIC_NUM = 344528;
  index_id_t index_id_;
  std::string index_name_;
  table_id_t table_id_;
  std::vector<uint32_t> key_map_; /** The mapping of index key to tuple key */
};

/**
 * The IndexInfo class maintains metadata about a index.
 */
class IndexInfo {
 public:
  static IndexInfo *Create() { return new IndexInfo(); }

  ~IndexInfo() {
    delete meta_data_;
    delete index_;
    delete key_schema_;
  }

/**
 * TODO: Student Implement - Done
 */
  void Init(IndexMetadata *meta_data, TableInfo *table_info, BufferPoolManager *buffer_pool_manager) {
    // init index metadata and table info
    // IndexInfo takes ownership of the meta_data passed in.
    this->meta_data_ = meta_data;
    if (this->meta_data_ == nullptr || table_info == nullptr || table_info->GetSchema() == nullptr ||
        buffer_pool_manager == nullptr) {
      this->key_schema_ = nullptr;
      this->index_ = nullptr;
      return;
    }

    // mapping index key to key schema
    this->key_schema_ = Schema::ShallowCopySchema(table_info->GetSchema(), meta_data_->GetKeyMapping());

    // call CreateIndex to create the index
    if (this->meta_data_ != nullptr && this->key_schema_ != nullptr && buffer_pool_manager != nullptr) {
        std::string index_type_str = "bptree";
        this->index_ = CreateIndex(buffer_pool_manager, index_type_str);
    } else {
        this->index_ = nullptr;
    }
  }

  inline Index *GetIndex() { return index_; }

  std::string GetIndexName() { return meta_data_->GetIndexName(); }

  IndexSchema *GetIndexKeySchema() { return key_schema_; }

 private:
  explicit IndexInfo() : meta_data_{nullptr}, index_{nullptr}, key_schema_{nullptr} {}

  Index *CreateIndex(BufferPoolManager *buffer_pool_manager, const string &index_type);

 private:
  IndexMetadata *meta_data_;
  Index *index_;
  IndexSchema *key_schema_;
};

#endif  // MINISQL_INDEXES_H

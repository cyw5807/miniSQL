#include "record/column.h"

#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

/**
* TODO: Student Implement
*/
uint32_t Column::SerializeTo(char *buf) const {
  char *p = buf;

   MACH_WRITE_UINT32(p, COLUMN_MAGIC_NUM);
   p += sizeof(uint32_t);

   uint32_t name_len = name_.length();
   MACH_WRITE_UINT32(p,name_len);
   p += sizeof(uint32_t);
   MACH_WRITE_STRING(p,name_);
   p += name_len;

   MACH_WRITE_TO(TypeId,p,type_);
   p += sizeof(TypeId);


  MACH_WRITE_UINT32(p, len_);
  p += sizeof(uint32_t);


  MACH_WRITE_UINT32(p, table_ind_);
  p += sizeof(uint32_t);

  MACH_WRITE_TO(bool, p, nullable_);
  p += sizeof(bool);

  
  MACH_WRITE_TO(bool, p, unique_);
  p += sizeof(bool);

  return p - buf; 
}

/**
 * TODO: Student Implement
 */
uint32_t Column::GetSerializedSize() const {
  // 计算所有成员变量序列化后的大小总和
  uint32_t size = 0;
  // 1. 魔数 (uint32_t)
  size += sizeof(uint32_t);
  // 2. 列名 (4字节长度 + 字符串内容)
  size += MACH_STR_SERIALIZED_SIZE(name_);
  // 3. 数据类型 (TypeId, 通常是 uint32_t 或 int)
  size += sizeof(TypeId);
  // 4. 长度 (uint32_t)
  size += sizeof(uint32_t);
  // 5. 列索引 (uint32_t)
  size += sizeof(uint32_t);
  // 6. 是否可为空 (bool)
  size += sizeof(bool);
  // 7. 是否唯一 (bool)
  size += sizeof(bool);
  return size;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
   // 检查输出参数是否已经被占用
  if (column != nullptr) {
    LOG(WARNING) << "Pointer to column is not null in column deserialize." << std::endl;
  }

  char *p = buf;

  uint32_t magic_num = MACH_READ_UINT32(p);
  
  ASSERT(magic_num == COLUMN_MAGIC_NUM, "Column magic number mismatch.");

  p += sizeof(uint32_t);

  uint32_t name_len = MACH_READ_UINT32(p);
  p += sizeof(uint32_t);

  std::string column_name(p, name_len);
  p += name_len;

  TypeId type = MACH_READ_FROM(TypeId, p);
  p += sizeof(TypeId);

  uint32_t col_len = MACH_READ_UINT32(p);
  p += sizeof(uint32_t);

  uint32_t col_ind = MACH_READ_UINT32(p);
  p += sizeof(uint32_t);

  bool nullable = MACH_READ_FROM(bool, p);
  p += sizeof(bool);

  bool unique = MACH_READ_FROM(bool, p);
  p += sizeof(bool);

  if (type == kTypeChar) {
    column = new Column(column_name, type, col_len, col_ind, nullable, unique);
  } else {
    column = new Column(column_name, type, col_ind, nullable, unique);
  }

  return p - buf;
}

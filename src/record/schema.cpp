#include "record/schema.h"

/**
 * TODO: Student Implement
 */
uint32_t Schema::SerializeTo(char *buf) const {
  char *p = buf; // 使用指针 p 追踪写入位置
  // 1. 写入魔数
  MACH_WRITE_UINT32(p, SCHEMA_MAGIC_NUM);
  p += sizeof(uint32_t);
  // 2. 写入列的数量
  uint32_t column_count = GetColumnCount();
  MACH_WRITE_UINT32(p, column_count);
  p += sizeof(uint32_t);

  // 3. 依次序列化每个 Column 对象
  for (const auto &column : columns_) {
    p += column->SerializeTo(p); // 调用 Column 的序列化并移动指针
  }

  return p - buf;
}

uint32_t Schema::GetSerializedSize() const {
  uint32_t size = 0;

  size += sizeof(uint32_t);

  size += sizeof(uint32_t);

  for (const auto &column : columns_) {
    size += column->GetSerializedSize();
  }

  return size;

}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
  if (schema != nullptr) {
    //LOG(WARNING) << "Pointer to schema is not null in schema deserialize." << std::endl;
  }
  char *p = buf; 

  uint32_t magic_num = MACH_READ_UINT32(p);
  ASSERT(magic_num == SCHEMA_MAGIC_NUM, "Schema magic number mismatch.");
  p += sizeof(uint32_t);

  uint32_t column_count = MACH_READ_UINT32(p);
  p += sizeof(uint32_t);

  // 3. 依次反序列化每个 Column 对象
  std::vector<Column *> columns;
  columns.reserve(column_count); // 预分配空间
  for (uint32_t i = 0; i < column_count; ++i) {
    Column *col = nullptr; 
    p += Column::DeserializeFrom(p, col);
    columns.push_back(col); // 将新创建的 Column 添加到 vector
  }

  schema = new Schema(columns, true);
  return p - buf;
}
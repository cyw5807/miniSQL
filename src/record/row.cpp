#include "record/row.h"

/**
 * TODO: Student Implement
 */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  const uint32_t field_count = fields_.size();

  // 空行不序列化
  if (field_count == 0) {
    return 0;
  }

  ASSERT(schema->GetColumnCount() == field_count, "Field count mismatch with schema.");

  char *p = buf; // 使用指针 p 追踪写入位置

  // 1. 写入 Header: 字段数量
  MACH_WRITE_UINT32(p, field_count);
  p += sizeof(uint32_t);

  // 2. 写入 Header: Null Bitmap
  const uint32_t null_bitmap_size = (field_count + 7) / 8;
  char *bitmap_ptr = p; // 获取 Null Bitmap 的起始地址
  memset(bitmap_ptr, 0, null_bitmap_size); // 初始化 Bitmap 为 0
  p += null_bitmap_size; // 移动指针到字段数据区

  // 填充 Null Bitmap 并计算哪些字段需要序列化
  for (uint32_t i = 0; i < field_count; ++i) {
    if (fields_[i]->IsNull()) {
      // 如果字段为 NULL，在 Bitmap 中设置对应的位为 1
      bitmap_ptr[i / 8] |= (1 << (i % 8));
    }
  }

  // 3. 写入 Field-1 到 Field-N (只写非空字段)
  for (uint32_t i = 0; i < field_count; ++i) {
    if (!fields_[i]->IsNull()) {
      // 调用 Field 的序列化方法，并移动指针
      p += fields_[i]->SerializeTo(p);
    }
  }

  // 返回总共写入的字节数
  return p - buf;

}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(fields_.empty(), "Non empty field in row.");
  char *p =buf;

  const uint32_t fields_num = MACH_READ_UINT32(p);
  p += sizeof(uint32_t);

  if(fields_num ==0){
    return sizeof(uint32_t);
  }

  const uint32_t null_size = (fields_num + 7)/8;
  const char *null_bitmap = p;
  p += null_size;

  fields_.reserve(fields_num);

   for (uint32_t i = 0; i < fields_num; ++i) {

    const Column *col_schema = schema->GetColumn(i);
    TypeId type = col_schema->GetType();

    // 准备一个空指针，用于接收 Field::DeserializeFrom 创建的对象
    Field *field = nullptr;

    bool is_null = (null_bitmap[i / 8] & (1 << (i % 8))) != 0;

    uint32_t bytes_read = Field::DeserializeFrom(p, type, &field, is_null);

    // 移动指针 p (如果 is_null=true，bytes_read 为 0，p 不动；否则 p 前进)
    p += bytes_read;

    ASSERT(field != nullptr, "Field::DeserializeFrom must create a field object.");
    fields_.push_back(field);
  }
return p - buf;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  
  const uint32_t cnt = fields_.size();
  ASSERT(schema->GetColumnCount() == cnt, "Fields size do not match schema's column size.");

  if (cnt == 0) {
    return 0;
  }
  uint32_t size = 0;
  size += sizeof(uint32_t);

  size += (cnt + 7) / 8;

  for (uint32_t i = 0; i < cnt; ++i) {
    if (fields_[i] != nullptr && !fields_[i]->IsNull()) {
      size += fields_[i]->GetSerializedSize();
    }
  }

  return size;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column : columns) {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
}

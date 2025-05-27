#ifndef MINISQL_TABLE_ITERATOR_H
#define MINISQL_TABLE_ITERATOR_H

#include "common/rowid.h"
#include "concurrency/txn.h"
#include "record/row.h"

class TableHeap;

class TableIterator {
public:
 // you may define your own constructor based on your member variables
 explicit TableIterator(TableHeap *table_heap, RowId rid, Txn *txn);

 TableIterator(const TableIterator &other);

  virtual ~TableIterator();

  bool operator==(const TableIterator &itr) const;

  bool operator!=(const TableIterator &itr) const;

  const Row &operator*();

  Row *operator->();

  TableIterator &operator=(const TableIterator &itr) noexcept;

  TableIterator &operator++();

  TableIterator operator++(int);

private:
  // 添加的成员变量
  TableHeap *table_heap_{nullptr};
  RowId rid_{INVALID_PAGE_ID, 0}; // 默认为无效 RowId
  Txn *txn_{nullptr};
  Row row_; // 存储当前迭代器指向的 Row 对象// add your own private member variables here
};

#endif  // MINISQL_TABLE_ITERATOR_H

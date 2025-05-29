#include "index/b_plus_tree.h"

#include "common/instance.h"
#include "gtest/gtest.h"
#include "index/comparator.h"
#include "utils/tree_file_mgr.h"
#include "utils/utils.h"

static const std::string db_name = "bp_tree_insert_test.db";

TEST(BPlusTreeTests, SampleTest) {
  // Init engine
  DBStorageEngine engine(db_name);
  std::vector<Column *> columns = {
      new Column("int", TypeId::kTypeInt, 0, false, false),
  };
  Schema *table_schema = new Schema(columns);
  KeyManager KP(table_schema, 17);
  BPlusTree tree(0, engine.bpm_, KP);
  TreeFileManagers mgr("tree_");
  // Prepare data
  const int n = 20000; // testing
  vector<GenericKey *> keys;
  vector<RowId> values;
  vector<GenericKey *> delete_seq;
  map<GenericKey *, RowId> kv_map;
  for (int i = 0; i < n; i++) {
    GenericKey *key = KP.InitKey();
    std::vector<Field> fields{Field(TypeId::kTypeInt, i)};
    KP.SerializeFromKey(key, Row(fields), table_schema);
    keys.push_back(key);
    values.push_back(RowId(i));
    delete_seq.push_back(key);
  }
  vector<GenericKey *> keys_copy(keys);
  // Shuffle data
  ShuffleArray(keys);
  ShuffleArray(values);
  ShuffleArray(delete_seq);
  // Map key value
  for (int i = 0; i < n; i++) {
    kv_map[keys[i]] = values[i];
  }
  // Insert data
  std::cout << "B+ Tree Test-ins" << std::endl << "----------------------------" << std::endl;
  for (int i = 0; i < n; i++) {
    tree.Insert(keys[i], values[i]);
  }
  ASSERT_TRUE(tree.Check());
  // Print tree
  tree.PrintTree(mgr[0], table_schema);
  // Search keys
  vector<RowId> ans;
  std::cout << "B+ Tree Test-get" << std::endl << "----------------------------" << std::endl;
  for (int i = 0; i < n; i++) {
    tree.GetValue(keys_copy[i], ans);
    ASSERT_EQ(kv_map[keys_copy[i]], ans[i]);
  }

  ASSERT_TRUE(tree.Check());
  // Delete half keys
  std::cout << "B+ Tree Test-del" << std::endl << "----------------------------" << std::endl;
  for (int i = 0; i < n / 2; i++) {
    tree.Remove(delete_seq[i]);
  }
  tree.PrintTree(mgr[1], table_schema);
  // Check valid
  ans.clear();
  std::cout << "B+ Tree Test-del-fir" << std::endl << "----------------------------" << std::endl;
  for (int i = 0; i < n / 2; i++) {
    ASSERT_FALSE(tree.GetValue(delete_seq[i], ans));
  }
  std::cout << "B+ Tree Test-del-sec" << std::endl << "----------------------------" << std::endl;
  for (int i = n / 2; i < n; i++) {
    ASSERT_TRUE(tree.GetValue(delete_seq[i], ans));
    ASSERT_EQ(kv_map[delete_seq[i]], ans[ans.size() - 1]);
  }
}

TEST(BPlusTreeTests, MySampleTest) {
  // Init engine
  DBStorageEngine engine(db_name);
  std::vector<Column *> columns = {
      new Column("int", TypeId::kTypeInt, 0, false, false),
  };
  Schema *table_schema = new Schema(columns);
  KeyManager KP(table_schema, 17);
  BPlusTree tree(0, engine.bpm_, KP);
  TreeFileManagers mgr("tree_");
  // Prepare data
  const int n = 20000; // testing
  vector<GenericKey *> keys;
  vector<RowId> values;
  vector<GenericKey *> delete_seq;
  vector<int> index;
  map<GenericKey *, RowId> kv_map;
  for (int i = 0; i < n; i++) {
    GenericKey *key = KP.InitKey();
    std::vector<Field> fields{Field(TypeId::kTypeInt, i)};
    KP.SerializeFromKey(key, Row(fields), table_schema);
    keys.push_back(key);
    values.push_back(RowId(i));
    delete_seq.push_back(key);
    index.push_back(i);
  }
  // Shuffle data
  ShuffleArray(keys);
  ShuffleArray(values);
  ShuffleArray(delete_seq);
  ShuffleArray(index);
  vector<GenericKey *> keys_copy(keys);
  
  // Map key value
  for (int i = 0; i < n; i++) {
    kv_map[keys[i]] = values[i];
  }
  // Insert data
  std::cout << "B+ Tree Test-ins1" << std::endl << "----------------------------" << std::endl;
  for (int i = 0; i < n / 2; i++) {
    tree.Insert(keys[i], values[i]);
  }
  ASSERT_TRUE(tree.Check());
  vector<RowId> ans;
  std::cout << "B+ Tree Test-get1" << std::endl << "----------------------------" << std::endl;
  for (int i = 0; i < n / 2; i++) {
    ASSERT_TRUE(tree.GetValue(keys_copy[i], ans));
    ASSERT_EQ(kv_map[keys_copy[i]], ans[i]);
  }
  for(int i = n / 2; i < n; i++){
    ASSERT_FALSE(tree.GetValue(keys_copy[i], ans));
  }
  std::cout << "B+ Tree Test-ins2" << std::endl << "----------------------------" << std::endl;
  for (int i = n / 2; i < n; i++) {
    tree.Insert(keys[i], values[i]);
  }
  ASSERT_TRUE(tree.Check());
  // Print tree
  tree.PrintTree(mgr[0], table_schema);
  // Search keys
  ans.clear();
  std::cout << "B+ Tree Test-get2" << std::endl << "----------------------------" << std::endl;
  for (int i = 0; i < n; i++) {
    ASSERT_TRUE(tree.GetValue(keys_copy[index[i]], ans));
    ASSERT_EQ(kv_map[keys_copy[index[i]]], ans[i]);
  }

  ASSERT_TRUE(tree.Check());
  // Delete half keys
  std::cout << "B+ Tree Test-del" << std::endl << "----------------------------" << std::endl;
  for (int i = 0; i < n / 2; i++) {
    tree.Remove(delete_seq[i]);
  }
  tree.PrintTree(mgr[1], table_schema);
  // Check valid
  ans.clear();
  std::cout << "B+ Tree Test-del-fir" << std::endl << "----------------------------" << std::endl;
  for (int i = 0; i < n / 2; i++) {
    ASSERT_FALSE(tree.GetValue(delete_seq[i], ans));
  }
  std::cout << "B+ Tree Test-del-sec" << std::endl << "----------------------------" << std::endl;
  for (int i = n / 2; i < n; i++) {
    ASSERT_TRUE(tree.GetValue(delete_seq[i], ans));
    ASSERT_EQ(kv_map[delete_seq[i]], ans[ans.size() - 1]);
  }
}

TEST(BPlusTreeTests, CreateSampleTest) {
  // Init engine
  DBStorageEngine engine(db_name);
  std::vector<Column *> columns = {
      new Column("int", TypeId::kTypeInt, 0, false, false),
  };
  Schema *table_schema = new Schema(columns);
  KeyManager KP(table_schema, 17);
  BPlusTree tree(0, engine.bpm_, KP);
  TreeFileManagers mgr("tree_");
  // Prepare data
  const int n = 100; // testing
  vector<GenericKey *> keys;
  vector<RowId> values;
  map<GenericKey *, RowId> kv_map;
  for (int i = 0; i < n; i++) {
    GenericKey *key = KP.InitKey();
    std::vector<Field> fields{Field(TypeId::kTypeInt, i)};
    KP.SerializeFromKey(key, Row(fields), table_schema);
    keys.push_back(key);
    values.push_back(RowId(i));
  }
  // Shuffle data
  ShuffleArray(keys);
  ShuffleArray(values);
  
  // Map key value
  for (int i = 0; i < n; i++) {
    kv_map[keys[i]] = values[i];
  }
  // Insert data
  std::cout << "B+ Tree Test-create" << std::endl << "----------------------------" << std::endl;
  vector<RowId> ans;
  for (int i = 0; i < n; i++) {
    ASSERT_FALSE(tree.GetValue(keys[i], ans));
    ASSERT_TRUE(tree.Insert(keys[i], values[i]));
    ASSERT_TRUE(tree.GetValue(keys[i], ans));
    ASSERT_EQ(kv_map[keys[i]], ans[i]);
    tree.Remove(keys[i]);
    ASSERT_FALSE(tree.GetValue(keys[i], ans));
  }
}

TEST(BPlusTreeTests, GeneralSampleTest) {
  // Init engine
  DBStorageEngine engine(db_name);
  std::vector<Column *> columns = {
      new Column("int", TypeId::kTypeInt, 0, false, false),
  };
  Schema *table_schema = new Schema(columns);
  KeyManager KP(table_schema, 17);
  BPlusTree tree(0, engine.bpm_, KP);
  TreeFileManagers mgr("tree_");
  // Prepare data
  const int n = 20000; // testing
  vector<GenericKey *> keys;
  vector<RowId> values;
  map<GenericKey *, RowId> kv_map;
  for (int i = 0; i < n; i++) {
    GenericKey *key = KP.InitKey();
    std::vector<Field> fields{Field(TypeId::kTypeInt, i)};
    KP.SerializeFromKey(key, Row(fields), table_schema);
    keys.push_back(key);
    values.push_back(RowId(i));
  }
  // Shuffle data
  ShuffleArray(keys);
  ShuffleArray(values);
  
  // Map key value
  for (int i = 0; i < n; i++) {
    kv_map[keys[i]] = values[i];
  }
  // Insert data
  std::cout << "B+ Tree Test-general" << std::endl << "----------------------------" << std::endl;
  vector<RowId> ans;
  std::cout << "B+ Tree Test-1" << std::endl << "----------------------------" << std::endl;
  for (int i = 0; i < n / 2; i++) {
    ASSERT_FALSE(tree.GetValue(keys[2 * i], ans));
    ASSERT_TRUE(tree.Insert(keys[2 * i], values[2 * i]));
    ASSERT_FALSE(tree.Insert(keys[2 * i], values[2 * i]));
    ASSERT_FALSE(tree.GetValue(keys[2 * i + 1], ans));
    ASSERT_TRUE(tree.Insert(keys[2 * i + 1], values[2 * i + 1]));
    ASSERT_FALSE(tree.Insert(keys[2 * i + 1], values[2 * i + 1]));
    ASSERT_TRUE(tree.GetValue(keys[2 * i], ans));
    ASSERT_EQ(kv_map[keys[2 * i]], ans[2 * i]);
    tree.Remove(keys[2 * i]);
    ASSERT_FALSE(tree.GetValue(keys[2 * i], ans));
    ASSERT_TRUE(tree.GetValue(keys[2 * i + 1], ans));
    ASSERT_EQ(kv_map[keys[2 * i + 1]], ans[2 * i + 1]);
  }
  ASSERT_TRUE(tree.Check());
  ans.clear();
  std::cout << "B+ Tree Test-2" << std::endl << "----------------------------" << std::endl;
  for (int i = 0; i < n / 2; i++) {
    ASSERT_FALSE(tree.GetValue(keys[2 * i], ans));
    ASSERT_TRUE(tree.GetValue(keys[2 * i + 1], ans));
    ASSERT_EQ(kv_map[keys[2 * i + 1]], ans[i]);
    tree.Remove(keys[2 * i + 1]);
    ASSERT_FALSE(tree.GetValue(keys[2 * i + 1], ans));
  }
}

TEST(BPlusTreeTests, GeneralSampleTest2) {
  // Init engine
  DBStorageEngine engine(db_name);
  std::vector<Column *> columns = {
      new Column("int", TypeId::kTypeInt, 0, false, false),
  };
  Schema *table_schema = new Schema(columns);
  KeyManager KP(table_schema, 17);
  BPlusTree tree(0, engine.bpm_, KP);
  TreeFileManagers mgr("tree_");
  // Prepare data
  const int n = 20000; // testing
  vector<GenericKey *> keys;
  vector<RowId> values;
  map<GenericKey *, RowId> kv_map;
  for (int i = 0; i < n; i++) {
    GenericKey *key = KP.InitKey();
    std::vector<Field> fields{Field(TypeId::kTypeInt, i)};
    KP.SerializeFromKey(key, Row(fields), table_schema);
    keys.push_back(key);
    values.push_back(RowId(i));
  }
  // Shuffle data
  ShuffleArray(keys);
  ShuffleArray(values);
  
  // Map key value
  for (int i = 0; i < n; i++) {
    kv_map[keys[i]] = values[i];
  }
  // Insert data
  std::cout << "B+ Tree Test-general2" << std::endl << "----------------------------" << std::endl;
  vector<RowId> ans;
  std::cout << "B+ Tree Test-1" << std::endl << "----------------------------" << std::endl;
  for (int i = 0; i < n / 2; i++) {
    ASSERT_FALSE(tree.GetValue(keys[2 * i], ans));
    ASSERT_TRUE(tree.Insert(keys[2 * i], values[2 * i]));
    ASSERT_FALSE(tree.Insert(keys[2 * i], values[2 * i]));
    ASSERT_FALSE(tree.GetValue(keys[2 * i + 1], ans));
    ASSERT_TRUE(tree.Insert(keys[2 * i + 1], values[2 * i + 1]));
    ASSERT_FALSE(tree.Insert(keys[2 * i + 1], values[2 * i + 1]));
    ASSERT_TRUE(tree.GetValue(keys[i], ans));
    ASSERT_EQ(kv_map[keys[i]], ans[i]);
    tree.Remove(keys[i]);
    ASSERT_FALSE(tree.GetValue(keys[i], ans));
  }
  ASSERT_TRUE(tree.Check());
  ans.clear();
  std::cout << "B+ Tree Test-2" << std::endl << "----------------------------" << std::endl;
  for (int i = 0; i < n / 2; i++) {
    int offset = n / 2;
    ASSERT_FALSE(tree.GetValue(keys[i], ans));
    ASSERT_TRUE(tree.GetValue(keys[i + offset], ans));
    ASSERT_EQ(kv_map[keys[i + offset]], ans[i]);
    tree.Remove(keys[i + offset]);
    ASSERT_FALSE(tree.GetValue(keys[i + offset], ans));
  }
}
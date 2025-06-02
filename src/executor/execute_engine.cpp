#include "executor/execute_engine.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <chrono>

#include "common/result_writer.h"
#include "executor/executors/delete_executor.h"
#include "executor/executors/index_scan_executor.h"
#include "executor/executors/insert_executor.h"
#include "executor/executors/seq_scan_executor.h"
#include "executor/executors/update_executor.h"
#include "executor/executors/values_executor.h"
#include "glog/logging.h"
#include "planner/planner.h"
#include "utils/utils.h"
extern "C" {
int yyparse(void);
//FILE *yyin;
#include "parser/minisql_lex.h"
#include "parser/parser.h"
}

ExecuteEngine::ExecuteEngine() {
  char path[] = "./databases";
  DIR *dir;
  if ((dir = opendir(path)) == nullptr) {
    mkdir("./databases", 0777);
    dir = opendir(path);
  }
  /** When you have completed all the code for
   *  the test, run it using main.cpp and uncomment
   *  this part of the code.
  struct dirent *stdir;
  while((stdir = readdir(dir)) != nullptr) {
    if( strcmp( stdir->d_name , "." ) == 0 ||
        strcmp( stdir->d_name , "..") == 0 ||
        stdir->d_name[0] == '.')
      continue;
    dbs_[stdir->d_name] = new DBStorageEngine(stdir->d_name, false);
  }
   **/
  // struct dirent *stdir;
  // while((stdir = readdir(dir)) != nullptr) {
  //   if( strcmp( stdir->d_name , "." ) == 0 ||
  //       strcmp( stdir->d_name , "..") == 0 ||
  //       stdir->d_name[0] == '.')
  //     continue;
  //   dbs_[stdir->d_name] = new DBStorageEngine(stdir->d_name, false);
  // }
  closedir(dir);
}

std::unique_ptr<AbstractExecutor> ExecuteEngine::CreateExecutor(ExecuteContext *exec_ctx,
                                                                const AbstractPlanNodeRef &plan) {
  switch (plan->GetType()) {
    // Create a new sequential scan executor
    case PlanType::SeqScan: {
      return std::make_unique<SeqScanExecutor>(exec_ctx, dynamic_cast<const SeqScanPlanNode *>(plan.get()));
    }
    // Create a new index scan executor
    case PlanType::IndexScan: {
      return std::make_unique<IndexScanExecutor>(exec_ctx, dynamic_cast<const IndexScanPlanNode *>(plan.get()));
    }
    // Create a new update executor
    case PlanType::Update: {
      auto update_plan = dynamic_cast<const UpdatePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, update_plan->GetChildPlan());
      return std::make_unique<UpdateExecutor>(exec_ctx, update_plan, std::move(child_executor));
    }
      // Create a new delete executor
    case PlanType::Delete: {
      auto delete_plan = dynamic_cast<const DeletePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, delete_plan->GetChildPlan());
      return std::make_unique<DeleteExecutor>(exec_ctx, delete_plan, std::move(child_executor));
    }
    case PlanType::Insert: {
      auto insert_plan = dynamic_cast<const InsertPlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, insert_plan->GetChildPlan());
      return std::make_unique<InsertExecutor>(exec_ctx, insert_plan, std::move(child_executor));
    }
    case PlanType::Values: {
      return std::make_unique<ValuesExecutor>(exec_ctx, dynamic_cast<const ValuesPlanNode *>(plan.get()));
    }
    default:
      throw std::logic_error("Unsupported plan type.");
  }
}

dberr_t ExecuteEngine::ExecutePlan(const AbstractPlanNodeRef &plan, std::vector<Row> *result_set, Txn *txn,
                                   ExecuteContext *exec_ctx) {
  // Construct the executor for the abstract plan node
  auto executor = CreateExecutor(exec_ctx, plan);

  try {
    executor->Init();
    RowId rid{};
    Row row{};
    while (executor->Next(&row, &rid)) {
      if (result_set != nullptr) {
        result_set->push_back(row);
      }
    }
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Executor Execution: " << ex.what() << std::endl;
    if (result_set != nullptr) {
      result_set->clear();
    }
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  auto start_time = std::chrono::system_clock::now();
  unique_ptr<ExecuteContext> context(nullptr);
  if (!current_db_.empty()) context = dbs_[current_db_]->MakeExecuteContext(nullptr);
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context.get());
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context.get());
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context.get());
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context.get());
    case kNodeShowTables:
      return ExecuteShowTables(ast, context.get());
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context.get());
    case kNodeDropTable:
      return ExecuteDropTable(ast, context.get());
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context.get());
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context.get());
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context.get());
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context.get());
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context.get());
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context.get());
    case kNodeExecFile:
      return ExecuteExecfile(ast, context.get());
    case kNodeQuit:
      return ExecuteQuit(ast, context.get());
    default:
      break;
  }
  // Plan the query.
  Planner planner(context.get());
  std::vector<Row> result_set{};
  try {
    planner.PlanQuery(ast);
    // Execute the query.
    ExecutePlan(planner.plan_, &result_set, nullptr, context.get());
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Planner: " << ex.what() << std::endl;
    return DB_FAILED;
  }
  auto stop_time = std::chrono::system_clock::now();
  double duration_time =
      double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
  // Return the result set as string.
  std::stringstream ss;
  ResultWriter writer(ss);

  if (planner.plan_->GetType() == PlanType::SeqScan || planner.plan_->GetType() == PlanType::IndexScan) {
    auto schema = planner.plan_->OutputSchema();
    auto num_of_columns = schema->GetColumnCount();
    if (!result_set.empty()) {
      // find the max width for each column
      vector<int> data_width(num_of_columns, 0);
      for (const auto &row : result_set) {
        for (uint32_t i = 0; i < num_of_columns; i++) {
          data_width[i] = max(data_width[i], int(row.GetField(i)->toString().size()));
        }
      }
      int k = 0;
      for (const auto &column : schema->GetColumns()) {
        data_width[k] = max(data_width[k], int(column->GetName().length()));
        k++;
      }
      // Generate header for the result set.
      writer.Divider(data_width);
      k = 0;
      writer.BeginRow();
      for (const auto &column : schema->GetColumns()) {
        writer.WriteHeaderCell(column->GetName(), data_width[k++]);
      }
      writer.EndRow();
      writer.Divider(data_width);

      // Transforming result set into strings.
      for (const auto &row : result_set) {
        writer.BeginRow();
        for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
          writer.WriteCell(row.GetField(i)->toString(), data_width[i]);
        }
        writer.EndRow();
      }
      writer.Divider(data_width);
    }
    writer.EndInformation(result_set.size(), duration_time, true);
  } else {
    writer.EndInformation(result_set.size(), duration_time, false);
  }
  std::cout << writer.stream_.rdbuf();
  // todo:: use shared_ptr for schema
  if (ast->type_ == kNodeSelect)
      delete planner.plan_->OutputSchema();
  return DB_SUCCESS;
}

void ExecuteEngine::ExecuteInformation(dberr_t result) {
  switch (result) {
    case DB_ALREADY_EXIST:
      cout << "Database already exists." << endl;
      break;
    case DB_NOT_EXIST:
      cout << "Database not exists." << endl;
      break;
    case DB_TABLE_ALREADY_EXIST:
      cout << "Table already exists." << endl;
      break;
    case DB_TABLE_NOT_EXIST:
      cout << "Table not exists." << endl;
      break;
    case DB_INDEX_ALREADY_EXIST:
      cout << "Index already exists." << endl;
      break;
    case DB_INDEX_NOT_FOUND:
      cout << "Index not exists." << endl;
      break;
    case DB_COLUMN_NAME_NOT_EXIST:
      cout << "Column not exists." << endl;
      break;
    case DB_KEY_NOT_FOUND:
      cout << "Key not exists." << endl;
      break;
    case DB_QUIT:
      cout << "Bye." << endl;
      break;
    default:
      break;
  }
}

dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) != dbs_.end()) {
    return DB_ALREADY_EXIST;
  }
  dbs_.insert(make_pair(db_name, new DBStorageEngine(db_name, true)));
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) == dbs_.end()) {
    return DB_NOT_EXIST;
  }
  remove(("./databases/" + db_name).c_str());
  delete dbs_[db_name];
  dbs_.erase(db_name);
  if (db_name == current_db_)
    current_db_ = "";
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  if (dbs_.empty()) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_SUCCESS;
  }
  int max_width = 8;
  for (const auto &itr : dbs_) {
    if (itr.first.length() > max_width) max_width = itr.first.length();
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_width) << "Database"
       << " |" << endl;
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  for (const auto &itr : dbs_) {
    cout << "| " << std::left << setfill(' ') << setw(max_width) << itr.first << " |" << endl;
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) != dbs_.end()) {
    current_db_ = db_name;
    cout << "Database changed" << endl;
    return DB_SUCCESS;
  }
  return DB_NOT_EXIST;
}

dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  if (current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  vector<TableInfo *> tables;
  if (dbs_[current_db_]->catalog_mgr_->GetTables(tables) == DB_FAILED) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_FAILED;
  }
  string table_in_db("Tables_in_" + current_db_);
  uint max_width = table_in_db.length();
  for (const auto &itr : tables) {
    if (itr->GetTableName().length() > max_width) max_width = itr->GetTableName().length();
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_width) << table_in_db << " |" << endl;
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  for (const auto &itr : tables) {
    cout << "| " << std::left << setfill(' ') << setw(max_width) << itr->GetTableName() << " |" << endl;
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement - Done
 */
struct ParsedColumnInfo {
    std::string name;
    TypeId type_id{TypeId::kTypeInvalid};
    uint32_t len_for_char = 0;
    bool is_unique_from_col_def = false;
    bool is_not_null_from_col_def = false;
};
dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif

  // 检查是否有选中的数据库
  if (context == nullptr || current_db_.empty()) {
    std::cout << "No database selected for CREATE TABLE operation." << std::endl;
    return DB_FAILED;
  }
  CatalogManager *catalog_manager = context->GetCatalog();
  if (catalog_manager == nullptr) {
    LOG(ERROR) << "Critical error: CatalogManager is null in ExecuteContext for database " << current_db_;
    return DB_FAILED;
  }
  Txn *txn = context->GetTransaction();

  // 获取表名
  if (ast == nullptr || ast->type_ != kNodeCreateTable || ast->child_ == nullptr ||
      ast->child_->type_ != kNodeIdentifier || ast->child_->val_ == nullptr) {
    LOG(ERROR) << "Syntax error: Invalid AST structure for CREATE TABLE statement (missing table name).";
    return DB_FAILED;
  }
  std::string table_name(ast->child_->val_);
  if (table_name.empty()) {
      LOG(ERROR) << "Syntax error: Table name cannot be empty.";
      return DB_FAILED;
  }

  // 初始化用于解析的变量
  std::vector<ParsedColumnInfo> parsed_col_definitions;
  std::vector<std::string> pk_column_names_from_ast;
  std::set<std::string> pk_column_set_for_lookup;

  // 遍历列定义和主键列表
  pSyntaxNode col_def_list_node = ast->child_->next_;
  if (col_def_list_node == nullptr || col_def_list_node->type_ != kNodeColumnDefinitionList) {
    LOG(ERROR) << "Syntax error: CREATE TABLE missing column definition list for table '" << table_name << "'.";
    return DB_FAILED;
  }

  pSyntaxNode current_item_node = col_def_list_node->child_;
  while (current_item_node != nullptr) {
    if (current_item_node->type_ == kNodeColumnDefinition) {
      ParsedColumnInfo pci;
      pSyntaxNode col_name_node = current_item_node->child_;
      if (!col_name_node || col_name_node->type_ != kNodeIdentifier || !col_name_node->val_) {
        LOG(ERROR) << "Syntax error: Malformed column definition - missing column name.";
        return DB_FAILED;
      }
      pci.name = col_name_node->val_;

      pSyntaxNode col_type_node = col_name_node->next_;
      if (!col_type_node || col_type_node->type_ != kNodeColumnType) {
        LOG(ERROR) << "Syntax error: Malformed column definition - missing column type for column '" << pci.name << "'.";
        return DB_FAILED;
      }

      std::string type_name_str(col_type_node->val_);
      std::transform(type_name_str.begin(), type_name_str.end(), type_name_str.begin(), ::tolower);

      if (type_name_str == "int") {
        pci.type_id = TypeId::kTypeInt;
      } else if (type_name_str == "float") {
        pci.type_id = TypeId::kTypeFloat;
      } else if (type_name_str == "char") {
        pci.type_id = TypeId::kTypeChar;
        pSyntaxNode char_len_node = col_type_node->child_;
        if (!char_len_node || char_len_node->type_ != kNodeNumber || !char_len_node->val_) {
          LOG(ERROR) << "Syntax error: CHAR type requires a length for column '" << pci.name << "'.";
          return DB_FAILED;
        }
        std::string len_str(char_len_node->val_);
        // 是否为负数
        if (len_str.find('-') != std::string::npos) {
            LOG(ERROR) << "Syntax error: Length for CHAR column '" << pci.name << "' cannot be negative ('" << len_str << "').";
            return DB_FAILED;
        }
        // 是否为小数 (如果字符串中包含 '.')
        if (len_str.find('.') != std::string::npos) {
            LOG(ERROR) << "Syntax error: Length for CHAR column '" << pci.name << "' must be an integer, not a decimal ('" << len_str << "').";
            return DB_FAILED;
        }
        try {
          unsigned long parsed_len_ul = std::stoul(len_str); 
          // 是否会溢出 uint32_t
          if (parsed_len_ul > std::numeric_limits<uint32_t>::max()) {
              LOG(ERROR) << "Syntax error: Length for CHAR column '" << pci.name << "' is too large ('" << len_str << "').";
              return DB_FAILED;
          }
          pci.len_for_char = static_cast<uint32_t>(parsed_len_ul);

        } catch (const std::invalid_argument &ia) {
          LOG(ERROR) << "Syntax error: Invalid character in length specification for CHAR column '" << pci.name << "' ('" << len_str << "'). Length must be a positive integer.";
          return DB_FAILED;
        } catch (const std::out_of_range &oor) {
          LOG(ERROR) << "Syntax error: Length for CHAR column '" << pci.name << "' is out of range for unsigned long ('" << len_str << "').";
          return DB_FAILED;
        }

        if (pci.len_for_char == 0) {
          LOG(ERROR) << "Invalid length " << pci.len_for_char << " for CHAR column '" << pci.name
                     << "'. Must be a positive integer greater than 0";
          return DB_FAILED;
        }
      } else {
        LOG(ERROR) << "Unsupported column type '" << type_name_str << "' for column '" << pci.name << "'.";
        return DB_FAILED;
      }

      // 解析列级约束 (UNIQUE, NOT NULL)
      // 约束是 current_item_node,其 val_ 是 "UNIQUE" 或 "NOT NULL"
      if (current_item_node->val_ != nullptr) {
          std::string constraint_val(current_item_node->val_);
          std::transform(constraint_val.begin(), constraint_val.end(), constraint_val.begin(), ::tolower);
          //LOG(INFO) << "constrain detect start!";
          if (constraint_val == "unique") {
              pci.is_unique_from_col_def = true;
              //LOG(INFO) << "unique constrain detected!";
          } else if (constraint_val == "not null" ) {
              std::string next_val(current_item_node->val_);
              std::transform(next_val.begin(), next_val.end(), next_val.begin(), ::tolower);
              pci.is_not_null_from_col_def = true;
          }
      }
      parsed_col_definitions.push_back(pci);
    } else if (current_item_node->type_ == kNodeColumnList) {
      // 在 kNodeColumnDefinitionList 中，如果出现 kNodeColumnList 类型的节点，它就代表 PRIMARY KEY (...) 子句。
      if (!pk_column_names_from_ast.empty()) { // 只允许一个主键定义
          LOG(ERROR) << "Syntax error: Multiple PRIMARY KEY definitions for table '" << table_name << "'.";
          return DB_FAILED;
      }
      pSyntaxNode pk_col_name_node = current_item_node->child_;
      while (pk_col_name_node != nullptr) {
        if (pk_col_name_node->type_ != kNodeIdentifier || pk_col_name_node->val_ == nullptr) {
            LOG(ERROR) << "Syntax error: Expected column name in PRIMARY KEY constraint for table '" << table_name << "'.";
            return DB_FAILED;
        }
        std::string pk_name(pk_col_name_node->val_);
        pk_column_names_from_ast.push_back(pk_name);
        pk_column_set_for_lookup.insert(pk_name);
        pk_col_name_node = pk_col_name_node->next_;
      }
    } else {
        LOG(ERROR) << "Syntax error: Unexpected node type '" << GetSyntaxNodeTypeStr(current_item_node->type_)
                   << "' in column definition list for table '" << table_name << "'.";
        return DB_FAILED;
    }
    current_item_node = current_item_node->next_;
  }

  if (parsed_col_definitions.empty()) {
    LOG(ERROR) << "Syntax error: No columns defined for table '" << table_name << "'.";
    return DB_FAILED;
  }

  // 验证主键列名是否都在已定义的列中
  for (const auto& pk_name : pk_column_names_from_ast) {
      bool found = false;
      for (const auto& pci : parsed_col_definitions) { if (pci.name == pk_name) { found = true; break; } }
      if (!found) {
          LOG(ERROR) << "Syntax error: Column '" << pk_name << "' in PRIMARY KEY constraint not defined in table '" << table_name << "'.";
          ExecuteInformation(DB_COLUMN_NAME_NOT_EXIST); return DB_COLUMN_NAME_NOT_EXIST;
      }
  }

  // 创建 Column 对象
  std::vector<Column *> actual_cols_for_schema;
  actual_cols_for_schema.reserve(parsed_col_definitions.size());
  uint32_t col_idx_counter = 0; 
  for (const auto &pci : parsed_col_definitions) {
    bool is_primary_key_col = (pk_column_set_for_lookup.count(pci.name) > 0);
    bool is_nullable_for_constructor = !is_primary_key_col && !pci.is_not_null_from_col_def;
    bool is_unique_for_constructor = pci.is_unique_from_col_def || is_primary_key_col;

    Column *new_column = nullptr;
    if (pci.type_id == TypeId::kTypeChar) {
      new_column = new Column(pci.name, pci.type_id, pci.len_for_char, col_idx_counter, is_nullable_for_constructor, is_unique_for_constructor);
    } else {
      new_column = new Column(pci.name, pci.type_id, col_idx_counter, is_nullable_for_constructor, is_unique_for_constructor);
    }
    actual_cols_for_schema.push_back(new_column);
    col_idx_counter++;
  }

  // 创建 Schema 对象
  TableSchema *schema_to_pass_to_catalog = new Schema(actual_cols_for_schema, true);

  // 调用 CatalogManager 创建表
  TableInfo *created_table_info_ptr = nullptr;
  dberr_t result = catalog_manager->CreateTable(table_name, schema_to_pass_to_catalog, txn, created_table_info_ptr);

  // CatalogManager::CreateTable 内部会进行深拷贝，所以这里创建的 schema_to_pass_to_catalog 需要被删除
  delete schema_to_pass_to_catalog;
  schema_to_pass_to_catalog = nullptr;

  if (result != DB_SUCCESS) {
    ExecuteInformation(result);
    return result;
  }
  // ASSERT(created_table_info_ptr != nullptr, "CatalogManager::CreateTable succeeded but output TableInfo is null.");

  // 创建主键索引 (如果定义了主键)
  if (!pk_column_names_from_ast.empty()) {
    std::string pk_index_name = table_name + "_PK";
    IndexInfo *pk_index_info_output = nullptr;
    dberr_t pk_idx_create_result = catalog_manager->CreateIndex(table_name, pk_index_name, pk_column_names_from_ast, txn, pk_index_info_output, "bptree");
    if (pk_idx_create_result != DB_SUCCESS) {
      LOG(ERROR) << "Table '" << table_name << "' created, but failed to create primary key index '" << pk_index_name << "'. Error code: " << pk_idx_create_result;
      // 尝试回滚：删除已创建的表
      dberr_t drop_res = catalog_manager->DropTable(table_name);
      if (drop_res != DB_SUCCESS) {
          LOG(ERROR) << "CRITICAL: Failed to rollback table '" << table_name << "' creation after PK index creation failed.";
      } else {
          //LOG(INFO) << "Successfully rolled back table '" << table_name << "' after PK index creation failure.";
      }
      ExecuteInformation(pk_idx_create_result); // 调用修正
      return pk_idx_create_result;
    }
  }

  // 创建其他唯一键索引 (基于列级 UNIQUE 非主键的列)
  for (const auto& pci : parsed_col_definitions) {
      if (pci.is_unique_from_col_def && pk_column_set_for_lookup.find(pci.name) == pk_column_set_for_lookup.end()) {
          std::string uk_index_name = table_name + "_" + pci.name + "_UK"; // 示例命名
          std::vector<std::string> uk_key_names = {pci.name};
          IndexInfo *uk_index_info_output = nullptr;
          dberr_t uk_idx_res = catalog_manager->CreateIndex(table_name, uk_index_name, uk_key_names, txn, uk_index_info_output, "bptree");
          if (uk_idx_res != DB_SUCCESS) {
              LOG(WARNING) << "Table '" << table_name << "' created, but failed to create unique index for column '" << pci.name << "'. Error: " << uk_idx_res;
          }
      }
  }

  // 输出成功信息并返回
  std::cout << "Table [" << table_name << "] created successfully." << std::endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement - Done
 */
dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  // 检查是否有选中的数据库
  if (context == nullptr || current_db_.empty()) {
    std::cout << "No database selected." << std::endl;
    return DB_FAILED;
  }
  CatalogManager *catalog_manager = context->GetCatalog();
  if (catalog_manager == nullptr) {
    LOG(ERROR) << "Critical error: CatalogManager is null in ExecuteContext for database " << current_db_;
    return DB_FAILED;
  }
  // 获取要删除的表名
  if (ast == nullptr || ast->type_ != kNodeDropTable || ast->child_ == nullptr ||
      ast->child_->type_ != kNodeIdentifier || ast->child_->val_ == nullptr) {
    LOG(ERROR) << "Syntax error: Invalid AST structure for DROP TABLE statement (missing table name).";
    return DB_FAILED;
  }
  std::string table_name(ast->child_->val_);
  if (table_name.empty()) {
      LOG(ERROR) << "Syntax error: Table name for DROP TABLE cannot be empty.";
      return DB_FAILED;
  }

  // 尝试在 CatalogManager 中删除表
  dberr_t res = catalog_manager->DropTable(table_name);

  // 如果删除失败，返回错误码
  if (res != DB_SUCCESS) {
    ExecuteInformation(res);
    return res;
  }

  std::cout << "Table [" << table_name << "] dropped successfully." << std::endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement - Done
 */
dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  // 检查是否有选中的数据库
  if (context == nullptr || current_db_.empty()) {
    std::cout << "No database selected." << std::endl;
    return DB_FAILED;
  }
  CatalogManager *catalog_manager = context->GetCatalog();
  if (catalog_manager == nullptr) {
    LOG(ERROR) << "Critical error: CatalogManager is null in ExecuteContext for database " << current_db_;
    return DB_FAILED;
  }

  // 获取数据库中的所有表
  std::vector<TableInfo *> tables_in_db;
  dberr_t res_gettables = catalog_manager->GetTables(tables_in_db);

  if (res_gettables != DB_SUCCESS) {
    if (res_gettables == DB_TABLE_NOT_EXIST) {
      std::cout << "No index exists in database '" << current_db_ << "' (no tables found)." << std::endl;
      return DB_SUCCESS; // "SHOW INDEXES" 命令本身是成功的，只是结果为空
    }
    ExecuteInformation(res_gettables);
    return res_gettables;
  }

  // 初始化变量
  bool any_index_found_in_db = false;
  std::stringstream ss; // 用于 ResultWriter
  ResultWriter writer(ss); // 默认 disable_header = false, separator = "|"
  bool first_table_block_printed = true; // 用于控制不同表索引列表之间的前导空行

  // 遍历所有表
  for (TableInfo *table_info : tables_in_db) {
    if (table_info == nullptr) {
        LOG(WARNING) << "Encountered a null TableInfo pointer while iterating tables in ExecuteShowIndexes.";
        continue;
    }

    std::string current_table_name = table_info->GetTableName();
    std::vector<IndexInfo *> indexes_on_this_table;

    // 获取当前表的索引
    dberr_t res_getindexes = catalog_manager->GetTableIndexes(current_table_name, indexes_on_this_table);

    if (res_getindexes != DB_SUCCESS && res_getindexes != DB_INDEX_NOT_FOUND) {
      ExecuteInformation(res_getindexes);
      return res_getindexes;
    }

    if (indexes_on_this_table.empty()) {
      continue; // 此表没有索引，跳到下一个表
    }

    any_index_found_in_db = true;

    // 如果这不是第一个输出索引列表的表，则在前面加一个空行以增加可读性
    if (!first_table_block_printed) {
        ss << std::endl; // 直接向 stringstream 写入换行符
    }
    first_table_block_printed = false;

    // 计算此表索引名称和表头的最大宽度
    std::string header_for_this_table = "Indexes_in_" + current_table_name;
    int max_width_for_this_table = header_for_this_table.length(); // ResultWriter 的 width 参数是 int
    for (IndexInfo *index_info_ptr : indexes_on_this_table) {
      if (index_info_ptr != nullptr) {
        max_width_for_this_table = std::max(max_width_for_this_table, (int)index_info_ptr->GetIndexName().length());
      }
    }
    // 确保列宽至少能容纳 "Index" 
    max_width_for_this_table = std::max(max_width_for_this_table, (int)std::string("Index").length());
    std::vector<int> col_widths = {max_width_for_this_table};

    // 输出当前表的索引列表的表头
    writer.Divider(col_widths);
    writer.BeginRow();
    writer.WriteHeaderCell(header_for_this_table, max_width_for_this_table);
    writer.EndRow();
    writer.Divider(col_widths);

    // 输出当前表的每个索引的名称
    for (IndexInfo *index_info_ptr : indexes_on_this_table) {
      if (index_info_ptr != nullptr) {
        writer.BeginRow();
        writer.WriteCell(index_info_ptr->GetIndexName(), max_width_for_this_table);
        writer.EndRow();
      }
    }
    writer.Divider(col_widths);
  }

  // 输出结果 (如果找到了任何索引)
  if (any_index_found_in_db) {
      std::cout << ss.str();
  }

  // 如果没有任何索引，输出提示信息
  if (!any_index_found_in_db) {
    std::cout << "No index exists in database '" << current_db_ << "'." << std::endl;
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement - Done
 */
dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  // 检查数据库和上下文
  if (context == nullptr || current_db_.empty()) {
    std::cout << "No database selected." << std::endl;
    return DB_FAILED;
  }
  CatalogManager *catalog_manager = context->GetCatalog();
  if (catalog_manager == nullptr) {
    LOG(ERROR) << "Critical error: CatalogManager is null in ExecuteContext for database " << current_db_;
    return DB_FAILED;
  }
  Txn *txn = context->GetTransaction();

  // 从 AST 中获取索引名、表名和列名
  if (ast == nullptr || ast->type_ != kNodeCreateIndex || ast->child_ == nullptr ||
      ast->child_->type_ != kNodeIdentifier || ast->child_->val_ == nullptr) {
    LOG(ERROR) << "Syntax error: Invalid AST for CREATE INDEX (missing index name).";
    return DB_FAILED;
  }
  std::string index_name(ast->child_->val_);

  pSyntaxNode table_name_node = ast->child_->next_;
  if (table_name_node == nullptr || table_name_node->type_ != kNodeIdentifier || table_name_node->val_ == nullptr) {
    LOG(ERROR) << "Syntax error: Invalid AST for CREATE INDEX (missing table name for index '" << index_name << "').";
    return DB_FAILED;
  }
  std::string table_name(table_name_node->val_);

  pSyntaxNode column_list_node = table_name_node->next_;
  if (column_list_node == nullptr || column_list_node->type_ != kNodeColumnList || column_list_node->child_ == nullptr) {
    LOG(ERROR) << "Syntax error: Invalid AST for CREATE INDEX (missing column list for index keys on index '" << index_name << "').";
    return DB_FAILED;
  }
  std::vector<std::string> index_key_column_names_from_ast; // 用于传递给 CatalogManager
  pSyntaxNode current_col_node = column_list_node->child_;
  while (current_col_node != nullptr) {
    if (current_col_node->type_ != kNodeIdentifier || current_col_node->val_ == nullptr) {
      LOG(ERROR) << "Syntax error: Expected column name in index key list for index '" << index_name << "'.";
      return DB_FAILED;
    }
    index_key_column_names_from_ast.push_back(std::string(current_col_node->val_));
    current_col_node = current_col_node->next_;
  }
  if (index_key_column_names_from_ast.empty()) {
      LOG(ERROR) << "Syntax error: No columns specified for index '" << index_name << "'.";
      return DB_FAILED;
  }

  // 处理索引类型
  std::string parsed_index_type = "bptree";
  pSyntaxNode index_type_node_outer = column_list_node->next_;
  if (index_type_node_outer != nullptr && index_type_node_outer->type_ == kNodeIndexType) {
    if (index_type_node_outer->child_ != nullptr && index_type_node_outer->child_->type_ == kNodeIdentifier &&
        index_type_node_outer->child_->val_ != nullptr) {
      parsed_index_type = index_type_node_outer->child_->val_;
    }
  }

  // 获取表信息
  TableInfo *table_info_ptr = nullptr;
  dberr_t get_table_res = catalog_manager->GetTable(table_name, table_info_ptr);
  if (get_table_res != DB_SUCCESS) {
    ExecuteInformation(get_table_res);
    return get_table_res;
  }
  ASSERT(table_info_ptr != nullptr, "GetTable succeeded but table_info_ptr is null.");


  std::vector<uint32_t> key_map_for_population;
  key_map_for_population.reserve(index_key_column_names_from_ast.size());
  TableSchema *table_schema = table_info_ptr->GetSchema();
  if (table_schema == nullptr) {
      LOG(ERROR) << "Table " << table_name << " has no schema. Cannot create index.";
      return DB_FAILED;
  }
  for (const std::string &key_col_name : index_key_column_names_from_ast) {
    uint32_t column_index_in_table;
    if (table_schema->GetColumnIndex(key_col_name, column_index_in_table) != DB_SUCCESS) {
      LOG(ERROR) << "Column '" << key_col_name << "' not found in table '" << table_name 
                 << "' for index '" << index_name << "'.";
      ExecuteInformation(DB_COLUMN_NAME_NOT_EXIST);
      return DB_COLUMN_NAME_NOT_EXIST;
    }
    key_map_for_population.push_back(column_index_in_table);
  }

  // 在 CatalogManager 中创建索引
  // CatalogManager::CreateIndex 接收的是 index_key_column_names_from_ast (字符串列表)
  IndexInfo *catalog_created_index_info = nullptr;
  dberr_t cat_create_idx_res = catalog_manager->CreateIndex(
      table_name, index_name, index_key_column_names_from_ast, txn, catalog_created_index_info, parsed_index_type);

  if (cat_create_idx_res != DB_SUCCESS) {
    ExecuteInformation(cat_create_idx_res);
    return cat_create_idx_res;
  }
  ASSERT(catalog_created_index_info != nullptr, "CatalogManager::CreateIndex succeeded but output IndexInfo is null.");

  // 将表中的现有记录插入到新创建的索引中
  TableHeap *table_heap = table_info_ptr->GetTableHeap();
  if (table_heap == nullptr) {
    LOG(ERROR) << "Failed to get TableHeap for table '" << table_name << "' while populating index '" << index_name << "'.";
    catalog_manager->DropIndex(table_name, index_name);
    return DB_FAILED;
  }

  Index *actual_index_structure = catalog_created_index_info->GetIndex();
  if (actual_index_structure == nullptr) {
      LOG(ERROR) << "Newly created IndexInfo (from CatalogManager) for index '" << index_name << "' does not have an initialized index structure.";
      catalog_manager->DropIndex(table_name, index_name);
      return DB_FAILED;
  }
  // IndexSchema *index_key_actual_schema = catalog_created_index_info->GetIndexKeySchema();
  // ASSERT(index_key_actual_schema != nullptr, "Index key schema is null in created IndexInfo.");

  for (TableIterator it = table_heap->Begin(txn); it != table_heap->End(); ++it) {
    Row table_row(it->GetRowId());
    if (!table_heap->GetTuple(&table_row, txn)) {
        LOG(WARNING) << "Failed to get tuple for rowid (Page: " << it->GetRowId().GetPageId() 
                     << ", Slot: " << it->GetRowId().GetSlotNum() << ") during index population for " << index_name;
        continue; 
    }
    
    RowId original_row_id = table_row.GetRowId();

    // 从表行中提取索引键字段，构建索引行 (row_idx)
    std::vector<Field> index_key_fields;
    index_key_fields.reserve(key_map_for_population.size());
    for (uint32_t table_column_idx_from_key_map : key_map_for_population) {
        // 从 table_row (基于原始表 schema) 中获取第 table_column_idx_from_key_map 个字段
        index_key_fields.push_back(*(table_row.GetField(table_column_idx_from_key_map))); 
    }
    Row index_key_row(index_key_fields); // 创建索引键行

    // 将条目插入物理索引结构
    if (actual_index_structure->InsertEntry(index_key_row, original_row_id, txn) != DB_SUCCESS) {
      LOG(ERROR) << "Failed to insert entry into index '" << index_name << "' for rowid (Page: " 
                 << original_row_id.GetPageId() << ", Slot: " << original_row_id.GetSlotNum()
                 << ") during initial population.";
      catalog_manager->DropIndex(table_name, index_name); // 尝试删除已部分创建的索引
      return DB_FAILED;
    }
  }
  std::cout << "Index [" << index_name << "] created successfully on table [" << table_name << "]." << std::endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement - Done
 */
dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  // 检查是否有选中的数据库
  if (context == nullptr || current_db_.empty()) {
    std::cout << "No database selected." << std::endl;
    return DB_FAILED;
  }
  CatalogManager *catalog_manager = context->GetCatalog();
  if (catalog_manager == nullptr) {
    LOG(ERROR) << "Critical error: CatalogManager is null in ExecuteContext for database " << current_db_;
    return DB_FAILED;
  }

  // 从 AST 中获取索引名
  if (ast == nullptr || ast->type_ != kNodeDropIndex || ast->child_ == nullptr ||
      ast->child_->type_ != kNodeIdentifier || ast->child_->val_ == nullptr) {
    LOG(ERROR) << "Syntax error: Invalid AST structure for DROP INDEX statement (missing index name).";
    return DB_FAILED;
  }
  std::string index_name_to_drop(ast->child_->val_);
  if (index_name_to_drop.empty()) {
      LOG(ERROR) << "Syntax error: Index name for DROP INDEX cannot be empty.";
      return DB_FAILED;
  }


  // 获取数据库中的所有表
  std::vector<TableInfo *> tables_in_db;
  dberr_t get_tables_res = catalog_manager->GetTables(tables_in_db);
  if (get_tables_res != DB_SUCCESS) {
      if (get_tables_res == DB_TABLE_NOT_EXIST) { // 库中没有表，自然没有索引
          ExecuteInformation(DB_INDEX_NOT_FOUND);
          return DB_INDEX_NOT_FOUND;
      }
      ExecuteInformation(get_tables_res); // 其他获取表列表的错误
      return get_tables_res;
  }

  // 遍历所有表
  for (TableInfo *table_info : tables_in_db) {
    if (table_info == nullptr) continue;

    std::string current_table_name = table_info->GetTableName();
    std::vector<IndexInfo *> indexes_on_this_table;

    // 获取当前表的所有索引
    dberr_t get_indexes_res = catalog_manager->GetTableIndexes(current_table_name, indexes_on_this_table);
    
    if (get_indexes_res == DB_SUCCESS) { // 只有当成功获取到索引列表（即使为空）才继续
        // 遍历该表的所有索引
        for (IndexInfo *index_info_ptr : indexes_on_this_table) {
            if (index_info_ptr == nullptr) continue;

            // if index name matches
            if (index_info_ptr->GetIndexName() == index_name_to_drop) {
                // drop index from catalog manager
                dberr_t drop_res = catalog_manager->DropIndex(current_table_name, index_name_to_drop);
                if (drop_res != DB_SUCCESS) {
                    ExecuteInformation(drop_res);
                    return drop_res;
                }
                std::cout << "Index [" << index_name_to_drop << "] dropped successfully from table [" << current_table_name << "]." << std::endl;
                return DB_SUCCESS;
            }
        }
    } else if (get_indexes_res != DB_INDEX_NOT_FOUND) {
        LOG(ERROR) << "Error fetching indexes for table " << current_table_name << " during DROP INDEX operation.";
        ExecuteInformation(get_indexes_res);
        return get_indexes_res;
    }
    // 如果 GetTableIndexes 返回 DB_INDEX_NOT_FOUND，则此表没有索引，继续检查下一个表。
  }
  // 如果遍历完所有表的所有索引都没有找到匹配的索引
  ExecuteInformation(DB_INDEX_NOT_FOUND);
  return DB_INDEX_NOT_FOUND;
}

dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  // 从 AST 中获取文件名
  // AST 结构: kNodeExecFile -> child_ (kNodeString 或 kNodeIdentifier: filename)
  if (ast == nullptr || ast->type_ != kNodeExecFile || ast->child_ == nullptr ||
      (ast->child_->type_ != kNodeString && ast->child_->type_ != kNodeIdentifier) ||
      ast->child_->val_ == nullptr) {
    LOG(ERROR) << "Syntax error: Invalid AST structure for EXECFILE statement (missing filename).";
    ExecuteInformation(DB_FAILED);
    return DB_FAILED;
  }
  std::string file_name(ast->child_->val_);
  if (file_name.empty()) {
    LOG(ERROR) << "Syntax error: Filename for EXECFILE cannot be empty.";
    ExecuteInformation(DB_FAILED);
    return DB_FAILED;
  }

  // 打开 SQL 脚本文件
  std::ifstream sql_script_file(file_name);
  if (!sql_script_file.is_open()) {
    LOG(ERROR) << "Cannot open file '" << file_name << "' for EXECFILE.";
    std::cout << "Error: Cannot open SQL script file '" << file_name << "'." << std::endl;
    return DB_FAILED;
  }

  std::cout << "Executing SQL script file [" << file_name << "] ..." << std::endl;
  std::string current_statement_buffer;
  char ch;
  dberr_t overall_status = DB_SUCCESS;
  int line_count_for_error_reporting = 0;

  // 逐条读取、解析和执行SQL语句
  while (sql_script_file.get(ch)) {
    current_statement_buffer += ch;
    if (ch == '\n') {
        line_count_for_error_reporting++;
    }

    if (ch == ';') {
      // Trim(current_statement_buffer); // 移除首尾空白
      current_statement_buffer.erase(0, current_statement_buffer.find_first_not_of(" \t\n\r\f\v"));
      current_statement_buffer.erase(current_statement_buffer.find_last_not_of(" \t\n\r\f\v") + 1);


      if (current_statement_buffer.empty() || current_statement_buffer == ";") {
        current_statement_buffer.clear();
        continue; // 跳过空语句
      }

      //LOG(INFO) << "Parsing from file [" << file_name << "]: " << current_statement_buffer;

      MinisqlParserInit(); // 初始化解析器状态

      YY_BUFFER_STATE flex_buffer = yy_scan_string(current_statement_buffer.c_str());
      if (flex_buffer == nullptr) {
          LOG(ERROR) << "Failed to create Flex buffer for SQL statement: " << current_statement_buffer;
          overall_status = DB_FAILED;
          MinisqlParserFinish(); // 清理
          break; 
      }
      
      int parse_result = yyparse(); // 调用 Bison 解析器
      yy_delete_buffer(flex_buffer); // 删除为当前语句创建的 Flex 缓冲区

      pSyntaxNode single_statement_ast = MinisqlGetParserRootNode();

      if (parse_result != 0 || single_statement_ast == nullptr || MinisqlParserGetError() != 0) {
        LOG(ERROR) << "Syntax error in file '" << file_name << "' (around line " << line_count_for_error_reporting 
                   << ") for statement: " << current_statement_buffer;
        if (MinisqlParserGetError() != 0 && MinisqlParserGetErrorMessage() != nullptr) {
            std::cout << "Error (approx. line " << line_count_for_error_reporting << "): " << MinisqlParserGetErrorMessage() << std::endl;
        } else {
            std::cout << "Error in file [" << file_name << "] (around line " << line_count_for_error_reporting << "): Syntax error in statement." << std::endl;
        }
        overall_status = DB_FAILED;
        DestroySyntaxTree();      // 清理可能产生的AST
        MinisqlParserFinish();    // 清理状态
        break; // 中止整个脚本的执行
      }

      // 递归调用 ExecuteEngine::Execute() 来执行解析出的单条语句
      // Execute() 函数会自己处理 Planner 和 Executor 的创建和调用
      // 它也会自己根据 current_db_ 创建 ExecuteContext
      dberr_t stmt_exec_result = Execute(single_statement_ast);
      
      DestroySyntaxTree(); // 清理为这条语句解析生成的AST
      MinisqlParserFinish();   // 清理状态

      if (stmt_exec_result == DB_QUIT) {
        overall_status = DB_QUIT;
        std::cout << "QUIT command encountered in script. Halting script execution." << std::endl;
        break; // 脚本中的QUIT会中止脚本执行
      }
      if (stmt_exec_result != DB_SUCCESS) {
        LOG(WARNING) << "Error executing statement from file '" << file_name << "' (around line " << line_count_for_error_reporting
                     << "): " << current_statement_buffer;
        overall_status = stmt_exec_result;
        break;
      }
      current_statement_buffer.clear(); // 为下一条语句清空缓冲区
    }
  }

  sql_script_file.close();

  if (overall_status == DB_SUCCESS) {
      std::cout << "SQL script file [" << file_name << "] executed successfully." << std::endl;
  } else if (overall_status != DB_QUIT) {
      std::cout << "Execution of SQL script file [" << file_name << "] encountered errors." << std::endl;
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement - Done
 */
dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  ExecuteInformation(DB_QUIT);
  return DB_QUIT;
}

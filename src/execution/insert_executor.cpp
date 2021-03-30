//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_{plan}, child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  auto *catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(plan_->TableOid());
  table_indexes_ = catalog->GetTableIndexes(table_info_->name_);
  if (child_executor_ != nullptr) {
    child_executor_->Init();
  }
}

void InsertExecutor::InsertTableAndIndex(Tuple *tuple, RID *rid, Transaction *txn) {
  if (table_info_->table_->InsertTuple(*tuple, rid, txn)) {
    for (const auto &index_info : table_indexes_) {
      Index *index = index_info->index_.get();
      Tuple index_key(tuple->KeyFromTuple(table_info_->schema_, index_info->key_schema_, index->GetKeyAttrs()));
      index->InsertEntry(index_key, *rid, txn);
    }
    return;
  }
  throw Exception("INSERT, tuple to be inserted is bigger than a page");
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (plan_->IsRawInsert()) {
    const auto &raw_values = plan_->RawValues();
    for (const auto &value : raw_values) {
      *tuple = Tuple(value, &table_info_->schema_);
      InsertTableAndIndex(tuple, rid, exec_ctx_->GetTransaction());
    }
    return false;
  }
  while (child_executor_->Next(tuple, rid)) {
    InsertTableAndIndex(tuple, rid, exec_ctx_->GetTransaction());
  }
  return false;
}

}  // namespace bustub

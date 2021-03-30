//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_{plan}, child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  auto *catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(plan_->TableOid());
  table_indexes_ = catalog->GetTableIndexes(table_info_->name_);
  child_executor_->Init();
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  const Schema table_schema = table_info_->schema_;
  if (child_executor_->Next(tuple, rid)) {
    Tuple new_tuple = GenerateUpdatedTuple(*tuple);
    if (table_info_->table_->UpdateTuple(new_tuple, *rid, exec_ctx_->GetTransaction())) {
      UpdateIndex(tuple, &new_tuple, *rid);
      return true;
    }
    // 更新失败, 先删除再插入
    table_info_->table_->MarkDelete(*rid, exec_ctx_->GetTransaction());
    if (table_info_->table_->InsertTuple(new_tuple, rid, exec_ctx_->GetTransaction())) {
      UpdateIndex(tuple, &new_tuple, *rid, true);
      return true;
    }
    throw Exception("UPDATE, new tuple is larger than a page");
  }
  return false;
}
}  // namespace bustub

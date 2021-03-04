//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_{plan}, child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  auto *catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(plan_->TableOid());
  table_indexes_ = catalog->GetTableIndexes(table_info_->name_);
  child_executor_->Init();
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (child_executor_->Next(tuple, rid)) {
    if (table_info_->table_->MarkDelete(*rid, exec_ctx_->GetTransaction())) {
      for (const auto &index_info : table_indexes_) {
        Index *index = index_info->index_.get();
        Tuple index_key(tuple->KeyFromTuple(table_info_->schema_, index_info->key_schema_, index->GetKeyAttrs()));
        index->DeleteEntry(index_key, *rid, exec_ctx_->GetTransaction());
      }
      return true;
    }
    throw Exception("DELETE, delete tuple that does not exist");
  }
  return false;
}

}  // namespace bustub

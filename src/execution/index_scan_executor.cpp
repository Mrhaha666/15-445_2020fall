//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_{plan}, predicate_{plan_->GetPredicate()},
      index_info_{exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid())},
      table_{exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_)->table_.get()}{}

void IndexScanExecutor::Init() {
  auto *b_plus_index = dynamic_cast<BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>> *>(index_info_->index_.get());
  if (b_plus_index == nullptr) {
    throw std::bad_cast();
  }
}

bool IndexScanExecutor::Next(Tuple *tuple, RID *rid) {
  return false;
}

}  // namespace bustub

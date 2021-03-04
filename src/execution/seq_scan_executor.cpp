//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_{plan},
      table_info_{exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())},
      iter_(table_info_->table_->Begin(exec_ctx_->GetTransaction())),
      predicate_{plan_->GetPredicate()} {}

void SeqScanExecutor::Init() {
  iter_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  const Schema *output_scheme = plan_->OutputSchema();
  std::vector<Value> values(output_scheme->GetColumnCount());
  while (iter_ != table_info_->table_->End() && (nullptr != predicate_) &&
         !predicate_->Evaluate(&(*iter_), &table_info_->schema_).GetAs<bool>()) {
    ++iter_;
  }
  if (iter_ != table_info_->table_->End()) {
    const auto& output_columns = output_scheme->GetColumns();
    for (size_t i = 0; i < values.size(); ++i) {
      values[i] = output_columns[i].GetExpr()->Evaluate(&(*iter_),&table_info_->schema_);
    }
    *tuple = Tuple(values, output_scheme);
    *rid = iter_->GetRid();
    ++iter_;
    return true;
  }
  return false;
}

}  // namespace bustub

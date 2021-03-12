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
      table_meta_{exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_)}{}

void IndexScanExecutor::Init() {
  auto *b_plus_index = dynamic_cast<BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>> *>(index_info_->index_.get());
  if (b_plus_index == nullptr) {
    throw std::bad_cast();
  }
  iter_ = b_plus_index->GetBeginIterator();
  end_iter_ = b_plus_index->GetEndIterator();
}

bool IndexScanExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple tmp_tuple;
  while (iter_ != end_iter_) {
    *rid = (*iter_).second;
    ++iter_;
    table_meta_->table_->GetTuple(*rid, &tmp_tuple, exec_ctx_->GetTransaction());
    if (nullptr == predicate_ || predicate_->Evaluate(&tmp_tuple, &table_meta_->schema_).GetAs<bool>()) {
      const Schema *output_scheme = plan_->OutputSchema();
      std::vector<Value> values(output_scheme->GetColumnCount());
      const auto& output_columns = output_scheme->GetColumns();
      for (size_t i = 0; i < values.size(); ++i) {
        values[i] = output_columns[i].GetExpr()->Evaluate(&tmp_tuple,&table_meta_->schema_);
      }
      *tuple = Tuple(values, output_scheme);
      return true;
    }
  }
  return false;
}

}  // namespace bustub

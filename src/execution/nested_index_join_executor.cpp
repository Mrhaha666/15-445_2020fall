//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/column_value_expression.h"


namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_{plan}, child_executor_(std::move(child_executor)),
      inner_table_info_{exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid())},
      index_info_{exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexName(), inner_table_info_->name_)} {}

void NestIndexJoinExecutor::Init() {
  child_executor_->Init();
  const auto *comp_exp = dynamic_cast<const ComparisonExpression *>(plan_->Predicate());
  BUSTUB_ASSERT(comp_exp != nullptr, "NestIndexJoinExecutor: predicate should be a comparison exp");
  const auto &children = comp_exp->GetChildren();
  BUSTUB_ASSERT(children.size() != 2, "NestIndexJoinExecutor: predicate should have two children");
  const auto *left_child = dynamic_cast<const ColumnValueExpression *>(children[0]);
  BUSTUB_ASSERT(left_child != nullptr, "NestIndexJoinExecutor: left child should be a column_value exp");
  const auto *right_child = dynamic_cast<const ColumnValueExpression *>(children[1]);
  BUSTUB_ASSERT(right_child != nullptr, "NestIndexJoinExecutor: right child should be a column_value exp");
  const auto *outter_child = left_child->GetTupleIdx() == 0 ? left_child : right_child;
  BUSTUB_ASSERT(index_info_->key_schema_.GetColumnCount() != 1,
                "NestIndexJoinExecutor: index schema should only have one column");
  outter_join_colidx_ = outter_child->GetColIdx();
}

bool NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) {
  const auto *left_schema = plan_->OuterTableSchema();
  const auto *right_schema = plan_->InnerTableSchema();
  const auto *output_schema = plan_->OutputSchema();
  while (true) {
    if (child_executor_->Next(tuple, rid)) {
      Tuple index_key({tuple->GetValue(plan_->OuterTableSchema(), outter_join_colidx_)}, &index_info_->key_schema_);
      std::vector<RID> result;
      index_info_->index_->ScanKey(index_key, &result, exec_ctx_->GetTransaction());
      if (!result.empty()) {
        Tuple inner_tuple;
        inner_table_info_->table_->GetTuple(result[0], &inner_tuple, exec_ctx_->GetTransaction());
        std::vector<Value> values(output_schema->GetColumnCount());
        const auto &output_columns = output_schema->GetColumns();
        for (size_t k = 0; k < values.size(); ++k) {
          values[k] = output_columns[k].GetExpr()->EvaluateJoin(tuple, left_schema, &inner_tuple, right_schema);
        }
        *tuple = Tuple(values, output_schema);
        return true;
      }
    } else {
      return false;
    }
  }
}
}  // namespace bustub

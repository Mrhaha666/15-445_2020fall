//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx), plan_{plan}, predicate_{plan_->Predicate()},
      left_(std::move(left_executor)), right_(std::move(right_executor)), left_end_{}, right_end_{} {}

void NestedLoopJoinExecutor::Init() {
  left_->Init();
  right_->Init();
  left_end_ = false;
  right_end_ = true;
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (!block_output_tuples_.empty()) {
    *tuple = block_output_tuples_.back();
    block_output_tuples_.pop_back();
    return true;
  }
  const auto *left_schema = plan_->GetLeftPlan()->OutputSchema();
  const auto *right_schema = plan_->GetRightPlan()->OutputSchema();
  const auto *output_schema = plan_->OutputSchema();
  do {
    if (right_end_) {
      block_left_tuples_.clear();
      right_end_ = false;
      for (int i = 0; i < BLOCK_TUPLES_NUM; ++i) {
        if (!left_->Next(tuple, rid)) {
          left_end_ = true;
          break;
        }
        block_left_tuples_.push_back(*tuple);
      }
    }
    block_right_tuples_.clear();
    for (int i = 0; i < BLOCK_TUPLES_NUM; ++i) {
      if (!right_->Next(tuple, rid)) {
        right_end_ = true;
        if (!left_end_) {
          right_->Init();
        }
        break;
      }
      block_right_tuples_.push_back(*tuple);
    }
    std::vector<Value> values(output_schema->GetColumnCount());
    for (const auto &left_tuple : block_left_tuples_) {
      for (const auto &right_tuple : block_right_tuples_) {
        if (plan_->Predicate()->EvaluateJoin(&left_tuple, left_schema, &right_tuple, right_schema).GetAs<bool>()) {
          const auto &output_columns = output_schema->GetColumns();
          for (size_t k = 0; k < values.size(); ++k) {
            values[k] = output_columns[k].GetExpr()->EvaluateJoin(&left_tuple, left_schema, &right_tuple, right_schema);
          }
          block_output_tuples_.emplace_back(values, output_schema);
        }
      }
    }
  } while(block_output_tuples_.empty() && !(left_end_ && right_end_));
  if (!block_output_tuples_.empty()) {
    *tuple = block_output_tuples_.back();
    block_output_tuples_.pop_back();
    return true;
  }
  return false;
}

}  // namespace bustub

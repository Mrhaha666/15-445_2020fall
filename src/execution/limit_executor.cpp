//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_{plan}, child_executor_(std::move(child_executor)),
      emit_tuple_num_{0}, skip_{false} {}

void LimitExecutor::Init() {
  child_executor_->Init();
}

bool LimitExecutor::Next(Tuple *tuple, RID *rid) {
  if (!skip_) {
    skip_ = true;
    size_t i = 0;
    while (child_executor_->Next(tuple, rid) && i < plan_->GetOffset()) {
      i++;
    }
    if (i < plan_->GetOffset()) {
      return false;
    }
  }
  if (emit_tuple_num_ < plan_->GetLimit()) {
    if (child_executor_->Next(tuple, rid)) {
      emit_tuple_num_++;
      return true;
    }
    return false;
  }
  return false;
}

}  // namespace bustub

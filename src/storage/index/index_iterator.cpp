/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  if (buffer_pool_manager_ != nullptr && leaf_ != nullptr) {
    reinterpret_cast<Page *>(leaf_)->RUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_->GetPageId(), false);
  }
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *buffer_pool_manager, Page *leaf, int index_in_leaf)
    : buffer_pool_manager_{buffer_pool_manager}, index_in_leaf_{index_in_leaf} {
  if (nullptr != leaf) {
    leaf_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(leaf);
    // leaf_id_ = leaf->GetPageId();
  }
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() { return leaf_ == nullptr && index_in_leaf_ == -1; }

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() { return leaf_->GetItem(index_in_leaf_); }

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  index_in_leaf_++;
  if (index_in_leaf_ == leaf_->GetSize()) {
    page_id_t next_page_id = leaf_->GetNextPageId();
    if (next_page_id == INVALID_PAGE_ID) {
      reinterpret_cast<Page *>(leaf_)->RUnlatch();
      buffer_pool_manager_->UnpinPage(leaf_->GetPageId(), false);
      leaf_ = nullptr;
      // leaf_id_ = INVALID_PAGE_ID;
      index_in_leaf_ = -1;
    } else {
      Page *page = buffer_pool_manager_->FetchPage(next_page_id);
      reinterpret_cast<Page *>(leaf_)->RUnlatch();
      buffer_pool_manager_->UnpinPage(leaf_->GetPageId(), false);
      if (page->TryRLatch()) {
        leaf_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page);
        // leaf_id_ = leaf_->GetPageId();
        index_in_leaf_ = 0;
      } else {
        reinterpret_cast<Page *>(leaf_)->RUnlatch();
        buffer_pool_manager_->UnpinPage(leaf_->GetPageId(), false);
        buffer_pool_manager_->UnpinPage(next_page_id, false);
        leaf_ = nullptr;
        // leaf_id_ = INVALID_PAGE_ID;
        index_in_leaf_ = -1;
        throw std::exception();
      }
    }
  }
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator=(const INDEXITERATOR_TYPE &rhs) {
  if (this != &rhs) {
    if (leaf_ != nullptr) {
      reinterpret_cast<Page *>(leaf_)->RUnlatch();
      buffer_pool_manager_->UnpinPage(leaf_->GetPageId(), false);
    }
    if (rhs.leaf_ != nullptr) {
      buffer_pool_manager_ = rhs.buffer_pool_manager_;
      Page *page = buffer_pool_manager_->FetchPage((rhs.leaf_)->GetPageId());
      page->RLatch();
      leaf_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page);
      // leaf_id_ = leaf_->GetPageId();
      index_in_leaf_ = rhs.index_in_leaf_;
    } else {
      leaf_ = nullptr;
      // leaf_id_ = INVALID_PAGE_ID;
      index_in_leaf_ = INVALID_PAGE_ID;
    }
  }
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(const IndexIterator &rhs) {
  if (rhs.leaf_ != nullptr) {
    buffer_pool_manager_ = rhs.buffer_pool_manager_;
    Page *page = buffer_pool_manager_->FetchPage((rhs.leaf_)->GetPageId());
    page->RLatch();
    leaf_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page);
    // leaf_id_ = leaf_->GetPageId();
    index_in_leaf_ = rhs.index_in_leaf_;
  }
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub

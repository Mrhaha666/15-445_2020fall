//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator();
  ~IndexIterator();

  //  IndexIterator(BufferPoolManager *buffer_pool_manager, BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *leaf,
  //                BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *last_leaf, int index_in_leaf, int
  //                end_index_last_leaf);
  IndexIterator(BufferPoolManager *buffer_pool_manager, page_id_t leaf_page_id, int index_in_leaf);

  bool isEnd();

  const MappingType &operator*();

  IndexIterator &operator++();

  bool operator==(const IndexIterator &itr) const { return leaf_ == itr.leaf_ && index_in_leaf_ == itr.index_in_leaf_; }

  bool operator!=(const IndexIterator &itr) const { return !(*this == itr); }

  IndexIterator &operator=(const IndexIterator &rhs);
  IndexIterator(const IndexIterator &rhs);

 private:
  // add your own private member variables here
  BufferPoolManager *buffer_pool_manager_{nullptr};
  BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *leaf_{nullptr};
  //  BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *last_leaf_{nullptr};
  int index_in_leaf_{-1};
  //  int end_index_last_leaf_{};
};

}  // namespace bustub

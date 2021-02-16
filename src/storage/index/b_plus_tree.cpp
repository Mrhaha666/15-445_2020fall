//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>
#include <algorithm>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size + 1),
      internal_max_size_(internal_max_size + 1) {
      leaf_max_size_ = std::min(leaf_max_size_, static_cast<int>(LEAF_PAGE_SIZE));
      internal_max_size_ = std::min(internal_max_size_, static_cast<int>(INTERNAL_PAGE_SIZE));
      Page *page = buffer_pool_manager->FetchPage(0);
      auto header_page = reinterpret_cast<HeaderPage *>(page);
      header_page->GetRootId(index_name_, &root_page_id_);
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const {
  return root_page_id_ == INVALID_PAGE_ID;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  Page *page = FindLeafPageOptimistic(key, AccessMode::SEARCH, transaction);
  if (nullptr == page) {
    return false;
  }
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page);
  ValueType value{};
  bool res = false;
  if (leaf_page->Lookup(key, &value, comparator_)) {
    res = true;
    result->push_back(value);
  }
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  transaction->GetPageSet()->clear();
  return res;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  //LOG_DEBUG("entering into Insert");
  //LOG_DEBUG("inserting %ld", *((int64_t*)key.data_));
  bool res =  InsertIntoLeaf(key, value, transaction);
  //LOG_DEBUG("leaving from Insert");
  return res;
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  //LOG_DEBUG("entering into StartNewTree");
  while (hold_root_.test_and_set(std::memory_order_relaxed)) {
    std::this_thread::yield();
  }
  Page *page = buffer_pool_manager_->NewPage(&root_page_id_);
  if (nullptr == page) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "out of memory");
  }
  page->WLatch();
  LeafPage *root_page = reinterpret_cast<LeafPage *>(page);
  root_page->Init(root_page_id_, root_page_id_, leaf_max_size_);
  root_page->Insert(key, value, comparator_);
  page->WUnlatch();
  assert(page->GetPinCount() == 1);
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
  UpdateRootPageId(1);
  hold_root_.clear(std::memory_order_relaxed);
  //LOG_DEBUG("leaving from StartNewTree");
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  //LOG_DEBUG("entering into InsertIntoLeaf");
  bool hold_root = FindLeafPagePessimistic(key, AccessMode::INSERT, transaction);
  auto page_set = transaction->GetPageSet();
  if (page_set->empty()) {
    StartNewTree(key, value);
    return true;
  }
  Page *page = page_set->back();
  page_set->pop_back();

  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page);
  int old_size = leaf_page->GetSize();
  int new_size = leaf_page->Insert(key, value, comparator_);
  bool insert_success = old_size < new_size;
  if (insert_success) {
    if (new_size == leaf_max_size_) {
      LeafPage *new_leaf_page = Split(leaf_page);
      KeyType middle_key = new_leaf_page->KeyAt(0);
      InsertIntoParent(leaf_page, middle_key , new_leaf_page, transaction);
      reinterpret_cast<Page *>(new_leaf_page)->WUnlatch();
      assert(reinterpret_cast<Page *>(new_leaf_page)->GetPinCount() == 1);
      buffer_pool_manager_->UnpinPage(new_leaf_page->GetPageId(), true);
    }
  }
  if (hold_root) {
    hold_root_.clear(std::memory_order_relaxed);
  }
  ReleaseAncestorsLock(transaction);
  reinterpret_cast<Page *>(leaf_page)->WUnlatch();
  assert(reinterpret_cast<Page *>(leaf_page)->GetPinCount() == 1);
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), insert_success);
  //LOG_DEBUG("leaving from InsertIntoLeaf");
  return insert_success;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  //LOG_DEBUG("entering into Split");
  page_id_t new_page_id;
  Page *page = buffer_pool_manager_->NewPage(&new_page_id);
  if (nullptr == page) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "out of memory");
  }
  page->WLatch();
  if (node->IsLeafPage()) {
    LeafPage *new_leaf_page = reinterpret_cast<LeafPage *>(page);
    new_leaf_page->Init(new_page_id, INVALID_PAGE_ID, leaf_max_size_);
    new_leaf_page->SetSize(0);
    LeafPage *old_leaf_page = reinterpret_cast<LeafPage *>(node);
    old_leaf_page->MoveHalfTo(new_leaf_page);
    new_leaf_page->SetNextPageId(old_leaf_page->GetNextPageId());
    old_leaf_page->SetNextPageId(new_leaf_page->GetPageId());
    //LOG_DEBUG("leaving from Split");
    return reinterpret_cast<N *>(new_leaf_page);
  }
  InternalPage *new_internal_page = reinterpret_cast<InternalPage *>(page);
  new_internal_page->Init(new_page_id, INVALID_PAGE_ID, internal_max_size_);
  new_internal_page->SetSize(0);
  InternalPage *old_internal_page = reinterpret_cast<InternalPage *>(node);
  old_internal_page->MoveHalfTo(new_internal_page, buffer_pool_manager_);
  //LOG_DEBUG("leaving from Split");
  return reinterpret_cast<N *>(new_internal_page);
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {

  // assert(old_node->IsLeafPage() == old_node->IsLeafPage());
  if (old_node->IsRootPage()) {
    Page *page = buffer_pool_manager_->NewPage(&root_page_id_);
    if (nullptr == page) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "out of memory");
    }
    page->WLatch();
    InternalPage *root_page = reinterpret_cast<InternalPage *>(page);
    root_page->Init(root_page_id_, root_page_id_, internal_max_size_);
    root_page->SetParentPageId(root_page_id_);
    root_page->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    old_node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);
    page->WUnlatch();
    assert(page->GetPinCount() == 1);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    UpdateRootPageId(0);
    return;
  }

  page_id_t parent_page_id = old_node->GetParentPageId();
  auto page_set = transaction->GetPageSet();
  Page *page = page_set->back();
  page_set->pop_back();
  assert(page->GetPageId() == parent_page_id);
  InternalPage *parent_page = reinterpret_cast<InternalPage *>(page);
  parent_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
  new_node->SetParentPageId(parent_page_id);
  if (parent_page->GetSize() == internal_max_size_) {
    InternalPage *new_internal_page = Split(parent_page);
    KeyType middle_key = parent_page->KeyAt(parent_page->GetMinSize());
    InsertIntoParent(parent_page, middle_key, new_internal_page, transaction);
    reinterpret_cast<Page *>(new_internal_page)->WUnlatch();
    assert(reinterpret_cast<Page *>(new_internal_page)->GetPinCount() == 1);
    buffer_pool_manager_->UnpinPage(new_internal_page->GetPageId(), true);
  }
  reinterpret_cast<Page *>(parent_page)->WUnlatch();
  assert(reinterpret_cast<Page *>(parent_page)->GetPinCount() == 1);
  buffer_pool_manager_->UnpinPage(parent_page_id, true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  LOG_DEBUG("entering into Remove");
  int64_t int_key{};
  memcpy(&int_key, key.data_, sizeof (int64_t));
  LOG_DEBUG("Removing %ld", int_key);
  bool hold_root = FindLeafPagePessimistic(key, AccessMode::DELETE, transaction);
  auto page_set = transaction->GetPageSet();
  if (page_set->empty()) {
    LOG_DEBUG("leaving from Remove");
    return;
  }
  Page *page = page_set->back();
  //assert(transaction->GetPageSet()->back() == page);
  transaction->GetPageSet()->pop_back();
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page);
  int old_size = leaf_page->GetSize();
  int new_size = leaf_page->RemoveAndDeleteRecord(key, comparator_);
  page_id_t leaf_page_id = leaf_page->GetPageId();
  if (new_size < leaf_page->GetMinSize()) {
    CoalesceOrRedistribute(leaf_page, transaction);
    if (hold_root) {
      hold_root_.clear(std::memory_order_relaxed);
    }
    ReleaseAncestorsLock(transaction);
    //LOG_DEBUG("leaving from Remove");
    return;
  }
  if (hold_root) {
    hold_root_.clear(std::memory_order_relaxed);
  }
  ReleaseAncestorsLock(transaction);
  page->WUnlatch();
  assert(page->GetPinCount() == 1);
  buffer_pool_manager_->UnpinPage(leaf_page_id, old_size != new_size);
  //LOG_DEBUG("leaving from Remove");
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  LOG_DEBUG("entering into CoalesceOrRedistribute");
  if (node->IsRootPage()) {
    bool res = AdjustRoot(node);
    LOG_DEBUG("leaving from CoalesceOrRedistribute");
    return res;
  }
  page_id_t sibling_page_id;

  int left_neibor_size{LEAF_PAGE_SIZE + 1};
  int right_neibor_size{LEAF_PAGE_SIZE + 1};
  page_id_t left_neibor{INVALID_PAGE_ID};
  page_id_t right_neibor{INVALID_PAGE_ID};
  page_id_t parent_page_id = node->GetParentPageId();
  auto page_set = transaction->GetPageSet();
  Page *page = page_set->back();
  page_set->pop_back();
  assert(page->GetPageId() == parent_page_id);
  InternalPage *parent_page = reinterpret_cast<InternalPage *>(page);
  int idx = parent_page->ValueIndex(node->GetPageId());
  // 左邻居
  if (idx != 0) {
    sibling_page_id = parent_page->ValueAt(idx - 1);
    left_neibor = sibling_page_id;
    page = buffer_pool_manager_->FetchPage(sibling_page_id);
    page->WLatch();
    N *sibling_page = reinterpret_cast<N *>(page);
    left_neibor_size = sibling_page->GetSize();
    if (left_neibor_size + node->GetSize() > node->GetMaxSize()) {
      Redistribute(sibling_page, node, 1);
      reinterpret_cast<Page *>(parent_page)->WUnlatch();
      assert(reinterpret_cast<Page *>(parent_page)->GetPinCount() == 1);
      buffer_pool_manager_->UnpinPage(parent_page_id, false);
      page->WUnlatch();
      assert(reinterpret_cast<Page *>(sibling_page)->GetPinCount() == 1);
      buffer_pool_manager_->UnpinPage(sibling_page_id, true);
      reinterpret_cast<Page *>(node)->WUnlatch();
      assert(reinterpret_cast<Page *>(node)->GetPinCount() == 1);
      buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
      LOG_DEBUG("leaving from CoalesceOrRedistribute");
      return false;
    }
    page->WUnlatch();
    assert(reinterpret_cast<Page *>(sibling_page)->GetPinCount() == 1);
    buffer_pool_manager_->UnpinPage(sibling_page_id, false);
  }
  // 右邻居
  if (idx != parent_page->GetSize() - 1) {
    sibling_page_id = parent_page->ValueAt(idx + 1);
    right_neibor = sibling_page_id;
    page = buffer_pool_manager_->FetchPage(sibling_page_id);
    page->WLatch();
    N *sibling_page = reinterpret_cast<N *>(page);
    right_neibor_size = sibling_page->GetSize();
    if (right_neibor_size + node->GetSize() > node->GetMaxSize()) {
      Redistribute(sibling_page, node, 0);
      reinterpret_cast<Page *>(parent_page)->WUnlatch();
      assert(reinterpret_cast<Page *>(parent_page)->GetPinCount() == 1);
      buffer_pool_manager_->UnpinPage(parent_page_id, false);
      page->WUnlatch();
      assert(reinterpret_cast<Page *>(sibling_page)->GetPinCount() == 1);
      buffer_pool_manager_->UnpinPage(sibling_page_id, true);
      reinterpret_cast<Page *>(node)->WUnlatch();
      assert(reinterpret_cast<Page *>(node)->GetPinCount() == 1);
      buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
      LOG_DEBUG("leaving from CoalesceOrRedistribute");
      return false;
    }
    page->WUnlatch();
    assert(reinterpret_cast<Page *>(sibling_page)->GetPinCount() == 1);
    buffer_pool_manager_->UnpinPage(sibling_page_id, false);
  }
  // 合并
  page_id_t neibor = left_neibor_size > right_neibor_size ? right_neibor : left_neibor;
  page = buffer_pool_manager_->FetchPage(neibor);
  page->WLatch();
  N *neibor_page = reinterpret_cast<N *>(page);
  N *old_node = node;
  Coalesce(&neibor_page, &node, &parent_page, idx, transaction);
  page_id_t neibor_id = neibor_page->GetPageId();
  reinterpret_cast<Page *>(neibor_page)->WUnlatch();
  assert(reinterpret_cast<Page *>(neibor_page)->GetPinCount() == 1);
  buffer_pool_manager_->UnpinPage(neibor_id, true);

  page_id_t node_id = node->GetPageId();
  reinterpret_cast<Page *>(node)->WUnlatch();
  assert(reinterpret_cast<Page *>(node)->GetPinCount() == 1);
  buffer_pool_manager_->UnpinPage(node_id, true);
  buffer_pool_manager_->DeletePage(node_id);
  transaction->AddIntoDeletedPageSet(node_id);
  LOG_DEBUG("leaving from CoalesceOrRedistribute");
  return node == old_node;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) {
  LOG_DEBUG("entering into Coalesce");
  if (index == 0 || (*parent)->ValueAt(index - 1) != (*neighbor_node)->GetPageId()) {
    using std::swap;
    index += 1;
    swap(*neighbor_node, *node);
  }
  if ((*node)->IsLeafPage()) {
    LeafPage *leaf_page = reinterpret_cast<LeafPage *>(*node);
    LeafPage *neibor_page = reinterpret_cast<LeafPage *>(*neighbor_node);
    leaf_page->MoveAllTo(neibor_page);
    neibor_page->SetNextPageId(leaf_page->GetNextPageId());
    (*parent)->Remove(index);
    bool res = false;
    if ((*parent)->GetSize() < (*parent)->GetMinSize()) {
      res = CoalesceOrRedistribute(*parent, transaction);
    } else {
      reinterpret_cast<Page *>(*parent)->WUnlatch();
      buffer_pool_manager_->UnpinPage((*parent)->GetPageId(), true);
    }
    LOG_DEBUG("leaving from Coalesce");
    return res;
  }
  InternalPage *internal_page = reinterpret_cast<InternalPage *>(*node);
  InternalPage *neibor_page = reinterpret_cast<InternalPage *>(*neighbor_node);
  KeyType middle_key = (*parent)->KeyAt(index);
  internal_page->MoveAllTo(neibor_page, middle_key, buffer_pool_manager_);
  (*parent)->Remove(index);
  bool res = false;
  if ((*parent)->GetSize() < (*parent)->GetMinSize()) {
    res =  CoalesceOrRedistribute(*parent, transaction);
  } else {
    reinterpret_cast<Page *>(*parent)->WUnlatch();
    buffer_pool_manager_->UnpinPage((*parent)->GetPageId(), true);
  }
  LOG_DEBUG("leaving from Coalesce");
  return res;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  LOG_DEBUG("entering into Redistribute");
  if (index == 0) {
    if (node->IsLeafPage()) {
      LeafPage *leaf_page = reinterpret_cast<LeafPage *>(node);
      LeafPage *sibling_page = reinterpret_cast<LeafPage *>(neighbor_node);

      page_id_t parent_page_id = sibling_page->GetParentPageId();
      Page *page = buffer_pool_manager_->FetchPage(parent_page_id);
      LOG_DEBUG("page size %d", page->GetPinCount());
      InternalPage *parent_page = reinterpret_cast<InternalPage *>(page);
      KeyType to_parent_key = sibling_page->KeyAt(1);
      int middle_idx = parent_page->ValueIndex(sibling_page->GetPageId());
      parent_page->SetKeyAt(middle_idx, to_parent_key);
      buffer_pool_manager_->UnpinPage(parent_page_id, true);

      sibling_page->MoveFirstToEndOf(leaf_page);
      LOG_DEBUG("leaving form Redistribute");
      return;
    }
    InternalPage *internal_page = reinterpret_cast<InternalPage *>(node);
    InternalPage *sibling_page = reinterpret_cast<InternalPage *>(neighbor_node);

    page_id_t parent_page_id = sibling_page->GetParentPageId();
    Page *page = buffer_pool_manager_->FetchPage(parent_page_id);
    LOG_DEBUG("page size %d", page->GetPinCount());
    InternalPage *parent_page = reinterpret_cast<InternalPage *>(page);
    KeyType to_parent_key = sibling_page->KeyAt(1);
    int middle_idx = parent_page->ValueIndex(sibling_page->GetPageId());
    KeyType middle_key = parent_page->KeyAt(middle_idx);
    parent_page->SetKeyAt(middle_idx, to_parent_key);
    buffer_pool_manager_->UnpinPage(parent_page_id, true);

    sibling_page->MoveFirstToEndOf(internal_page, middle_key, buffer_pool_manager_);
    LOG_DEBUG("leaving form Redistribute");
    return;
  }
  if (node->IsLeafPage()) {
    LeafPage *leaf_page = reinterpret_cast<LeafPage *>(node);
    LeafPage *sibling_page = reinterpret_cast<LeafPage *>(neighbor_node);

    page_id_t parent_page_id = leaf_page->GetParentPageId();
    Page *page = buffer_pool_manager_->FetchPage(parent_page_id);
    LOG_DEBUG("page size %d", page->GetPinCount());

    InternalPage *parent_page = reinterpret_cast<InternalPage *>(page);
    KeyType to_parent_key = sibling_page->KeyAt(sibling_page->GetSize() - 1);
    int middle_idx = parent_page->ValueIndex(leaf_page->GetPageId());
    parent_page->SetKeyAt(middle_idx, to_parent_key);
    buffer_pool_manager_->UnpinPage(parent_page_id, true);

    sibling_page->MoveLastToFrontOf(leaf_page);
    LOG_DEBUG("leaving form Redistribute");
    return;
  }

  InternalPage *internal_page = reinterpret_cast<InternalPage *>(node);
  InternalPage *sibling_page = reinterpret_cast<InternalPage *>(neighbor_node);

  page_id_t parent_page_id = internal_page->GetParentPageId();
  Page *page = buffer_pool_manager_->FetchPage(parent_page_id);
  LOG_DEBUG("page size %d", page->GetPinCount());

  InternalPage *parent_page = reinterpret_cast<InternalPage *>(page);
  KeyType to_parent_key = sibling_page->KeyAt(sibling_page->GetSize() - 1);
  int middle_idx = parent_page->ValueIndex(internal_page->GetPageId());
  KeyType middle_key = parent_page->KeyAt(middle_idx);
  parent_page->SetKeyAt(middle_idx, to_parent_key);
  buffer_pool_manager_->UnpinPage(parent_page_id, true);

  sibling_page->MoveLastToFrontOf(internal_page, middle_key, buffer_pool_manager_);
  LOG_DEBUG("leaving form Redistribute");

}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  LOG_DEBUG("entering into AdjustRoot");
  if (old_root_node->IsLeafPage()) {
    if (old_root_node->GetSize() > 0){
      reinterpret_cast<Page *>(old_root_node)->WUnlatch();
      assert(reinterpret_cast<Page *>(old_root_node)->GetPinCount() == 1);
      buffer_pool_manager_->UnpinPage(root_page_id_, true);
      LOG_DEBUG("leaving form AdjustRoot");
      return false;
    }
    // 空树
    root_page_id_ = INVALID_PAGE_ID;
    page_id_t old_root_page_id = old_root_node->GetPageId();
    reinterpret_cast<Page *>(old_root_node)->WUnlatch();
    assert(reinterpret_cast<Page *>(old_root_node)->GetPinCount() == 1);
    buffer_pool_manager_->UnpinPage(old_root_page_id, true);
    assert(buffer_pool_manager_->DeletePage(old_root_page_id));
    root_page_id_ = INVALID_PAGE_ID;
    HeaderPage *header_page = reinterpret_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
    header_page->WLatch();
    header_page->DeleteRecord(index_name_);
    header_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
    LOG_DEBUG("leaving form AdjustRoot");
    return true;
  }
  // 根节点更换
  if (old_root_node->GetSize() == 1) {
    InternalPage *root_page = reinterpret_cast<InternalPage *>(old_root_node);
    page_id_t old_root_page_id = old_root_node->GetPageId();
    page_id_t new_root_page_id = root_page->RemoveAndReturnOnlyChild();
    reinterpret_cast<Page *>(old_root_node)->WUnlatch();
    assert(reinterpret_cast<Page *>(old_root_node)->GetPinCount() == 1);
    buffer_pool_manager_->UnpinPage(old_root_page_id, true);
    assert(buffer_pool_manager_->DeletePage(old_root_page_id));

    Page *page = buffer_pool_manager_->FetchPage(new_root_page_id);
    BPlusTreePage *new_root_page = reinterpret_cast<BPlusTreePage *>(page);
    new_root_page->SetParentPageId(new_root_page_id);
    buffer_pool_manager_->UnpinPage(new_root_page_id, true);
    root_page_id_ = new_root_page_id;
    UpdateRootPageId(0);
    LOG_DEBUG("leaving form AdjustRoot");
    return true;
  }
  reinterpret_cast<Page *>(old_root_node)->WUnlatch();
  assert(reinterpret_cast<Page *>(old_root_node)->GetPinCount() == 1);
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
  LOG_DEBUG("leaving form AdjustRoot");
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() {
  if (IsEmpty()) {
    return INDEXITERATOR_TYPE();
  }
  KeyType key{};
  Page *page = FindLeafPage(key, true);
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page);
  page_id_t leaf_page_id = leaf_page->GetPageId();
  buffer_pool_manager_->UnpinPage(leaf_page_id, false);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf_page_id , 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  if (IsEmpty()) {
    return INDEXITERATOR_TYPE();
  }
  Page *page = FindLeafPage(key);
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page);
  int index = leaf_page->KeyIndex(key, comparator_);
  page_id_t leaf_page_id = leaf_page->GetPageId();
  buffer_pool_manager_->UnpinPage(leaf_page_id, false);
  if ( index != -1) {
    return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf_page_id, index);
  }
  return INDEXITERATOR_TYPE();
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() {
  if (IsEmpty()) {
    return INDEXITERATOR_TYPE();
  }
  page_id_t child_page_id = root_page_id_;
  Page *child_page = buffer_pool_manager_->FetchPage(child_page_id);
  BPlusTreePage *b_plus_page = reinterpret_cast<BPlusTreePage *>(child_page);
  while (! b_plus_page->IsLeafPage() ) {
    InternalPage *internal_page = reinterpret_cast<InternalPage *>(b_plus_page);
    child_page_id = internal_page->ValueAt(internal_page->GetSize()-1);
    buffer_pool_manager_->UnpinPage(internal_page->GetPageId(), false);
    child_page = buffer_pool_manager_->FetchPage(child_page_id);
    b_plus_page = reinterpret_cast<BPlusTreePage *>(child_page);
  }
  int end_idx = b_plus_page->GetSize();
  buffer_pool_manager_->UnpinPage(child_page_id, false);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, child_page_id, end_idx);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  //LOG_DEBUG("entering into FindLeafPage");

  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
  BPlusTreePage *b_plus_page = reinterpret_cast<BPlusTreePage *>(page);
  while (!b_plus_page->IsLeafPage()) {
    InternalPage *internal_page = reinterpret_cast<InternalPage *>(b_plus_page);
    page_id_t child_page_id;
    if (leftMost) {
      child_page_id = internal_page->ValueAt(0);
    } else {
      child_page_id = internal_page->Lookup(key, comparator_);
    }
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    page = buffer_pool_manager_->FetchPage(child_page_id);
    b_plus_page = reinterpret_cast<BPlusTreePage *>(page);
  }
  //LOG_DEBUG("leaving from FindLeafPage");
  return page;
}

INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPageOptimistic(const KeyType &key, AccessMode access_mode, Transaction *transaction, bool leftMost){
  LOG_DEBUG("entering into FindLeafPageOptimistic");
  while(hold_root_.test_and_set(std::memory_order_relaxed)) {
      // std::this_thread::yield();
  }
  if (IsEmpty()) {
    hold_root_.clear(std::memory_order_relaxed);
    LOG_DEBUG("leaving from FindLeafPageOptimistic");
    return nullptr;
  }
  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
  page->RLatch();
  BPlusTreePage *b_plus_page = reinterpret_cast<BPlusTreePage *>(page);
  if (b_plus_page->IsLeafPage()) {
    if (access_mode != AccessMode::SEARCH) {
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(root_page_id_, false);
      page = buffer_pool_manager_->FetchPage(root_page_id_);
      page->WLatch();
    } else {
      hold_root_.clear(std::memory_order_relaxed);
    }
    transaction->AddIntoPageSet(page);
    LOG_DEBUG("leaving from FindLeafPageOptimistic");
    return page;
  }
  hold_root_.clear(std::memory_order_relaxed);
  Page *parent_page;
  while (true) {
    parent_page = page;
    InternalPage *internal_page = reinterpret_cast<InternalPage *>(parent_page);
    page_id_t child_page_id;
    if (leftMost) {
      child_page_id = internal_page->ValueAt(0);
    } else {
      child_page_id = internal_page->Lookup(key, comparator_);
    }
    page = buffer_pool_manager_->FetchPage(child_page_id);
    page->RLatch();
    b_plus_page = reinterpret_cast<BPlusTreePage *>(page);
    if (b_plus_page->IsLeafPage()) {
      if (access_mode != AccessMode::SEARCH) {
        page->RUnlatch();
        page->WLatch();
        transaction->AddIntoPageSet(page);
      }
      parent_page->RUnlatch();
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), false);
      LOG_DEBUG("leaving from FindLeafPageOptimistic");
      return page;
    }
    parent_page->RUnlatch();
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), false);
  }
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::FindLeafPagePessimistic(const KeyType &key, AccessMode access_mode, Transaction *transaction) {
  LOG_DEBUG("entering from FindLeafPagePessimistic");
  Page *page = FindLeafPageOptimistic(key, access_mode, transaction);
  if (nullptr == page) {
    LOG_DEBUG("leaving from FindLeafPagePessimistic");
    return false;
  }
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page);
  if (leaf_page->IsSafe(access_mode)) {
    if (leaf_page->IsRootPage()) {
      hold_root_.clear(std::memory_order_relaxed);
    }
    LOG_DEBUG("leaving from FindLeafPagePessimistic");
    return false;
  }
  page->WUnlatch();
  transaction->GetPageSet()->pop_front();
  assert(transaction->GetPageSet()->empty());
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);

//  while(hold_root_.test_and_set(std::memory_order_relaxed)) {
//    // std::this_thread::yield();
//  }

  page = buffer_pool_manager_->FetchPage(root_page_id_);
  page->WLatch();
  BPlusTreePage *b_plus_page = reinterpret_cast<BPlusTreePage *>(page);
  bool root_hold = true;
  while (!b_plus_page->IsLeafPage()) {
    InternalPage *internal_page = reinterpret_cast<InternalPage *>(b_plus_page);
    page_id_t child_page_id;
    child_page_id = internal_page->Lookup(key, comparator_);
    transaction->AddIntoPageSet(page);
    page = buffer_pool_manager_->FetchPage(child_page_id);
    page->WLatch();
    b_plus_page = reinterpret_cast<BPlusTreePage *>(page);
    if (b_plus_page->IsSafe(access_mode)) {
      if (root_hold) {
        hold_root_.clear(std::memory_order_relaxed);
        root_hold = false;
      }
      ReleaseAncestorsLock(transaction);
    }
  }
  transaction->AddIntoPageSet(page);
  LOG_DEBUG("leaving from FindLeafPagePessimistic");
  return root_hold;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReleaseAncestorsLock(Transaction *transaction) {
  auto latched_pages = transaction->GetPageSet();
  for (auto iter : *latched_pages) {
    iter->WUnlatch();
    buffer_pool_manager_->UnpinPage(iter->GetPageId(), false);
  }
  latched_pages->clear();
}



/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key{};
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key{};
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub

//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"
#include <cassert>
#include <cstring>

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : size_{0} {
  size_t bit_num = (num_pages + (sizeof(unsigned char) << 3) - 1U) & ~((sizeof(unsigned char) << 3) - 1U);
  size_t unchar_num = bit_num / (sizeof(unsigned char) << 3);
  pin_bits_ = new unsigned char[unchar_num];
  memset(pin_bits_, 0xff, unchar_num * sizeof(unsigned char));
}

LRUReplacer::~LRUReplacer() { delete[] pin_bits_; }

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if (Size() == 0) {
    *frame_id = -1;
    return false;
  }
  auto iter = unpin_frames_.begin();
  *frame_id = *iter;
  unpin_frames_.erase(iter);
  frame2iter_.erase(*frame_id);
  ClrPinBit(*frame_id);
  size_--;
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  if (0U == GetPinBit(frame_id)) {  // 该 frame 之前没被钉住
    auto map_iter = frame2iter_.find(frame_id);
    if (map_iter != frame2iter_.end()) {  // 该frame在bufferpool中且是unpin状态
      unpin_frames_.erase(map_iter->second);
      frame2iter_.erase(map_iter);
      size_--;
    }  // 隐含的 else 代表的是该frame从磁盘读入并被钉住,不需要做额外的事情
    SetPinBit(frame_id);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  if (0U != GetPinBit(frame_id)) {  // 该 frame 之前被钉住
    ClrPinBit(frame_id);
    size_++;
    unpin_frames_.push_back(frame_id);
    frame2iter_.insert(std::make_pair(frame_id, --unpin_frames_.end()));
  } else {  // 该frame 之前就未被钉住, 正常情况不会再次Unpin一个Unpin的page,但防止意外写下此种分支.
            // (但好像测试通不过，只好再注释掉)
    //    auto map_iter = frame2iter_.find(frame_id);
    //    assert( map_iter != frame2iter_.end());
    //    if (map_iter != frame2iter_.end()) {
    //      unpin_frames_.erase(map_iter->second);
    //      frame2iter_.erase(map_iter);
    //    }
    //    unpin_frames_.push_back(frame_id);
    //    frame2iter_.insert(std::make_pair(frame_id, --unpin_frames_.end()));
  }
}

inline size_t LRUReplacer::Size() { return size_; }

inline size_t LRUReplacer::GetPinBit(frame_id_t frame_id) {
  size_t idx1 = frame_id / (sizeof(unsigned char) << 3);
  size_t idx2 = frame_id - idx1 * (sizeof(unsigned char) << 3);
  return pin_bits_[idx1] & (static_cast<unsigned char>(1) << idx2);
}

inline void LRUReplacer::SetPinBit(frame_id_t frame_id) {
  size_t idx1 = frame_id / (sizeof(unsigned char) << 3);
  size_t idx2 = frame_id - idx1 * (sizeof(unsigned char) << 3);
  pin_bits_[idx1] |= (static_cast<unsigned char>(1) << idx2);
}

inline void LRUReplacer::ClrPinBit(frame_id_t frame_id) {
  size_t idx1 = frame_id / (sizeof(unsigned char) << 3);
  size_t idx2 = frame_id - idx1 * (sizeof(unsigned char) << 3);
  pin_bits_[idx1] &= ~(static_cast<unsigned char>(1) << idx2);
}

}  // namespace bustub

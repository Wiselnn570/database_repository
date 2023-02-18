/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "page/b_plus_tree_leaf_page.h"

namespace scudb {

#define INDEXITERATOR_TYPE                                                     \
  IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
public:
  // you may define your own constructor based on your member variables
  IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE *leaf, int index, BufferPoolManager *bufferPoolManager);
  ~IndexIterator();

  bool isEnd(){
    return (leaf == nullptr);
  }

  const MappingType &operator*() {
    return leaf->GetItem(index);
  }

  IndexIterator &operator++() {
    index++;
    if (index >= leaf->GetSize()) {
      page_id_t next = leaf->GetNextPageId();
      bufferPoolManager->FetchPage(leaf->GetPageId())->RUnlatch();
      bufferPoolManager->UnpinPage(leaf->GetPageId(), false);
      if (next == INVALID_PAGE_ID) {
        leaf = nullptr;
      } else {
        Page *page = bufferPoolManager_->FetchPage(next);
        page->RLatch();
        leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
        index = 0;
      }
    }
    return *this;
  }

private:
  int index;
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf;
  BufferPoolManager *bufferPoolManager;
};

} // namespace scudb

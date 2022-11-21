// 魏熙林 2019141410450
#include "buffer/lru_replacer.h"
#include "page/page.h"
#include <iostream>
#include <cstring>

namespace scudb
{

  template <typename T>
  LRUReplacer<T>::LRUReplacer()
  {
    head = make_shared<Node>();
    tail = make_shared<Node>();
    head->ne = tail;
    tail->pre = head;
  }

  template <typename T>
  LRUReplacer<T>::~LRUReplacer() {}

  // 将值插入LRU
  template <typename T>
  void LRUReplacer<T>::Insert(const T &value)
  {
    lock_guard<mutex> lck(mtx);
    shared_ptr<Node> cur;
    if (mp.find(value) != mp.end())
    {
      cur = mp[value];
      shared_ptr<Node> pre = cur->pre;
      shared_ptr<Node> success = cur->ne;
      pre->ne = success;
      success->pre = pre;
    }
    else
    {
      cur = make_shared<Node>(value);
    }
    shared_ptr<Node> fir = head->ne;
    cur->ne = fir, fir->pre = cur, cur->pre = head, head->ne = cur;
    mp[value] = cur;
    return;
  }

  template <typename T>
  bool LRUReplacer<T>::Victim(T &value)
  {
    lock_guard<mutex> lck(mtx);
    // 如果lru为空返回false
    if (mp.empty())
    {
      return false;
    }
    // 如果lru不为空则弹出tail所指元素，返回true
    shared_ptr<Node> last = tail->pre;
    tail->pre = last->pre;
    last->pre->ne = tail;
    value = last->val;
    mp.erase(last->val);
    return true;
  }

  // 链表操作，将当前删除节点从链表中去掉
  template <typename T>
  bool LRUReplacer<T>::Erase(const T &value)
  {
    lock_guard<mutex> lck(mtx);
    if (mp.find(value) != mp.end())
    {
      shared_ptr<Node> cur = mp[value];
      cur->pre->ne = cur->ne;
      cur->ne->pre = cur->pre;
    }
    return mp.erase(value);
  }

  template <typename T>
  size_t LRUReplacer<T>::Size()
  {
    lock_guard<mutex> lck(mtx);
    return mp.size();
  }

  template class LRUReplacer<Page *>;
  template class LRUReplacer<int>;

}

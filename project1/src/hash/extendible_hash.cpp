// 魏熙林 2019141410450
#include <list>
#include <bitset>
#include <iostream>
#include <algorithm>

#include "hash/extendible_hash.h"
#include "page/page.h"
using namespace std;

namespace scudb
{

  template <typename K, typename V>
  ExtendibleHash<K, V>::ExtendibleHash(size_t size) : global_depth(0), bucket_size(size), bucket_num(1)
  {
    buckets.push_back(make_shared<Bucket>(0));
  }
  template <typename K, typename V>
  ExtendibleHash<K, V>::ExtendibleHash()
  {
    ExtendibleHash(64);
  }

  // 计算输入键哈希值
  template <typename K, typename V>
  size_t ExtendibleHash<K, V>::HashKey(const K &key) const
  {
    return hash<K>{}(key);
  }

  // 返回哈希值的全局深度
  template <typename K, typename V>
  int ExtendibleHash<K, V>::GetGlobalDepth() const
  {
    lock_guard<mutex> lck(mtx);
    return global_depth;
  }

  // 返回一个具体桶的局部深度
  template <typename K, typename V>
  int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const
  {
    if (buckets[bucket_id])
    {
      lock_guard<mutex> lck(buckets[bucket_id]->mtx);
      if (buckets[bucket_id]->mp.size() == 0)
        return -1;
      return buckets[bucket_id]->localDepth;
    }
    return -1;
  }

  // 返回哈希表中当前桶的数量
  template <typename K, typename V>
  int ExtendibleHash<K, V>::GetNumBuckets() const
  {
    lock_guard<mutex> lck(mtx);
    return bucket_num;
  }

  // 查找与输入键相关的值
  template <typename K, typename V>
  bool ExtendibleHash<K, V>::Find(const K &key, V &value)
  {

    int idx = getIdx(key);
    lock_guard<mutex> lck(buckets[idx]->mtx);
    if (buckets[idx]->mp.find(key) != buckets[idx]->mp.end())
    {
      value = buckets[idx]->mp[key];
      return true;
    }
    return false;
  }

  template <typename K, typename V>
  int ExtendibleHash<K, V>::getIdx(const K &key) const
  {
    lock_guard<mutex> lck(mtx);
    return HashKey(key) & ((1 << global_depth) - 1);
  }

  // 删除函数
  template <typename K, typename V>
  bool ExtendibleHash<K, V>::Remove(const K &key)
  {
    int idx = getIdx(key);
    lock_guard<mutex> lck(buckets[idx]->mtx);
    shared_ptr<Bucket> cur = buckets[idx];
    if (cur->mp.find(key) == cur->mp.end())
    {
      return false;
    }
    cur->mp.erase(key);
    return true;
  }

  // 插入函数
  template <typename K, typename V>
  void ExtendibleHash<K, V>::Insert(const K &key, const V &value)
  {
    int idx = getIdx(key);
    shared_ptr<Bucket> cur = buckets[idx];
    while (1)
    {
      lock_guard<mutex> lck(cur->mtx);
      // 如果说哈希值桶内能找到或者没装满就可以插入
      if (cur->mp.find(key) != cur->mp.end() || cur->mp.size() < bucket_size)
      {
        cur->mp[key] = value;
        break;
      }
      int mask = (1 << (cur->localDepth));
      cur->localDepth++;

      {
        lock_guard<mutex> lock2(mtx);
        if (cur->localDepth > global_depth)
        {

          size_t length = buckets.size();
          for (size_t i = 0; i < length; i++)
          {
            buckets.push_back(buckets[i]);
          }
          global_depth++;
        }
        bucket_num++;
        //如果散列表中指针没有创建对应桶则新建桶
        auto newBuc = make_shared<Bucket>(cur->localDepth);

        typename map<K, V>::iterator it;
        for (it = cur->mp.begin(); it != cur->mp.end();)
        {
          if (HashKey(it->first) & mask)
          {
            newBuc->mp[it->first] = it->second;
            it = cur->mp.erase(it);
          }
          else
            it++;
        }
        // 遍历桶数组
        for (size_t i = 0; i < buckets.size(); i++)
        {
          if (buckets[i] == cur && (i & mask))
            buckets[i] = newBuc;
        }
      }
      idx = getIdx(key);
      cur = buckets[idx];
    }
  }

  template class ExtendibleHash<page_id_t, Page *>;
  template class ExtendibleHash<Page *, std::list<Page *>::iterator>;
  // test purpose
  template class ExtendibleHash<int, std::string>;
  template class ExtendibleHash<int, std::list<int>::iterator>;
  template class ExtendibleHash<int, int>;
} // namespace scudb

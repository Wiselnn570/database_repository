// 魏熙林 2019141410450
#pragma once

#include <memory>
#include <unordered_map>
#include <mutex>
#include "buffer/replacer.h"

using namespace std;
namespace scudb
{

  template <typename T>
  class LRUReplacer : public Replacer<T>
  {
    struct Node
    {
      Node(){};
      Node(T val) : val(val){};
      T val;
      shared_ptr<Node> pre;
      shared_ptr<Node> ne;
    };

  public:
    LRUReplacer();

    ~LRUReplacer();

    void Insert(const T &value);

    bool Victim(T &value);

    bool Erase(const T &value);

    size_t Size();

  private:
    shared_ptr<Node> head;
    shared_ptr<Node> tail;
    unordered_map<T, shared_ptr<Node>> map;
    mutable mutex mtx;
  };

}

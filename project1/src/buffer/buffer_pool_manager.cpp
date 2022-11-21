// 魏熙林 2019141410450
#include "buffer/buffer_pool_manager.h"
#include <iostream>
namespace scudb
{
  BufferPoolManager::BufferPoolManager(size_t pool_size,
                                       DiskManager *disk_manager,
                                       LogManager *log_manager)
      : pool_size_(pool_size), disk_manager_(disk_manager),
        log_manager_(log_manager)
  {
    // 用于缓冲池的连续内存空间
    pages_ = new Page[pool_size_];
    page_table_ = new ExtendibleHash<page_id_t, Page *>(BUCKET_SIZE);
    replacer_ = new LRUReplacer<Page *>;
    free_list_ = new std::list<Page *>;

    // 初始化的时候，把所有页面放进可用表里
    for (size_t i = 0; i < pool_size_; ++i)
    {
      free_list_->push_back(&pages_[i]);
    }
  }

  BufferPoolManager::~BufferPoolManager()
  {
    delete[] pages_;
    delete page_table_;
    delete replacer_;
    delete free_list_;
  }

  Page *BufferPoolManager::FetchPage(page_id_t page_id)
  {
    lock_guard<mutex> lck(latch_);
    Page *target = nullptr;
    if (page_table_->Find(page_id, target))
    { // 如果存在，给页面打上标记并立即返回
      target->pin_count_++;
      replacer_->Erase(target);
      return target;
    }
    // 如果不存在，从空闲列表或lru replacer中查找替换条目。
    target = GetVictimPage();
    if (target == nullptr)
      return target;
    // 如果选择替换的条目被修改过的，则将其写回磁盘。
    if (target->is_dirty_)
    {
      disk_manager_->WritePage(target->GetPageId(), target->data_);
    }
    // 从散列表中删除旧页面的条目，并为新页面插入一个条目。
    page_table_->Remove(target->GetPageId());
    page_table_->Insert(page_id, target);
    // 更新页面元数据，从磁盘文件读取页面内容并返回页面指针
    disk_manager_->ReadPage(page_id, target->data_);
    target->pin_count_ = 1;
    target->is_dirty_ = false;
    target->page_id_ = page_id;

    return target;
  }

  bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty)
  {
    lock_guard<mutex> lck(latch_);
    Page *target = nullptr;
    page_table_->Find(page_id, target);
    if (target == nullptr)
    {
      return false;
    }
    target->is_dirty_ = is_dirty;
    if (target->GetPinCount() <= 0)
    {
      return false;
    };
    if (--target->pin_count_ == 0)
    {
      replacer_->Insert(target);
    }
    return true;
  }

 // 属于utils，将缓冲区修改页面写回磁盘
  bool BufferPoolManager::FlushPage(page_id_t page_id)
  {
    lock_guard<mutex> lck(latch_);
    Page *target = nullptr;
    page_table_->Find(page_id, target);
    if (target == nullptr || target->page_id_ == INVALID_PAGE_ID)
    {
      return false;
    }
    if (target->is_dirty_)
    {
      disk_manager_->WritePage(page_id, target->GetData());
      target->is_dirty_ = false;
    }

    return true;
  }

  /*
  用户应该调用此方法来删除页面。这个例程将调用磁盘管理器来释放页面。
  首先，如果页表中发现了页，缓冲池管理器应该负责从页表中删除该条目，重置页元数据并将其添加回空闲列表。
  其次，调用磁盘管理器的DeallocatePage()方法从磁盘文件中删除。
  */
  bool BufferPoolManager::DeletePage(page_id_t page_id)
  {
    lock_guard<mutex> lck(latch_);
    Page *target = nullptr;
    page_table_->Find(page_id, target);
    if (target != nullptr)
    {
      if (target->GetPinCount() > 0)
      {
        return false;
      }
      replacer_->Erase(target);
      page_table_->Remove(page_id);
      target->is_dirty_ = false;
      target->ResetMemory();
      free_list_->push_back(target);
    }
    disk_manager_->DeallocatePage(page_id);
    return true;
  }

  Page *BufferPoolManager::NewPage(page_id_t &page_id)
  {
    lock_guard<mutex> lck(latch_);
    Page *target = nullptr;
    target = GetVictimPage();
    if (target == nullptr)
      return target;

    page_id = disk_manager_->AllocatePage();

    if (target->is_dirty_)
    {
      disk_manager_->WritePage(target->GetPageId(), target->data_);
    }

    page_table_->Remove(target->GetPageId());
    page_table_->Insert(page_id, target);

    target->page_id_ = page_id;
    target->ResetMemory();
    target->is_dirty_ = false;
    target->pin_count_ = 1;

    return target;
  }

  Page *BufferPoolManager::GetVictimPage()
  {
    Page *target = nullptr;
    if (free_list_->empty())
    {
      if (replacer_->Size() == 0)
      {
        return nullptr;
      }
      replacer_->Victim(target);
    }
    else
    {
      target = free_list_->front();
      free_list_->pop_front();
      assert(target->GetPageId() == INVALID_PAGE_ID);
    }
    assert(target->GetPinCount() == 0);
    return target;
  }

}

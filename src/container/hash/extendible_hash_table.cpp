//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstddef>
#include <cstdlib>
#include <functional>
#include <list>
#include <memory>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
      std::shared_ptr<Bucket> first_bucket=std::make_shared<Bucket>(bucket_size,0);
      dir_.push_back(first_bucket);
    }

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  //UNREACHABLE("not implemented");
  std::scoped_lock<std::mutex> lock(latch_); // 加锁
  size_t key_index=IndexOf(key);
  if(static_cast<size_t>(num_buckets_)<key_index+1){
    return false;
  }
  return dir_[key_index]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  //UNREACHABLE("not implemented");
  std::scoped_lock<std::mutex> lock(latch_); // 加锁
  size_t key_index=IndexOf(key);
  if(static_cast<size_t>(num_buckets_)<key_index+1){
    return false;
  }

  return dir_[key_index]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  // UNREACHABLE("not implemented");
  std::scoped_lock<std::mutex> lock(latch_); // 加锁
  size_t key_index=IndexOf(key);

  //插入不成功等价于对应桶满
  while(!dir_[key_index]->Insert(key,value))
  {
    //桶分裂
    //如果存储桶的本地深度等于全局深度，则增加全局深度并将目录的大小增加一倍。
    if(dir_[key_index]->GetDepth()==global_depth_){
      global_depth_++;
      size_t dir_size=dir_.size();
      for(size_t i=0;i<dir_size;i++){
        dir_.push_back(dir_[i]);
      }
    }
    //增加存储桶的本地深度
    dir_[key_index]->IncrementDepth();
    //创建一个新的空桶
    std::shared_ptr<Bucket> newbucket=std::make_shared<Bucket>(bucket_size_,dir_[key_index]->GetDepth());
    //重新分配存储桶中的目录指针
    //感觉需要更改的目录指针，应该是key_index+2的localdepth-1次方，让对应的目录指向新的bucket
    dir_[key_index+(1<<(dir_[key_index]->GetDepth()-1))]=newbucket;

    //重新分配存储桶中的kv对
    auto datalist=dir_[key_index]->GetItems();
    dir_[key_index]->ClearList();
    for(auto& pair :datalist){
      size_t new_index=IndexOf(pair.first);
      dir_[new_index]->Insert(pair.first,pair.second);
    }
  }

}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  // UNREACHABLE("not implemented");
  for(auto& pair:list_){
        if(pair.first==key){ 
          value=pair.second;
          // pair.second=value;
          return true;
        }
      }
      return false;
}

//remove删除的可能不止一个元素
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  // UNREACHABLE("not implemented");
  bool flag=false;
  for(auto iter=list_.begin();iter!=list_.end();){
    if(iter->first==key){
      iter=list_.erase(iter);//返回被删除位置的后一个迭代器
      flag=true;
    }
    else{
      iter++;
    }
  }
  return flag;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  // UNREACHABLE("not implemented");
        if(IsFull()){
        return false;
      }
      list_.push_back(std::pair<K,V>{key, value});
      return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub

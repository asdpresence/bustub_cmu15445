/**
 * lru_k_replacer_test.cpp
 */

#include "buffer/lru_k_replacer.h"

#include <algorithm>
#include <cstdio>
#include <memory>
#include <random>
#include <set>
#include <thread>  // NOLINT
#include <vector>

#include "gtest/gtest.h"

namespace bustub {

TEST(LRUKReplacerTest, SampleTest) {
  LRUKReplacer lru_replacer(7, 2);

  // Scenario: add six elements to the replacer. We have [1,2,3,4,5]. Frame 6 is non-evictable.
  lru_replacer.RecordAccess(1);
  lru_replacer.RecordAccess(2);
  lru_replacer.RecordAccess(3);
  lru_replacer.RecordAccess(4);
  lru_replacer.RecordAccess(5);
  lru_replacer.RecordAccess(6);
  lru_replacer.SetEvictable(1, true);
  lru_replacer.SetEvictable(2, true);
  lru_replacer.SetEvictable(3, true);
  lru_replacer.SetEvictable(4, true);
  lru_replacer.SetEvictable(5, true);
  lru_replacer.SetEvictable(6, false);
  ASSERT_EQ(5, lru_replacer.Size());

  // Scenario: Insert access history for frame 1. Now frame 1 has two access histories.
  // All other frames have max backward k-dist. The order of eviction is [2,3,4,5,1].
  lru_replacer.RecordAccess(1);

  // Scenario: Evict three pages from the replacer. Elements with max k-distance should be popped
  // first based on LRU.
  int value;
  lru_replacer.Evict(&value);
  ASSERT_EQ(2, value);
  lru_replacer.Evict(&value);
  ASSERT_EQ(3, value);
  lru_replacer.Evict(&value);
  ASSERT_EQ(4, value);
  ASSERT_EQ(2, lru_replacer.Size());

  // Scenario: Now replacer has frames [5,1].
  // Insert new frames 3, 4, and update access history for 5. We should end with [3,1,5,4]
  lru_replacer.RecordAccess(3);
  lru_replacer.RecordAccess(4);
  lru_replacer.RecordAccess(5);
  lru_replacer.RecordAccess(4);
  lru_replacer.SetEvictable(3, true);
  lru_replacer.SetEvictable(4, true);
  ASSERT_EQ(4, lru_replacer.Size());

  // Scenario: continue looking for victims. We expect 3 to be evicted next.
  lru_replacer.Evict(&value);
  ASSERT_EQ(3, value);
  ASSERT_EQ(3, lru_replacer.Size());

  // Set 6 to be evictable. 6 Should be evicted next since it has max backward k-dist.
  lru_replacer.SetEvictable(6, true);
  ASSERT_EQ(4, lru_replacer.Size());
  lru_replacer.Evict(&value);
  ASSERT_EQ(6, value);
  ASSERT_EQ(3, lru_replacer.Size());

  // Now we have [1,5,4]. Continue looking for victims.
  lru_replacer.SetEvictable(1, false);
  ASSERT_EQ(2, lru_replacer.Size());
  ASSERT_EQ(true, lru_replacer.Evict(&value));
  ASSERT_EQ(5, value);
  ASSERT_EQ(1, lru_replacer.Size());

  // Update access history for 1. Now we have [4,1]. Next victim is 4.
  lru_replacer.RecordAccess(1);
  lru_replacer.RecordAccess(1);
  lru_replacer.SetEvictable(1, true);
  ASSERT_EQ(2, lru_replacer.Size());
  ASSERT_EQ(true, lru_replacer.Evict(&value));
  ASSERT_EQ(value, 4);

  ASSERT_EQ(1, lru_replacer.Size());
  lru_replacer.Evict(&value);
  ASSERT_EQ(value, 1);
  ASSERT_EQ(0, lru_replacer.Size());

  // These operations should not modify size
  ASSERT_EQ(false, lru_replacer.Evict(&value));
  ASSERT_EQ(0, lru_replacer.Size());
  lru_replacer.Remove(1);
  ASSERT_EQ(0, lru_replacer.Size());
}

TEST(LRUKReplacerTest, DuplicateSetEvictable) {
  LRUKReplacer lru_replacer(5, 2);

  lru_replacer.RecordAccess(1);
  lru_replacer.RecordAccess(1);
  lru_replacer.SetEvictable(1, true);
  EXPECT_EQ(lru_replacer.Size(), 1);

  // 重复设置同一帧为可驱逐
  lru_replacer.SetEvictable(1, true);
  EXPECT_EQ(lru_replacer.Size(), 1);  // 大小应保持不变

  // 重复设置同一帧为不可驱逐
  lru_replacer.SetEvictable(1, false);
  EXPECT_EQ(lru_replacer.Size(), 0);

  lru_replacer.SetEvictable(1, false);
  EXPECT_EQ(lru_replacer.Size(), 0);  // 大小应保持不变
}

TEST(LRUKReplacerTest, RemoveNonEvictableFrame) {
  LRUKReplacer lru_replacer(5, 2);

  lru_replacer.SetEvictable(1, false);
  EXPECT_EQ(lru_replacer.Size(), 0);

  // 尝试移除不可驱逐的帧
  lru_replacer.Remove(1);
  EXPECT_EQ(lru_replacer.Size(), 0);  // 大小应保持不变
}

TEST(LRUKReplacerTest, ConcurrentAccess) {
  LRUKReplacer lru_replacer(100, 2);

  auto set_evictable = [&lru_replacer](int start, int end) {
    for (int i = start; i < end; ++i) {
      lru_replacer.SetEvictable(i, true);
    }
  };

  auto remove_evictable = [&lru_replacer](int start, int end) {
    for (int i = start; i < end; ++i) {
      lru_replacer.SetEvictable(i, false);
    }
  };

  std::thread t1(set_evictable, 0, 50);
  std::thread t2(set_evictable, 50, 100);
  std::thread t3(remove_evictable, 25, 75);

  t1.join();
  t2.join();
  t3.join();

  // 最终，evictable_frames_ 应包含 0-24 和 75-99，共50个帧
  EXPECT_EQ(lru_replacer.Size(), 50);
}

}  // namespace bustub

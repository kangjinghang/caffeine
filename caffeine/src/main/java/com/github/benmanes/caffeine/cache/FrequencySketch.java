/*
 * Copyright 2015 Ben Manes. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.benmanes.caffeine.cache;

import static com.github.benmanes.caffeine.cache.Caffeine.requireArgument;

import org.checkerframework.checker.index.qual.NonNegative;

/**
 * A probabilistic multiset for estimating the popularity of an element within a time window. The
 * maximum frequency of an element is limited to 15 (4-bits) and an aging process periodically
 * halves the popularity of all elements.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
final class FrequencySketch<E> {

  /*
   * This class maintains a 4-bit CountMinSketch [1] with periodic aging to provide the popularity
   * history for the TinyLfu admission policy [2]. The time and space efficiency of the sketch
   * allows it to cheaply estimate the frequency of an entry in a stream of cache access events.
   *
   * The counter matrix is represented as a single dimensional array holding 16 counters per slot. A
   * fixed depth of four balances the accuracy and cost, resulting in a width of four times the
   * length of the array. To retain an accurate estimation the array's length equals the maximum
   * number of entries in the cache, increased to the closest power-of-two to exploit more efficient
   * bit masking. This configuration results in a confidence of 93.75% and error bound of e / width.
   *
   * To improve hardware efficiency an item's counters are constrained to a 64 byte block, which is
   * the size of an L1 cache line. This differs from the theoretical ideal where counters are
   * uniformly distributed in order to minimize collisions. In that configuration the memory
   * accesses are not predictable and lack spatial locality, which may cause the pipeline to need to
   * wait for four memory loads. Instead the items are uniformly distributed to blocks and each
   * counter is selected uniformly from a distinct 16 byte segment. While the runtime memory layout
   * may result in the blocks not being cache aligned, the L2 spatial prefetcher tries to load
   * aligned pairs of cache lines so the typical cost is only one memory access.
   *
   * The frequency of all entries is aged periodically using a sampling window based on the maximum
   * number of entries in the cache. This is referred to as the reset operation by TinyLfu and keeps
   * the sketch fresh by dividing all counters by two and subtracting based on the number of odd
   * counters found. The O(n) cost of aging is amortized, ideal for hardware prefetching, and uses
   * inexpensive bit manipulations per array location.
   *
   * [1] An Improved Data Stream Summary: The Count-Min Sketch and its Applications
   * http://dimacs.rutgers.edu/~graham/pubs/papers/cm-full.pdf
   * [2] TinyLFU: A Highly Efficient Cache Admission Policy
   * https://dl.acm.org/citation.cfm?id=3149371
   */

  static final long RESET_MASK = 0x7777777777777777L;
  static final long ONE_MASK = 0x1111111111111111L;

  int sampleSize; // table.length*10。一个阈值
  int blockMask; // table.length/8 - 1;
  long[] table; // 存储数据的一维long数组
  int size; // 所有记录的频率统计之和，即每个记录加1，这个size都会加1

  /**
   * Creates a lazily initialized frequency sketch, requiring {@link #ensureCapacity} be called
   * when the maximum size of the cache has been determined.
   */
  @SuppressWarnings("NullAway.Init")
  public FrequencySketch() {}

  /**
   * Initializes and increases the capacity of this <tt>FrequencySketch</tt> instance, if necessary,
   * to ensure that it can accurately estimate the popularity of elements given the maximum size of
   * the cache. This operation forgets all previous counts when resizing.
   *
   * @param maximumSize the maximum size of the cache
   */
  public void ensureCapacity(@NonNegative long maximumSize) {
    requireArgument(maximumSize >= 0);
    int maximum = (int) Math.min(maximumSize, Integer.MAX_VALUE >>> 1);
    if ((table != null) && (table.length >= maximum)) {
      return;
    }

    table = new long[Math.max(Caffeine.ceilingPowerOfTwo(maximum), 8)];
    sampleSize = (maximumSize == 0) ? 10 : (10 * maximum);
    blockMask = (table.length >>> 3) - 1;
    if (sampleSize <= 0) {
      sampleSize = Integer.MAX_VALUE;
    }
    size = 0;
  }

  /**
   * Returns if the sketch has not yet been initialized, requiring that {@link #ensureCapacity} is
   * called before it begins to track frequencies.
   */
  public boolean isNotInitialized() {
    return (table == null);
  }

  /**
   * Returns the estimated number of occurrences of an element, up to the maximum (15).
   *
   * @param e the element to count occurrences of
   * @return the estimated number of occurrences of the element; possibly zero but never negative
   */
  @NonNegative
  public int frequency(E e) {
    if (isNotInitialized()) {
      return 0;
    }

    int[] count = new int[4]; // 4次
    int blockHash = spread(e.hashCode());
    int counterHash = rehash(blockHash);
    int block = (blockHash & blockMask) << 3;
    for (int i = 0; i < 4; i++) {
      int h = counterHash >>> (i << 3);
      int index = (h >>> 1) & 15;
      int offset = h & 1;
      count[i] = (int) ((table[block + offset + (i << 1)] >>> (index << 2)) & 0xfL); // count[i] 赋值为 counter 的计数
    }
    return Math.min(Math.min(count[0], count[1]), Math.min(count[2], count[3])); // 取四个中的较小值
  }

  /**
   * Increments the popularity of the element if it does not exceed the maximum (15). The popularity
   * of all elements will be periodically down sampled when the observed events exceed a threshold.
   * This process provides a frequency aging to allow expired long term entries to fade away.
   *
   * @param e the element to add
   */
  @SuppressWarnings("ShortCircuitBoolean")
  public void increment(E e) {
    if (isNotInitialized()) {
      return;
    }

    int[] index = new int[8];
    int blockHash = spread(e.hashCode()); // 根据 key 的 hashCode 通过一个哈希函数得到一个 hash 值，为什么还要再做一次 hash？怕原来的 hashCode 不够均匀分散，再打散一下
    int counterHash = rehash(blockHash);
    int block = (blockHash & blockMask) << 3;
    for (int i = 0; i < 4; i++) { // Caffeine把一个long的64bit划分成16个等分，每一等分4个bit。
      int h = counterHash >>> (i << 3);
      index[i] = (h >>> 1) & 15; // 0<= index[i] <= 15，定位到 long 中的哪个 counter，表示16个等分的下标 0<= j <= 15，一个long有 64bit，可以存储16个这样的 Counter 统计数
      int offset = h & 1;
      index[i + 4] = block + offset + (i << 1); // index[i+4] = block + offset + (i << 1)，table 数组的下标，定位到哪个 long，每个 long 中有个 16个 counter
    }
    boolean added = // 根据index和start(+1, +2, +3)的值，把table[index]对应的等分追加1
          incrementAt(index[4], index[0])
        | incrementAt(index[5], index[1])
        | incrementAt(index[6], index[2])
        | incrementAt(index[7], index[3]);

    if (added && (++size == sampleSize)) { // 当整体的统计计数 size 达到 sampleSize 时，那么所有记录的频率统计除以2
      reset();
    }
  }

  /** Applies a supplemental hash functions to defends against poor quality hash. */
  int spread(int x) {
    x ^= x >>> 17;
    x *= 0xed5ad4bb;
    x ^= x >>> 11;
    x *= 0xac4c1b51;
    x ^= x >>> 15;
    return x;
  }

  /** Applies another round of hashing for additional randomization. */
  int rehash(int x) {
    x *= 0x31848bab;
    x ^= x >>> 14;
    return x;
  }

  /**
   * Increments the specified counter by 1 if it is not already at the maximum value (15).
   *
   * @param i the table index (16 counters) table 数组的下标，定位到哪个 long，每个 long 中有个 16个 counter
   * @param j the counter to increment 定位到 long 中的哪个 counter，表示16个等分的下标 0<= j <= 15，一个long有 64bit，可以存储16个这样的 Counter 统计数
   * @return if incremented
   */
  boolean incrementAt(int i, int j) {
    int offset = j << 2; // 0<= j <= 63 且是4的倍数，定位到 long 的哪一位，4个一组，应该16份
    long mask = (0xfL << offset); // Caffeine把频率统计最大定为15，即0xfL，mask 就是在64位中的掩码，即1111后面跟 offset 个数的0
    if ((table[i] & mask) != mask) { // 如果&的结果不等于15，那么就追加1。等于15就不会再加了（最多15份 offset 个数的0 ）
      table[i] += (1L << offset); // table[i] 加上 1 后面跟 offset 个数的0（加上1份 offset 个数的0）
      return true;
    }
    return false;
  }

  /** Reduces every counter by half of its original value. */
  void reset() { // 所有记录的频率统计除以2
    int count = 0;
    for (int i = 0; i < table.length; i++) {
      count += Long.bitCount(table[i] & ONE_MASK);
      table[i] = (table[i] >>> 1) & RESET_MASK;
    }
    size = (size - (count >>> 2)) >>> 1;
  }
}

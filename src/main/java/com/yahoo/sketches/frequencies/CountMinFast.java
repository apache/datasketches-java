/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the Apache License 2.0. See LICENSE file
 * at the project root for terms.
 */

package com.yahoo.sketches.frequencies;

import com.yahoo.sketches.hash.MurmurHash3;

//TODO reexamine CU
/**
 * The Count-Min sketch of Cormode and Muthukrishnan is useful for approximately answering point
 * queries, i.e., queries of the form "what is the frequency of key i"? It can also answer other
 * queries as well (range queries, inner product queries, heavy hitters, quantiles, etc.), though it
 * incurs significant overheads for some of these other queries.
 * 
 * The Count-Min algorithm can process deletion of items as well as insertions, since it is a linear
 * sketch. However, using Count-Min to return frequent items in the presence of deletions requires
 * significant overhead. To avoid this overhead, this class only answers point queries; it does have
 * a function for returning frequent items. This class can process deletions as well as insertions.
 * 
 *
 * This implementation also supports the Conservative Update  rule proposed by Estan and Varghese (
 * "New Directions in Traffic Measurement and Accounting: Focusing on the Elephants, Ignoring the Mice"
 * ), which can provide more accurate answers than the update rule in the basic Count-Min sketch.
 * 
 * This implementation uses powers of 2 hash-table sizes and a single-dimensional array for 
 * improved performance.
 * 
 * 
 * 
 * @author Justin8712
 */

// @SuppressWarnings("cast")
public class CountMinFast {
  // hashes denotes the number of cells in the sketcheach key is hashed to
  private int hashes;
  // length denotes the length (i.e., number of cells) of the data structure maintained by CountMin.
  // this implementation will always set length to be a power of 2, to enable fast modulo arithmetic
  private int length;
  // logLength denotes log_2(length)
  private int logLength;
  // arrayMask is used for fast modulo arithmetic
  private int arrayMask;
  // update_sum denotes the sum of all the increments the sketch has processed.
  private long update_sum;
  // counts is the array containing the actual Count-Min data structure
  private long[] counts;
  // keyArr is used for evaluating MurmurHash
  private long[] keyArr = new long[1];
  // eps is a parameter controlling the error guarantees and
  // "frequent threshold" of the answers returned by Count-Min
  private double eps;
  // STRIDE_HASH_BITS and STRIDE_MASK are used for hash function evaluations
  // STRIDE_HASH_BITS is set to log(S), where S is an upper bound on the number
  // of cells in the table.
  private static final int STRIDE_HASH_BITS = 30;
  static final int STRIDE_MASK = (1 << STRIDE_HASH_BITS) - 1;

  /**
   * Constructs and initializes a CountMin sketch, with various optimizations for speed. The
   * guarantee of the sketch is that the answer returned to any individual point query will, with
   * probability at least 1-delta, be accurate to error plus or minus eps*F, where F is the sum of
   * all the increments the sketch has processed.
   * 
   * @param eps Estimates are guaranteed to have error eps*n with probability at least 1-delta,
   *        where n is sum of item frequencies
   * @param delta Estimates are guaranteed to have error eps*n with probability at least 1-delta,
   *        where n is sum of item frequencies
   */
  public CountMinFast(double eps, double delta) {
    if (eps <= 0 || delta <= 0) {
      throw new IllegalArgumentException("Received negative or zero value for eps or delta.");
    }
    this.eps = eps;

    // set this.hashes to be the integer larger than log_2(1/delta)
    this.hashes = (int) (Math.ceil(Math.log(1 / delta) / Math.log(2.0)));

    // set this.length to be the smallest power of two larger than 2/eps,
    // and set this.logLength and this.arrayMask accordingly
    int columns = (int) (2 * Math.ceil(1 / eps));
    this.length = columns * this.hashes;
    this.length = Integer.highestOneBit(2 * (this.length - 1));
    this.logLength = Integer.numberOfTrailingZeros(this.length);
    this.arrayMask = length - 1;

    // if this.length is greater than STRIDE_MASK, this implementation will
    // not be guaranteed to have a stride that is a random odd number between 1
    // and length-1. Hence, raise an exception.
    // if (this.length > STRIDE_MASK){
    // throw new IllegalArgumentException("Sketch size is too large (greater than STRIDE_MASK)");
    // }

    // initialize counts to contain only zeros
    counts = new long[this.length];
    for (int i = 0; i < this.length; i++) {
      counts[i] = 0;
    }

    // initalize update_sum to 0
    this.update_sum = 0;
  }

  /**
   * @param key Process a key (specified as a long) update and treat the increment as 1, using the
   *        update function specified by Cormode and Muthukrishnan
   */
  public void update(long key) {
    update(key, 1);
  }

  /**
   * @param key Process a key (specified as a long) update and treat the increment as 1, using the
   *        conservative_update function that increments each counter to the smallest value still
   *        guaranteed to not underestimate any item's frequency.
   */
  public void conservative_update(long key) {
    conservative_update(key, 1);
  }

  /**
   * @param key
   * @param increment Process a key (specified as a long) and an increment (also specified as a
   *        long). Increment can be negative
   */
  public void update(long key, long increment) {
    // add increment update_sum
    this.update_sum += increment;

    long hash = hash(key);

    // We will use double hashing to determine the cells that key hashes to.
    // Determine probe (i.e., the first cell this key is hashed to)
    // and then make the stride odd and independent of the probe.
    // The first logLength bits of hash are used to determine probe,
    // so the stride will be computed using the higher-order bits of hash.
    int probe = (int) (hash & arrayMask);
    int stride = ((int) ((hash >> logLength) & STRIDE_MASK) << 1) + 1;

    for (int i = this.hashes; i-- > 0;) {
      counts[probe] += increment;
      probe = (probe + stride) & arrayMask;
    }
  }

  /**
   * @param key
   * @param increment Process a key (specified as a long) and an increment (also specified as a
   *        long). Increment can be negative
   */
  public void conservative_update(long key, long increment) {
    // add increment update_sum
    this.update_sum += increment;
    long hash = hash(key);

    // We will use double hashing to determine the cells that key hashes to.
    // Determine probe (i.e., the first cell this key is hashed to)
    // and then make the stride odd and independent of the probe.
    // The first logLength bits of hash are used to determine probe,
    // so the stride will be computed using the higher-order bits of hash.
    int probe = (int) (hash & arrayMask);
    int stride = ((int) ((hash >> logLength) & STRIDE_MASK) << 1) + 1;

    // min_count will store the smallest counter value encountered for this key
    long min_count = Long.MAX_VALUE;
    for (int i = this.hashes; i-- > 0;) {
      if (counts[probe] < min_count) {
        min_count = counts[probe];
      }
      probe = (probe + stride) & arrayMask;
    }

    // now that min_count has been computed, update all counts to the smallest
    // possible value guaranteed not to underestimate the frequency of key.
    probe = (int) (hash & arrayMask);
    for (int i = 0; i < this.hashes; i++) {
      if (counts[probe] < min_count + increment) {
        counts[probe] = min_count + increment;
      }
      probe = (probe + stride) & arrayMask;
    }
  }

  /**
   * @param key to be hashed
   * @return an index into the hash table
   */
  protected long hash(long key) {
    keyArr[0] = key;
    return MurmurHash3.hash(keyArr, 0)[0];
  }

  /**
   * @param key whose count estimate is returned.
   * @return the approximate count for the key. It is guaranteed that with probability at least
   *         1-delta 1) get(key) >= real count 2) get(key) <= real count + getMaxError()
   */
  public long getEstimate(long key) {
    keyArr[0] = key;
    long min_count = Long.MAX_VALUE;

    long hash = hash(key);
    // We will use double hashing to determine the cells that key hashes to.
    // Determine probe (i.e., the first cell this key is hashed to)
    // and then make the stride odd and independent of the probe.
    // The first logLength bits of hash are used to determine probe,
    // so the stride will be computed using the higher-order bits of hash.
    int probe = (int) (hash & arrayMask);
    int stride = ((int) ((hash >> logLength) & STRIDE_MASK) << 1) + 1;

    for (int i = 0; i < this.hashes; i++) {
      if (counts[probe] < min_count) {
        min_count = counts[probe];
      }
      probe = ((probe + stride) & arrayMask);
    }
    return min_count;
  }

  /**
   * @param key whose count estimate is returned.
   * @return an upper bound on the count for the key (upper bound holds deterministically)
   */
  public long getEstimateUpperBound(long key) {
    return getEstimate(key);
  }

  /**
   * @param key whose count estimate is returned.
   * @return a lower bound on the count for the key (lower bound holds with probability at least
   *         1-delta)
   */
  public long getEstimateLowerBound(long key) {
    return getEstimate(key) - getMaxError();
  }

  /**
   * @return a bound on the error of the estimate one gets from get(key). Note that the error is one
   *         sided. if the real count is realCount(key) then get(key) >= realCount(key). The
   *         guarantee of the sketch is that, for any fixed key, with probability at least 1-delta,
   *         realCount(key) is also at most get(key) + getMaxError()
   */
  public long getMaxError() {
    return (long) (Math.ceil(this.eps * this.update_sum));
  }

  /**
   * @param other Another CountMinFE sketch. Must have been created using the same hash functions
   *        and have the same parameter values eps, delta.
   * @return pointer to the sketch resulting in adding the approximate counts of another sketch.
   *         This method does not create a new sketch. The sketch whose function is executed is
   *         changed.
   */
  public CountMinFast merge(CountMinFast other) {
    if (this.hashes != other.hashes || this.length != other.length) {
      throw new IllegalArgumentException(
          "Trying to merge two CountMin data structures of different sizes.");
    }
    for (int i = 0; i < this.length; i++) {
      this.counts[i] += other.counts[i];
    }
    this.update_sum += other.update_sum;
    return this;
  }
}

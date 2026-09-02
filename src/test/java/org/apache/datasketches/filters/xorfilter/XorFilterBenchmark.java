/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.filters.xorfilter;

/**
 * A small standalone benchmark for the XorFilter. It is not a unit test (there is no {@code @Test}
 * method), so it never runs during the normal build; run its {@code main} method directly to
 * measure construction speed, query speed, space usage, and the observed false positive rate for
 * 8-bit and 16-bit fingerprints across a few set sizes. The numbers can be compared against the
 * expectations in Graf and Lemire, "Xor Filters: Faster and Smaller Than Bloom and Cuckoo Filters."
 */
public final class XorFilterBenchmark {

  private XorFilterBenchmark() { }

  private static final int[] BITS = { 8, 16 };
  private static final int[] SIZES = { 1_000_000, 10_000_000 };
  private static final long NEGATIVE_OFFSET = 0x4000_0000_0000_0000L;

  /**
   * Runs the benchmark and prints a results table to standard output.
   * @param args ignored
   */
  public static void main(final String[] args) {
    System.out.println("XorFilter benchmark");
    System.out.printf("%-10s %-6s %-12s %-12s %-12s %-14s%n",
        "keys", "bits", "build ns/key", "query ns/op", "bits/item", "observed fpp");

    for (final int numKeys : SIZES) {
      for (final int bits : BITS) {
        runOne(numKeys, bits);
      }
    }
  }

  // builds one filter and reports construction time, query time, space, and the false positive rate
  private static void runOne(final int numKeys, final int bits) {
    // warm up the JIT on a small filter of the same shape
    measure(50_000, bits);

    final Result r = measure(numKeys, bits);
    System.out.printf("%-10d %-6d %-12.1f %-12.1f %-12.3f %-14.6f%n",
        numKeys, bits, r.buildNanosPerKey, r.queryNanosPerOp, r.bitsPerItem, r.observedFpp);
  }

  private static Result measure(final int numKeys, final int bits) {
    final XorFilterBuilder builder = new XorFilterBuilder(bits, 1L);
    for (long i = 0; i < numKeys; ++i) {
      builder.update(i);
    }

    final long buildStart = System.nanoTime();
    final XorFilter filter = builder.build();
    final long buildElapsed = System.nanoTime() - buildStart;

    // query present keys to time the hot path and confirm there are no false negatives
    long sink = 0;
    final long queryStart = System.nanoTime();
    for (long i = 0; i < numKeys; ++i) {
      if (filter.query(i)) { ++sink; }
    }
    final long queryElapsed = System.nanoTime() - queryStart;
    if (sink != numKeys) {
      throw new IllegalStateException("Unexpected false negative during benchmark");
    }

    // query keys that were never added to observe the false positive rate
    long falsePositives = 0;
    for (long i = 0; i < numKeys; ++i) {
      if (filter.query(NEGATIVE_OFFSET + i)) { ++falsePositives; }
    }

    final Result r = new Result();
    r.buildNanosPerKey = (double) buildElapsed / numKeys;
    r.queryNanosPerOp = (double) queryElapsed / numKeys;
    r.bitsPerItem = filter.getBitsPerItem();
    r.observedFpp = (double) falsePositives / numKeys;
    return r;
  }

  // holds the measured statistics for a single filter
  private static final class Result {
    private double buildNanosPerKey;
    private double queryNanosPerOp;
    private double bitsPerItem;
    private double observedFpp;
  }
}

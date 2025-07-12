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

package org.apache.datasketches.sampling;
import static java.lang.foreign.ValueLayout.JAVA_DOUBLE_UNALIGNED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;

import org.apache.datasketches.common.ArrayOfLongsSerDe2;
import org.apache.datasketches.common.ArrayOfStringsSerDe2;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.sampling.EbppsItemsSketch;
import org.apache.datasketches.sampling.PreambleUtil;
import org.testng.annotations.Test;

public class EbppsItemsSketchTest {
  private static final double EPS = 1e-13;

  static EbppsItemsSketch<Integer> createUnweightedSketch(final int k, final long n) {
    final EbppsItemsSketch<Integer> sk = new EbppsItemsSketch<>(k);
    for (long i = 0; i < n; ++i) {
      sk.update((int) i);
    }
    return sk;
  }

  static <T> void checkIfEqual(final EbppsItemsSketch<T> sk1, final EbppsItemsSketch<T> sk2) {
    assertEquals(sk1.getK(), sk2.getK());
    assertEquals(sk1.getN(), sk2.getN());
    assertEquals(sk1.getC(), sk2.getC());
    assertEquals(sk1.getCumulativeWeight(), sk2.getCumulativeWeight());

    // results may validly differ in size based on presence of partial items
    final ArrayList<T> sample1 = sk1.getResult();
    final ArrayList<T> sample2 = sk2.getResult();

    if (sk1.getC() < 1.0) {
      if ((sample1 != null) && (sample2 != null)) {
        assertEquals(sample1.size(), sample2.size());
        assertEquals(sample1.get(0), sample2.get(0));
      }
      // nothing to test if one is null and the other isn't
    } else {
      // sk1.getC() >= 1.0 and sk2.getC() >= 1.0 (they're equal per above)
      // so the samples shouldn't be null
      assertTrue((sample1 != null) && (sample2 != null));
      final int len = Math.min(sample1.size(), sample2.size());
      for (int i = 0; i < len; ++i) {
        assertEquals(sample1.get(i), sample2.get(i));
      }
      assertTrue(((len == Math.floor(sk1.getC())) || (len == Math.ceil(sk1.getC()))));

      // if c != floor(c) one sketch may not have reached the end,
      // but that's not reliably testable from the external API
    }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkZeroK() {
    new EbppsItemsSketch<String>(0);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkTooBigK() {
    new EbppsItemsSketch<String>(Integer.MAX_VALUE);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkNegativeWeight() {
    final EbppsItemsSketch<String> sk = new EbppsItemsSketch<>(1);
    sk.update("a", -1.0);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInfiniteWeight() {
    final EbppsItemsSketch<String> sk = new EbppsItemsSketch<>(1);
    sk.update("a", Double.POSITIVE_INFINITY);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkNaNWeight() {
    final EbppsItemsSketch<String> sk = new EbppsItemsSketch<>(1);
    sk.update("a", Double.NaN);
  }

  @Test
  public void insertItems() {
    int n = 0;
    final int k = 5;

    // empty sketch
    EbppsItemsSketch<Integer> sk = createUnweightedSketch(k, n);
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), 0);
    assertEquals(sk.getC(), 0.0);
    assertEquals(sk.getCumulativeWeight(), 0.0);
    assertTrue(sk.isEmpty());

    // exact mode
    n = k;
    sk = createUnweightedSketch(k, n);
    assertFalse(sk.isEmpty());
    assertEquals(sk.getN(), n);
    assertEquals(sk.getC(), k);
    assertEquals(sk.getCumulativeWeight(), n);
    assertEquals(sk.getResult().size(), sk.getK());
    for (final Integer val : sk.getResult()) {
      assertTrue(val < n);
    }

    // sampling mode with uniform eights
    n = k * 10;
    sk = createUnweightedSketch(k, n);
    assertFalse(sk.isEmpty());
    assertEquals(sk.getN(), n);
    assertEquals(sk.getCumulativeWeight(), n);
    assertEquals(sk.getC(), k, EPS);
    assertEquals(sk.getResult().size(), sk.getK());
    for (final Integer val : sk.getResult()) {
      assertTrue(val < n);
    }

    // add a very heavy item
    sk.update(n, n);
    assertTrue(sk.getC() < sk.getK());
  }

  @Test
  public void mergeSmallIntoLarge() {
    final int k = 100;

    final EbppsItemsSketch<Integer> sk1 = createUnweightedSketch(k, k);
    final EbppsItemsSketch<Integer> sk2 = new EbppsItemsSketch<>(k / 2);
    sk2.update(-1, k / 10.0); // one heavy item, but less than sk1 weight

    sk1.merge(sk2);
    assertEquals(sk1.getK(), k / 2);
    assertEquals(sk1.getN(), k + 1);
    assertTrue(sk1.getC() < k);
    assertEquals(sk1.getCumulativeWeight(), 1.1 * k, EPS);
  }

  @Test
  public void mergeLargeIntoSmall() {
    final int k = 100;

    final EbppsItemsSketch<Integer> sk1 = new EbppsItemsSketch<>(k / 2);
    sk1.update(-1, k / 4.0);
    sk1.update(-2, k / 8.0);
    final EbppsItemsSketch<Integer> sk2 = createUnweightedSketch(k, k);
    assertEquals(sk2.getN(), k);
    assertEquals(sk2.getC(), k, EPS);

    sk1.merge(sk2);
    assertEquals(sk1.getK(), k / 2);
    assertEquals(sk1.getN(), k + 2);
    assertTrue(sk1.getC() < k);
    // cumulative weight is now (1 + 0.25 + 0.125)k = 1.375k
    assertEquals(sk1.getCumulativeWeight(), 1.375 * k, EPS);
  }

  @Test
  public void serializeDeserializeString() {
    // since C <= k we don't have the usual sketch notion of exact vs estimation
    // mode at any time. The only real serialization cases are empty and non-empty
    // with and without a partial item
    final int k = 10;
    final EbppsItemsSketch<String> sk = new EbppsItemsSketch<>(k);

    // empty
    byte[] bytes = sk.toByteArray(new ArrayOfStringsSerDe2());
    assertEquals(bytes.length, sk.getSerializedSizeBytes(new ArrayOfStringsSerDe2()));
    MemorySegment seg = MemorySegment.ofArray(bytes);
    EbppsItemsSketch<String> sk_heapify = EbppsItemsSketch.heapify(seg, new ArrayOfStringsSerDe2());
    checkIfEqual(sk, sk_heapify);

    // add uniform items
    for (int i = 0; i < k; ++i) {
      sk.update(Integer.toString(i));
    }

    // non-empty, no partial item
    bytes = sk.toByteArray(new ArrayOfStringsSerDe2());
    assertEquals(bytes.length, sk.getSerializedSizeBytes(new ArrayOfStringsSerDe2()));
    seg = MemorySegment.ofArray(bytes);
    sk_heapify = EbppsItemsSketch.heapify(seg, new ArrayOfStringsSerDe2());
    checkIfEqual(sk, sk_heapify);

    // non-empty with partial item
    sk.update(Integer.toString(2 * k), 2.5);
    assertEquals(sk.getCumulativeWeight(), k + 2.5, EPS);
    bytes = sk.toByteArray(new ArrayOfStringsSerDe2());
    assertEquals(bytes.length, sk.getSerializedSizeBytes(new ArrayOfStringsSerDe2()));
    seg = MemorySegment.ofArray(bytes);
    sk_heapify = EbppsItemsSketch.heapify(seg, new ArrayOfStringsSerDe2());
    checkIfEqual(sk, sk_heapify);

    assertNotNull(sk.toString());
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void deserializeZeroK() {
    final EbppsItemsSketch<String> sk = new EbppsItemsSketch<>(5);
    final byte[] bytes = sk.toByteArray(new ArrayOfStringsSerDe2());
    final MemorySegment seg = MemorySegment.ofArray(bytes);
    PreambleUtil.insertK(seg, 0);
    EbppsItemsSketch.heapify(seg, new ArrayOfStringsSerDe2());
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void deserializeTooLargeK() {
    final EbppsItemsSketch<String> sk = new EbppsItemsSketch<>(5);
    final byte[] bytes = sk.toByteArray(new ArrayOfStringsSerDe2());
    final MemorySegment seg = MemorySegment.ofArray(bytes);
    PreambleUtil.insertK(seg, Integer.MAX_VALUE);
    EbppsItemsSketch.heapify(seg, new ArrayOfStringsSerDe2());
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void deserializeBadSerVer() {
    final EbppsItemsSketch<String> sk = new EbppsItemsSketch<>(5);
    final byte[] bytes = sk.toByteArray(new ArrayOfStringsSerDe2());
    final MemorySegment seg = MemorySegment.ofArray(bytes);
    PreambleUtil.insertSerVer(seg, -1);
    EbppsItemsSketch.heapify(seg, new ArrayOfStringsSerDe2());
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void deserializeBadFamily() {
    final EbppsItemsSketch<String> sk = new EbppsItemsSketch<>(5);
    final byte[] bytes = sk.toByteArray(new ArrayOfStringsSerDe2());
    final MemorySegment seg = MemorySegment.ofArray(bytes);
    PreambleUtil.insertFamilyID(seg, 0);
    EbppsItemsSketch.heapify(seg, new ArrayOfStringsSerDe2());
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void deserializeNegativeN() {
    final EbppsItemsSketch<String> sk = new EbppsItemsSketch<>(5);
    for (int i = 0; i < 10; ++i) {
      sk.update(Integer.toString(i));
    }
    final byte[] bytes = sk.toByteArray(new ArrayOfStringsSerDe2());
    final MemorySegment seg = MemorySegment.ofArray(bytes);
    PreambleUtil.insertN(seg, -1000);
    EbppsItemsSketch.heapify(seg, new ArrayOfStringsSerDe2());
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void deserializeNaNCumulativeWeight() {
    final EbppsItemsSketch<String> sk = new EbppsItemsSketch<>(5);
    for (int i = 0; i < 10; ++i) {
      sk.update(Integer.toString(i));
    }
    final byte[] bytes = sk.toByteArray(new ArrayOfStringsSerDe2());
    final MemorySegment seg = MemorySegment.ofArray(bytes);
    PreambleUtil.insertEbppsCumulativeWeight(seg, Double.NaN);
    EbppsItemsSketch.heapify(seg, new ArrayOfStringsSerDe2());
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void deserializeInfiniteMaxWeight() {
    final EbppsItemsSketch<String> sk = new EbppsItemsSketch<>(5);
    for (int i = 0; i < 10; ++i) {
      sk.update(Integer.toString(i));
    }
    final byte[] bytes = sk.toByteArray(new ArrayOfStringsSerDe2());
    final MemorySegment seg = MemorySegment.ofArray(bytes);
    PreambleUtil.insertEbppsMaxWeight(seg, Double.POSITIVE_INFINITY);
    EbppsItemsSketch.heapify(seg, new ArrayOfStringsSerDe2());
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void deserializeNegativeRho() {
    final EbppsItemsSketch<String> sk = new EbppsItemsSketch<>(5);
    for (int i = 0; i < 10; ++i) {
      sk.update(Integer.toString(i));
    }
    final byte[] bytes = sk.toByteArray(new ArrayOfStringsSerDe2());
    final MemorySegment seg = MemorySegment.ofArray(bytes);
    PreambleUtil.insertEbppsRho(seg, -0.1);
    EbppsItemsSketch.heapify(seg, new ArrayOfStringsSerDe2());
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void deserializeNegativeC() {
    final EbppsItemsSketch<String> sk = new EbppsItemsSketch<>(5);
    for (int i = 0; i < 10; ++i) {
      sk.update(Integer.toString(i));
    }
    final byte[] bytes = sk.toByteArray(new ArrayOfStringsSerDe2());
    final MemorySegment seg = MemorySegment.ofArray(bytes);
    seg.set(JAVA_DOUBLE_UNALIGNED, 40, -2.0); // from the defined spec
    EbppsItemsSketch.heapify(seg, new ArrayOfStringsSerDe2());
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void deserializeTooShort() {
    final EbppsItemsSketch<Long> sk = new EbppsItemsSketch<>(5);
    for (long i = 0; i < 10; ++i) {
      sk.update(i);
    }
    final byte[] bytes = sk.toByteArray(new ArrayOfLongsSerDe2());
    final MemorySegment seg = MemorySegment.ofArray(bytes);
    final MemorySegment shortSeg = seg.asSlice(0, seg.byteSize() - 1);
    EbppsItemsSketch.heapify(shortSeg, new ArrayOfLongsSerDe2());
  }
}

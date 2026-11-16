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


package org.apache.datasketches.tdigest;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.lang.foreign.MemorySegment;

import org.testng.annotations.Test;

/**
 * Checks that a t-Digest allocates its storage on first use and grows it as values arrive,
 * or allocates it up front with ensureCapacity(), with the same results either way.
 */
public class TDigestLazyAllocationTest {
  private static final short K = 100;
  private static final int FULL_CENTROIDS = (2 * K) + 10;
  private static final int FULL_BUFFER = 4 * FULL_CENTROIDS;

  private static double value(final int i) { return (i * 7919) % 100_003; } //not sorted, deterministic

  private static void assertAllocated(final TDigestDouble td, final int centroids, final int buffer) {
    assertEquals(td.getAllocatedCentroids(), centroids, "centroids");
    assertEquals(td.getAllocatedBuffer(), buffer, "buffer");
  }

  @Test
  public void emptyDigestHoldsNoStorage() {
    final TDigestDouble td = new TDigestDouble(K);
    assertAllocated(td, 0, 0);
    td.update(Double.NaN); //ignored, so not a first use
    td.update(Double.POSITIVE_INFINITY);
    td.merge(new TDigestDouble(K)); //merging an empty t-Digest is not a first use
    assertTrue(td.isEmpty());
    td.toString(true);
    final TDigestDouble reserved = new TDigestDouble(K);
    reserved.ensureCapacity();
    assertEquals(td.toByteArray(), reserved.toByteArray());
    assertAllocated(td, 0, 0);
    final TDigestDouble copy = TDigestDouble.heapify(MemorySegment.ofArray(td.toByteArray()));
    assertAllocated(copy, 0, 0);
  }

  @Test
  public void bufferGrowsGeometrically() {
    final TDigestDouble td = new TDigestDouble(K);
    td.update(1);
    assertAllocated(td, 0, 8);
    for (int i = 2; i <= 9; i++) { td.update(i); }
    assertAllocated(td, 0, 16);
    for (int i = 10; i <= FULL_BUFFER; i++) { td.update(i); }
    assertEquals(td.getAllocatedBuffer(), FULL_BUFFER); //capped at the full size
    assertEquals(td.getAllocatedCentroids(), 0); //nothing compressed yet
    td.update(0); //the buffer is full, so this compresses
    assertEquals(td.getAllocatedBuffer(), FULL_BUFFER);
    assertTrue(td.getAllocatedCentroids() > 0);
    assertTrue(td.getAllocatedCentroids() <= FULL_CENTROIDS);
  }

  @Test
  public void smallDigestNeverAllocatesFullStorage() {
    final TDigestDouble td = new TDigestDouble(K);
    for (int i = 0; i < 100; i++) { td.update(value(i)); }
    assertEquals(td.getAllocatedBuffer(), 128);
    td.getRank(50_000); //compresses
    assertTrue(td.getAllocatedCentroids() <= 100, "centroids " + td.getAllocatedCentroids());
    assertTrue(td.getAllocatedCentroids() < FULL_CENTROIDS);
  }

  @Test
  public void ensureCapacityAllocatesFullStorage() {
    final TDigestDouble td = new TDigestDouble(K);
    td.ensureCapacity();
    assertAllocated(td, FULL_CENTROIDS, FULL_BUFFER);
    td.ensureCapacity(); //no effect once allocated
    assertAllocated(td, FULL_CENTROIDS, FULL_BUFFER);
    final TDigestDouble lazy = new TDigestDouble(K);
    for (int i = 0; i < 100_000; i++) { td.update(value(i)); lazy.update(value(i)); }
    assertAllocated(td, FULL_CENTROIDS, FULL_BUFFER); //the storage never grew
    assertEquals(td.toByteArray(), lazy.toByteArray());
    assertEquals(td.getQuantile(0.5), lazy.getQuantile(0.5));
  }

  @Test
  public void ensureCapacityAfterUpdatesKeepsContents() {
    final TDigestDouble td = new TDigestDouble(K);
    final TDigestDouble ref = new TDigestDouble(K);
    for (int i = 0; i < 1000; i++) { td.update(value(i)); ref.update(value(i)); }
    final String before = td.toString(true).replaceAll("\\(allocated \\d+\\)", "");
    td.ensureCapacity();
    assertEquals(td.toString(true).replaceAll("\\(allocated \\d+\\)", ""), before); //buffered values included
    assertAllocated(td, FULL_CENTROIDS, FULL_BUFFER);
    for (int i = 1000; i < 50_000; i++) { td.update(value(i)); ref.update(value(i)); }
    assertEquals(td.toByteArray(), ref.toByteArray());
  }

  @Test
  public void mergeIntoNewDigest() {
    final TDigestDouble other = new TDigestDouble(K);
    for (int i = 0; i < 10_000; i++) { other.update(value(i)); }
    final TDigestDouble lazy = new TDigestDouble(K);
    final TDigestDouble reserved = new TDigestDouble(K);
    reserved.ensureCapacity();
    lazy.merge(other);
    reserved.merge(other);
    assertTrue(lazy.getAllocatedCentroids() > 0);
    assertEquals(lazy.getAllocatedBuffer(), 0); //a merge does not need the buffer
    assertEquals(lazy.toByteArray(), reserved.toByteArray());
    assertEquals(lazy.getMinValue(), other.getMinValue());
    assertEquals(lazy.getMaxValue(), other.getMaxValue());

    final TDigestDouble single = new TDigestDouble(K);
    single.update(42);
    final TDigestDouble fromSingle = new TDigestDouble(K);
    fromSingle.merge(single);
    assertEquals(fromSingle.toByteArray(), single.toByteArray());
  }

  @Test
  public void heapifiedDigestIsSizedToItsImage() {
    final TDigestDouble src = new TDigestDouble(K);
    for (int i = 0; i < 10_000; i++) { src.update(value(i)); }
    final byte[] image = src.toByteArray();
    final TDigestDouble td = TDigestDouble.heapify(MemorySegment.ofArray(image));
    final TDigestDouble reserved = TDigestDouble.heapify(MemorySegment.ofArray(image));
    reserved.ensureCapacity();
    assertEquals(td.getAllocatedBuffer(), 0);
    assertTrue(td.getAllocatedCentroids() < FULL_CENTROIDS);
    for (int i = 0; i < 20_000; i++) { td.update(value(i) + 0.5); reserved.update(value(i) + 0.5); }
    td.merge(src);
    reserved.merge(src);
    assertEquals(td.toByteArray(), reserved.toByteArray());
  }
}

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

package org.apache.datasketches.tuple.arrayofdoubles;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.SketchesArgumentException;
import org.testng.Assert;
import org.testng.annotations.Test;

public class HeapArrayOfDoublesCompactSketchTest {

  @Test
  public void emptyFromQuickSelectSketch() {
    ArrayOfDoublesUpdatableSketch us = new ArrayOfDoublesUpdatableSketchBuilder().build();
    ArrayOfDoublesCompactSketch sketch = us.compact();
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertFalse(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getEstimate(), 0.0);
    Assert.assertEquals(sketch.getLowerBound(1), 0.0);
    Assert.assertEquals(sketch.getUpperBound(1), 0.0);
    Assert.assertEquals(sketch.getRetainedEntries(), 0);
    Assert.assertEquals(sketch.getThetaLong(), Long.MAX_VALUE);
    Assert.assertEquals(sketch.getTheta(), 1.0);
    Assert.assertNotNull(sketch.getValues());
    Assert.assertEquals(sketch.getValues().length, 0);
    ArrayOfDoublesSketchIterator it = sketch.iterator();
    while (it.next()) {
      Assert.fail("empty sketch expected");
    }
  }

  @Test
  public void exactModeFromQuickSelectSketch() {
    ArrayOfDoublesUpdatableSketch us = new ArrayOfDoublesUpdatableSketchBuilder().build();
    us.update(1, new double[] {1.0});
    us.update(2, new double[] {1.0});
    us.update(3, new double[] {1.0});
    us.update(1, new double[] {1.0});
    us.update(2, new double[] {1.0});
    us.update(3, new double[] {1.0});
    ArrayOfDoublesCompactSketch sketch = us.compact();
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertFalse(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getEstimate(), 3.0);
    Assert.assertEquals(sketch.getLowerBound(1), 3.0);
    Assert.assertEquals(sketch.getUpperBound(1), 3.0);
    Assert.assertEquals(sketch.getRetainedEntries(), 3);
    Assert.assertEquals(sketch.getThetaLong(), Long.MAX_VALUE);
    Assert.assertEquals(sketch.getTheta(), 1.0);
    double[][] values = sketch.getValues();
    Assert.assertEquals(values.length, 3);
    for (double[] array: values) {
      Assert.assertEquals(array[0], 2.0);
    }
  }

  @Test
  public void serializeDeserializeSmallExact() {
    ArrayOfDoublesUpdatableSketch us = new ArrayOfDoublesUpdatableSketchBuilder().build();
    us.update("a", new double[] {1.0});
    us.update("b", new double[] {1.0});
    us.update("c", new double[] {1.0});
    ArrayOfDoublesCompactSketch sketch1 = us.compact();
    ArrayOfDoublesSketch sketch2 =
        ArrayOfDoublesSketch.heapifySketch(MemorySegment.ofArray(sketch1.toByteArray()));
    Assert.assertFalse(sketch2.isEmpty());
    Assert.assertFalse(sketch2.isEstimationMode());
    Assert.assertEquals(sketch2.getEstimate(), 3.0);
    Assert.assertEquals(sketch2.getLowerBound(1), 3.0);
    Assert.assertEquals(sketch2.getUpperBound(1), 3.0);
    Assert.assertEquals(sketch2.getRetainedEntries(), 3);
    Assert.assertEquals(sketch2.getThetaLong(), Long.MAX_VALUE);
    Assert.assertEquals(sketch2.getTheta(), 1.0);
    double[][] values = sketch2.getValues();
    Assert.assertEquals(values.length, 3);
    for (double[] array: values) {
      Assert.assertEquals(array[0], 1.0);
    }
  }

  @Test
  public void serializeDeserializeEstimation() {
    ArrayOfDoublesUpdatableSketch us = new ArrayOfDoublesUpdatableSketchBuilder().build();
    for (int i = 0; i < 8192; i++) {
      us.update(i, new double[] {1.0});
    }
    MemorySegment wseg = MemorySegment.ofArray(us.toByteArray());
    ArrayOfDoublesUpdatableSketch wrappedUS = ArrayOfDoublesSketch.wrapUpdatableSketch(wseg);
    Assert.assertFalse(wrappedUS.isEmpty());
    Assert.assertTrue(wrappedUS.isEstimationMode());
    Assert.assertEquals(wrappedUS.getEstimate(), us.getEstimate());
    Assert.assertEquals(wrappedUS.getThetaLong(), us.getThetaLong());

    ArrayOfDoublesUpdatableSketch heapUS = ArrayOfDoublesSketch.heapifyUpdatableSketch(wseg);
    Assert.assertFalse(heapUS.isEmpty());
    Assert.assertTrue(heapUS.isEstimationMode());
    Assert.assertEquals(heapUS.getEstimate(), us.getEstimate());
    Assert.assertEquals(heapUS.getThetaLong(), us.getThetaLong());

    ArrayOfDoublesCompactSketch sketch1 = us.compact();
    ArrayOfDoublesSketch sketch2 =
        ArrayOfDoublesSketch.heapifySketch(MemorySegment.ofArray(sketch1.toByteArray()));
    Assert.assertFalse(sketch2.isEmpty());
    Assert.assertTrue(sketch2.isEstimationMode());
    Assert.assertEquals(sketch2.getEstimate(), sketch1.getEstimate());
    Assert.assertEquals(sketch2.getThetaLong(), sketch1.getThetaLong());
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void deserializeWithWrongSeed() {
    ArrayOfDoublesUpdatableSketch us = new ArrayOfDoublesUpdatableSketchBuilder().build();
    for (int i = 0; i < 8192; i++) {
      us.update(i, new double[] {1.0});
    }
    ArrayOfDoublesCompactSketch sketch1 = us.compact();
    MemorySegment seg = MemorySegment.ofArray(sketch1.toByteArray());
    ArrayOfDoublesSketch.heapifySketch(seg, 123);
  }
  @Test
  public void emptyImageSeedHashIsZeroAndIgnoredOnRead() {
    final long seed = 12345;
    final ArrayOfDoublesUpdatableSketch us = new ArrayOfDoublesUpdatableSketchBuilder().setSeed(seed).build();
    final byte[] heapBytes = us.compact().toByteArray();
    final byte[] directBytes = new byte[heapBytes.length + 8]; // the direct writer asks for room for one entry
    us.compact(MemorySegment.ofArray(directBytes));
    Assert.assertEquals(java.util.Arrays.copyOf(directBytes, heapBytes.length), heapBytes);
    Assert.assertEquals(MemorySegment.ofArray(heapBytes).get(java.lang.foreign.ValueLayout.JAVA_SHORT_UNALIGNED,
        ArrayOfDoublesSketch.SEED_HASH_SHORT), (short) 0);

    // a historical empty image may carry a seed hash; it is ignored on read
    final byte[] historical = heapBytes.clone();
    MemorySegment.ofArray(historical).set(java.lang.foreign.ValueLayout.JAVA_SHORT_UNALIGNED,
        ArrayOfDoublesSketch.SEED_HASH_SHORT, (short) 0x1234);
    for (final byte[] bytes : new byte[][] {heapBytes, historical}) {
      final ArrayOfDoublesSketch heap = new HeapArrayOfDoublesCompactSketch(MemorySegment.ofArray(bytes), seed);
      final ArrayOfDoublesSketch direct = new DirectArrayOfDoublesCompactSketch(MemorySegment.ofArray(bytes), seed);
      Assert.assertEquals(heap.toByteArray(), heapBytes); // re-serializing writes a zero seed hash
      for (final ArrayOfDoublesSketch empty : new ArrayOfDoublesSketch[] {heap, direct}) {
        Assert.assertTrue(empty.isEmpty());

        // set operations ignore the seed hash of an empty input
        final ArrayOfDoublesUpdatableSketch other = new ArrayOfDoublesUpdatableSketchBuilder().setSeed(seed).build();
        other.update(1, new double[] {1.0});
        final ArrayOfDoublesUnion union = new ArrayOfDoublesSetOperationBuilder().setSeed(seed).buildUnion();
        union.union(empty);
        union.union(other);
        Assert.assertEquals(union.getResult().getEstimate(), 1.0);
        final ArrayOfDoublesIntersection inter = new ArrayOfDoublesSetOperationBuilder().setSeed(seed).buildIntersection();
        inter.intersect(empty, null);
        Assert.assertTrue(inter.getResult().isEmpty());
        final ArrayOfDoublesAnotB aNotB = new ArrayOfDoublesSetOperationBuilder().setSeed(seed).buildAnotB();
        aNotB.update(other, empty);
        Assert.assertEquals(aNotB.getResult().getEstimate(), 1.0);
        aNotB.update(empty, other);
        Assert.assertTrue(aNotB.getResult().isEmpty());
      }
    }

    // a non-empty image still has its seed hash checked
    us.update(1, new double[] {1.0});
    final byte[] withEntry = us.compact().toByteArray();
    Assert.assertThrows(SketchesArgumentException.class,
        () -> new HeapArrayOfDoublesCompactSketch(MemorySegment.ofArray(withEntry), seed + 1));
    Assert.assertThrows(SketchesArgumentException.class,
        () -> new DirectArrayOfDoublesCompactSketch(MemorySegment.ofArray(withEntry), seed + 1));
  }
}

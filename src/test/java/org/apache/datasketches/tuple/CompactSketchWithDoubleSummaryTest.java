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

package org.apache.datasketches.tuple;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.tuple.CompactTupleSketch;
import org.apache.datasketches.tuple.TupleSketch;
import org.apache.datasketches.tuple.TupleSketchIterator;
import org.apache.datasketches.tuple.UpdatableTupleSketch;
import org.apache.datasketches.tuple.UpdatableTupleSketchBuilder;
import org.apache.datasketches.tuple.adouble.DoubleSummary;
import org.apache.datasketches.tuple.adouble.DoubleSummaryDeserializer;
import org.apache.datasketches.tuple.adouble.DoubleSummaryFactory;
import org.apache.datasketches.tuple.adouble.DoubleSummary.Mode;
import org.testng.Assert;
import org.testng.annotations.Test;

public class CompactSketchWithDoubleSummaryTest {
  private final DoubleSummary.Mode mode = Mode.Sum;

  @Test
  public void emptyFromNonPublicConstructorNullArray() {
    CompactTupleSketch<DoubleSummary> sketch =
        new CompactTupleSketch<>(null, null, Long.MAX_VALUE, true);
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertFalse(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getEstimate(), 0.0);
    Assert.assertEquals(sketch.getLowerBound(1), 0.0);
    Assert.assertEquals(sketch.getUpperBound(1), 0.0);
    Assert.assertEquals(sketch.getRetainedEntries(), 0);
    Assert.assertEquals(sketch.getThetaLong(), Long.MAX_VALUE);
    Assert.assertEquals(sketch.getTheta(), 1.0);
    TupleSketchIterator<DoubleSummary> it = sketch.iterator();
    Assert.assertNotNull(it);
    Assert.assertFalse(it.next());
    sketch.toString();
  }

  @Test
  public void emptyFromNonPublicConstructor() {
    long[] keys = new long[0];
    DoubleSummary[] summaries =
        (DoubleSummary[]) java.lang.reflect.Array.newInstance(DoubleSummary.class, 0);
    CompactTupleSketch<DoubleSummary> sketch =
        new CompactTupleSketch<>(keys, summaries, Long.MAX_VALUE, true);
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertFalse(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getEstimate(), 0.0);
    Assert.assertEquals(sketch.getLowerBound(1), 0.0);
    Assert.assertEquals(sketch.getUpperBound(1), 0.0);
    Assert.assertEquals(sketch.getRetainedEntries(), 0);
    Assert.assertEquals(sketch.getThetaLong(), Long.MAX_VALUE);
    Assert.assertEquals(sketch.getTheta(), 1.0);
    TupleSketchIterator<DoubleSummary> it = sketch.iterator();
    Assert.assertNotNull(it);
    Assert.assertFalse(it.next());
  }

  @Test
  public void emptyFromQuickSelectSketch() {
    UpdatableTupleSketch<Double, DoubleSummary> us =
        new UpdatableTupleSketchBuilder<>(new DoubleSummaryFactory(mode)).build();
    CompactTupleSketch<DoubleSummary> sketch = us.compact();
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertFalse(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getEstimate(), 0.0);
    Assert.assertEquals(sketch.getLowerBound(1), 0.0);
    Assert.assertEquals(sketch.getUpperBound(1), 0.0);
    Assert.assertEquals(sketch.getRetainedEntries(), 0);
    Assert.assertEquals(sketch.getThetaLong(), Long.MAX_VALUE);
    Assert.assertEquals(sketch.getTheta(), 1.0);
    TupleSketchIterator<DoubleSummary> it = sketch.iterator();
    Assert.assertNotNull(it);
    Assert.assertFalse(it.next());
  }

  @Test
  public void exactModeFromQuickSelectSketch() {
    UpdatableTupleSketch<Double, DoubleSummary> us =
        new UpdatableTupleSketchBuilder<>(new DoubleSummaryFactory(mode)).build();
    us.update(1, 1.0);
    us.update(2, 1.0);
    us.update(3, 1.0);
    us.update(1, 1.0);
    us.update(2, 1.0);
    us.update(3, 1.0);
    CompactTupleSketch<DoubleSummary> sketch = us.compact();
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertFalse(sketch.isEstimationMode());
    Assert.assertEquals(sketch.getEstimate(), 3.0);
    Assert.assertEquals(sketch.getLowerBound(1), 3.0);
    Assert.assertEquals(sketch.getUpperBound(1), 3.0);
    Assert.assertEquals(sketch.getRetainedEntries(), 3);
    Assert.assertEquals(sketch.getThetaLong(), Long.MAX_VALUE);
    Assert.assertEquals(sketch.getTheta(), 1.0);
    TupleSketchIterator<DoubleSummary> it = sketch.iterator();
    int count = 0;
    while (it.next()) {
     Assert.assertEquals(it.getSummary().getValue(), 2.0);
     count++;
    }
    Assert.assertEquals(count, 3);
  }

  @Test
  public void serializeDeserializeSmallExact() {
    UpdatableTupleSketch<Double, DoubleSummary> us =
        new UpdatableTupleSketchBuilder<>(new DoubleSummaryFactory(mode)).build();
    us.update("a", 1.0);
    us.update("b", 1.0);
    us.update("c", 1.0);
    CompactTupleSketch<DoubleSummary> sketch1 = us.compact();
    TupleSketch<DoubleSummary> sketch2 =
        TupleSketch.heapifySketch(MemorySegment.ofArray(sketch1.toByteArray()),
            new DoubleSummaryDeserializer());
    Assert.assertFalse(sketch2.isEmpty());
    Assert.assertFalse(sketch2.isEstimationMode());
    Assert.assertEquals(sketch2.getEstimate(), 3.0);
    Assert.assertEquals(sketch2.getLowerBound(1), 3.0);
    Assert.assertEquals(sketch2.getUpperBound(1), 3.0);
    Assert.assertEquals(sketch2.getRetainedEntries(), 3);
    Assert.assertEquals(sketch2.getThetaLong(), Long.MAX_VALUE);
    Assert.assertEquals(sketch2.getTheta(), 1.0);
    TupleSketchIterator<DoubleSummary> it = sketch2.iterator();
    int count = 0;
    while (it.next()) {
     Assert.assertEquals(it.getSummary().getValue(), 1.0);
     count++;
    }
    Assert.assertEquals(count, 3);
  }

  @Test
  public void serializeDeserializeEstimation() throws Exception {
    UpdatableTupleSketch<Double, DoubleSummary> us =
        new UpdatableTupleSketchBuilder<>(new DoubleSummaryFactory(mode)).build();
    for (int i = 0; i < 8192; i++) {
      us.update(i, 1.0);
    }
    us.trim();
    CompactTupleSketch<DoubleSummary> sketch1 = us.compact();
    byte[] bytes = sketch1.toByteArray();

    // for binary testing
    //TestUtil.writeBytesToFile(bytes, "CompactSketchWithDoubleSummary4K.sk");

    TupleSketch<DoubleSummary> sketch2 =
        TupleSketch.heapifySketch(MemorySegment.ofArray(bytes), new DoubleSummaryDeserializer());
    Assert.assertFalse(sketch2.isEmpty());
    Assert.assertTrue(sketch2.isEstimationMode());
    Assert.assertEquals(sketch2.getEstimate(), sketch1.getEstimate());
    Assert.assertEquals(sketch2.getThetaLong(), sketch1.getThetaLong());
    TupleSketchIterator<DoubleSummary> it = sketch2.iterator();
    int count = 0;
    while (it.next()) {
      Assert.assertEquals(it.getSummary().getValue(), 1.0);
      count++;
    }
    Assert.assertEquals(count, 4096);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void deserializeWrongType() {
    UpdatableTupleSketch<Double, DoubleSummary> us =
        new UpdatableTupleSketchBuilder<>(new DoubleSummaryFactory(mode)).build();
    for (int i = 0; i < 8192; i++) {
      us.update(i, 1.0);
    }
    CompactTupleSketch<DoubleSummary> sketch1 = us.compact();
    TupleSketch.heapifyUpdatableSketch(MemorySegment.ofArray(sketch1.toByteArray()),
        new DoubleSummaryDeserializer(),
        new DoubleSummaryFactory(mode));
  }

}

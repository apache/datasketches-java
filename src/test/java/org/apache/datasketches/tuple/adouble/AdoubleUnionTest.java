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

package org.apache.datasketches.tuple.adouble;

import static org.testng.Assert.fail;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.theta.UpdatableThetaSketch;
import org.apache.datasketches.theta.UpdatableThetaSketchBuilder;
import org.apache.datasketches.tuple.CompactTupleSketch;
import org.apache.datasketches.tuple.TupleSketchIterator;
import org.apache.datasketches.tuple.TupleUnion;
import org.apache.datasketches.tuple.UpdatableTupleSketch;
import org.apache.datasketches.tuple.UpdatableTupleSketchBuilder;
import org.apache.datasketches.tuple.adouble.DoubleSummary;
import org.apache.datasketches.tuple.adouble.DoubleSummaryFactory;
import org.apache.datasketches.tuple.adouble.DoubleSummarySetOperations;
import org.apache.datasketches.tuple.adouble.DoubleSummary.Mode;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class AdoubleUnionTest {
  private final DoubleSummary.Mode mode = Mode.Sum;

  @Test
  public void unionEmptySampling() {
    final UpdatableTupleSketch<Double, DoubleSummary> sketch =
        new UpdatableTupleSketchBuilder<>(new DoubleSummaryFactory(mode)).setSamplingProbability(0.01f).build();
    sketch.update(1, 1.0);
    Assert.assertEquals(sketch.getRetainedEntries(), 0); // not retained due to low sampling probability

    final TupleUnion<DoubleSummary> union = new TupleUnion<>(new DoubleSummarySetOperations(mode, mode));
    union.union(sketch);
    final CompactTupleSketch<DoubleSummary> result = union.getResult();
    Assert.assertEquals(result.getRetainedEntries(), 0);
    Assert.assertFalse(result.isEmpty());
    Assert.assertTrue(result.isEstimationMode());
    Assert.assertEquals(result.getEstimate(), 0.0);
  }

  @Test
  public void unionExactMode() {
    final UpdatableTupleSketch<Double, DoubleSummary> sketch1 =
        new UpdatableTupleSketchBuilder<>(new DoubleSummaryFactory(mode)).build();
    sketch1.update(1, 1.0);
    sketch1.update(1, 1.0);
    sketch1.update(1, 1.0);
    sketch1.update(2, 1.0);

    final UpdatableTupleSketch<Double, DoubleSummary> sketch2 =
        new UpdatableTupleSketchBuilder<>(new DoubleSummaryFactory(mode)).build();
    sketch2.update(2, 1.0);
    sketch2.update(2, 1.0);
    sketch2.update(3, 1.0);
    sketch2.update(3, 1.0);
    sketch2.update(3, 1.0);

    final TupleUnion<DoubleSummary> union = new TupleUnion<>(new DoubleSummarySetOperations(mode, mode));
    union.union(sketch1);
    union.union(sketch2);
    CompactTupleSketch<DoubleSummary> result = union.getResult();
    Assert.assertEquals(result.getEstimate(), 3.0);

    final TupleSketchIterator<DoubleSummary> it = result.iterator();
    Assert.assertTrue(it.next());
    Assert.assertEquals(it.getSummary().getValue(), 3.0);
    Assert.assertTrue(it.next());
    Assert.assertEquals(it.getSummary().getValue(), 3.0);
    Assert.assertTrue(it.next());
    Assert.assertEquals(it.getSummary().getValue(), 3.0);
    Assert.assertFalse(it.next());

    union.reset();
    result = union.getResult();
    Assert.assertEquals(result.getRetainedEntries(), 0);
    Assert.assertTrue(result.isEmpty());
    Assert.assertFalse(result.isEstimationMode());
    Assert.assertEquals(result.getEstimate(), 0.0);
    Assert.assertEquals(result.getLowerBound(1), 0.0);
    Assert.assertEquals(result.getUpperBound(1), 0.0);
    Assert.assertEquals(result.getTheta(), 1.0);
  }

  @Test
  public void unionEstimationMode() {
    int key = 0;
    final UpdatableTupleSketch<Double, DoubleSummary> sketch1 =
        new UpdatableTupleSketchBuilder<>(new DoubleSummaryFactory(mode)).build();
    for (int i = 0; i < 8192; i++) {
      sketch1.update(key++, 1.0);
    }

    key -= 4096; // overlap half of the entries
    final UpdatableTupleSketch<Double, DoubleSummary> sketch2 =
        new UpdatableTupleSketchBuilder<>(new DoubleSummaryFactory(mode)).build();
    for (int i = 0; i < 8192; i++) {
      sketch2.update(key++, 1.0);
    }

    final TupleUnion<DoubleSummary> union = new TupleUnion<>(4096, new DoubleSummarySetOperations(mode, mode));
    union.union(sketch1);
    union.union(sketch2);
    final CompactTupleSketch<DoubleSummary> result = union.getResult();
    Assert.assertEquals(result.getEstimate(), 12288.0, 12288 * 0.01);
    Assert.assertTrue(result.getLowerBound(1) <= result.getEstimate());
    Assert.assertTrue(result.getUpperBound(1) > result.getEstimate());
  }

  @Test
  public void unionMixedMode() {
    int key = 0;
    final UpdatableTupleSketch<Double, DoubleSummary> sketch1 =
        new UpdatableTupleSketchBuilder<>(new DoubleSummaryFactory(mode)).build();
    for (int i = 0; i < 1000; i++) {
      sketch1.update(key++, 1.0);
      //System.out.println("theta1=" + sketch1.getTheta() + " " + sketch1.getThetaLong());
    }

    key -= 500; // overlap half of the entries
    final UpdatableTupleSketch<Double, DoubleSummary> sketch2 =
        new UpdatableTupleSketchBuilder<>
          (new DoubleSummaryFactory(mode)).setSamplingProbability(0.2f).build();
    for (int i = 0; i < 20000; i++) {
      sketch2.update(key++, 1.0);
      //System.out.println("theta2=" + sketch2.getTheta() + " " + sketch2.getThetaLong());
    }

    final TupleUnion<DoubleSummary> union = new TupleUnion<>(4096, new DoubleSummarySetOperations(mode, mode));
    union.union(sketch1);
    union.union(sketch2);
    final CompactTupleSketch<DoubleSummary> result = union.getResult();
    Assert.assertEquals(result.getEstimate(), 20500.0, 20500 * 0.01);
    Assert.assertTrue(result.getLowerBound(1) <= result.getEstimate());
    Assert.assertTrue(result.getUpperBound(1) > result.getEstimate());
  }

  @Test
  public void checkUnionUpdateWithTheta() {
    final TupleUnion<DoubleSummary> union = new TupleUnion<>(new DoubleSummarySetOperations(mode, mode));
    UpdatableThetaSketch usk = null;
    DoubleSummary dsum = null;

    try { union.union(usk, dsum); fail(); }
    catch (final SketchesArgumentException e) { }

    usk = new UpdatableThetaSketchBuilder().build();
    try { union.union(usk, dsum); fail(); }
    catch (final SketchesArgumentException e) { }

    dsum = new DoubleSummaryFactory(mode).newSummary();
    for (int i = 0; i < 10; i++) { usk.update(i); }
    union.union(usk, dsum);
    Assert.assertEquals(union.getResult().getEstimate(), 10.0);
  }

}

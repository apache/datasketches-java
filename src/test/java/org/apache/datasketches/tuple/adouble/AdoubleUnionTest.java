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

import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.datasketches.theta.UpdateSketchBuilder;
import org.apache.datasketches.tuple.CompactSketch;
import org.apache.datasketches.tuple.SketchIterator;
import org.apache.datasketches.tuple.Union;
import org.apache.datasketches.tuple.UpdatableSketch;
import org.apache.datasketches.tuple.UpdatableSketchBuilder;
import org.apache.datasketches.tuple.adouble.DoubleSummary.Mode;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
@SuppressWarnings("javadoc")
public class AdoubleUnionTest {
  private final DoubleSummary.Mode mode = Mode.Sum;

  @Test
  public void unionEmptySampling() {
    UpdatableSketch<Double, DoubleSummary> sketch =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory(mode)).setSamplingProbability(0.01f).build();
    sketch.update(1, 1.0);
    Assert.assertEquals(sketch.getRetainedEntries(), 0); // not retained due to low sampling probability

    Union<DoubleSummary> union = new Union<>(new DoubleSummarySetOperations(mode, mode));
    union.update(sketch);
    CompactSketch<DoubleSummary> result = union.getResult();
    Assert.assertEquals(result.getRetainedEntries(), 0);
    Assert.assertFalse(result.isEmpty());
    Assert.assertTrue(result.isEstimationMode());
    Assert.assertEquals(result.getEstimate(), 0.0);
  }

  @Test
  public void unionExactMode() {
    UpdatableSketch<Double, DoubleSummary> sketch1 =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory(mode)).build();
    sketch1.update(1, 1.0);
    sketch1.update(1, 1.0);
    sketch1.update(1, 1.0);
    sketch1.update(2, 1.0);

    UpdatableSketch<Double, DoubleSummary> sketch2 =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory(mode)).build();
    sketch2.update(2, 1.0);
    sketch2.update(2, 1.0);
    sketch2.update(3, 1.0);
    sketch2.update(3, 1.0);
    sketch2.update(3, 1.0);

    Union<DoubleSummary> union = new Union<>(new DoubleSummarySetOperations(mode, mode));
    union.update(sketch1);
    union.update(sketch2);
    CompactSketch<DoubleSummary> result = union.getResult();
    Assert.assertEquals(result.getEstimate(), 3.0);

    SketchIterator<DoubleSummary> it = result.iterator();
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
    UpdatableSketch<Double, DoubleSummary> sketch1 =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory(mode)).build();
    for (int i = 0; i < 8192; i++) {
      sketch1.update(key++, 1.0);
    }

    key -= 4096; // overlap half of the entries
    UpdatableSketch<Double, DoubleSummary> sketch2 =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory(mode)).build();
    for (int i = 0; i < 8192; i++) {
      sketch2.update(key++, 1.0);
    }

    Union<DoubleSummary> union = new Union<>(4096, new DoubleSummarySetOperations(mode, mode));
    union.update(sketch1);
    union.update(sketch2);
    CompactSketch<DoubleSummary> result = union.getResult();
    Assert.assertEquals(result.getEstimate(), 12288.0, 12288 * 0.01);
    Assert.assertTrue(result.getLowerBound(1) <= result.getEstimate());
    Assert.assertTrue(result.getUpperBound(1) > result.getEstimate());
  }

  @Test
  public void unionMixedMode() {
    int key = 0;
    UpdatableSketch<Double, DoubleSummary> sketch1 =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory(mode)).build();
    for (int i = 0; i < 1000; i++) {
      sketch1.update(key++, 1.0);
      //System.out.println("theta1=" + sketch1.getTheta() + " " + sketch1.getThetaLong());
    }

    key -= 500; // overlap half of the entries
    UpdatableSketch<Double, DoubleSummary> sketch2 =
        new UpdatableSketchBuilder<>
          (new DoubleSummaryFactory(mode)).setSamplingProbability(0.2f).build();
    for (int i = 0; i < 20000; i++) {
      sketch2.update(key++, 1.0);
      //System.out.println("theta2=" + sketch2.getTheta() + " " + sketch2.getThetaLong());
    }

    Union<DoubleSummary> union = new Union<>(4096, new DoubleSummarySetOperations(mode, mode));
    union.update(sketch1);
    union.update(sketch2);
    CompactSketch<DoubleSummary> result = union.getResult();
    Assert.assertEquals(result.getEstimate(), 20500.0, 20500 * 0.01);
    Assert.assertTrue(result.getLowerBound(1) <= result.getEstimate());
    Assert.assertTrue(result.getUpperBound(1) > result.getEstimate());
  }

  @Test
  public void checkUnionUpdateWithTheta() {
    Union<DoubleSummary> union = new Union<>(new DoubleSummarySetOperations(mode, mode));
    UpdateSketch usk = null;
    DoubleSummary dsum = null;

    try { union.update(usk, dsum); fail(); }
    catch (SketchesArgumentException e) { }

    usk = new UpdateSketchBuilder().build();
    try { union.update(usk, dsum); fail(); }
    catch (SketchesArgumentException e) { }

    dsum = new DoubleSummaryFactory(mode).newSummary();
    for (int i = 0; i < 10; i++) { usk.update(i); }
    union.update(usk, dsum);
    Assert.assertEquals(union.getResult().getEstimate(), 10.0);
  }

}

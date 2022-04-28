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
import org.apache.datasketches.SketchesStateException;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.datasketches.theta.UpdateSketchBuilder;
import org.apache.datasketches.tuple.CompactSketch;
import org.apache.datasketches.tuple.Intersection;
import org.apache.datasketches.tuple.Sketch;
import org.apache.datasketches.tuple.SketchIterator;
import org.apache.datasketches.tuple.Sketches;
import org.apache.datasketches.tuple.UpdatableSketch;
import org.apache.datasketches.tuple.UpdatableSketchBuilder;
import org.apache.datasketches.tuple.adouble.DoubleSummary.Mode;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class AdoubleIntersectionTest {
  private final DoubleSummary.Mode mode = Mode.Sum;

  @Test
  public void intersectionNotEmptyNoEntries() {
    final UpdatableSketch<Double, DoubleSummary> sketch1 =
        new UpdatableSketchBuilder<>
          (new DoubleSummaryFactory(mode)).setSamplingProbability(0.01f).build();
    sketch1.update("a", 1.0); // this happens to get rejected because of sampling with low probability
    final Intersection<DoubleSummary> intersection =
        new Intersection<>(new DoubleSummarySetOperations(mode, mode));
    intersection.intersect(sketch1);
    final CompactSketch<DoubleSummary> result = intersection.getResult();
    Assert.assertEquals(result.getRetainedEntries(), 0);
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.getEstimate(), 0.0);
    Assert.assertEquals(result.getLowerBound(1), 0.0, 0.0001);
    Assert.assertTrue(result.getUpperBound(1) > 0);
  }

  @Test
  public void intersectionExactWithEmpty() {
    final UpdatableSketch<Double, DoubleSummary> sketch1 =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory(mode)).build();
    sketch1.update(1, 1.0);
    sketch1.update(2, 1.0);
    sketch1.update(3, 1.0);

    final Sketch<DoubleSummary> sketch2 = Sketches.createEmptySketch();

    final Intersection<DoubleSummary> intersection =
        new Intersection<>(new DoubleSummarySetOperations(mode, mode));
    intersection.intersect(sketch1);
    intersection.intersect(sketch2);
    final CompactSketch<DoubleSummary> result = intersection.getResult();
    Assert.assertEquals(result.getRetainedEntries(), 0);
    Assert.assertTrue(result.isEmpty());
    Assert.assertEquals(result.getEstimate(), 0.0);
    Assert.assertEquals(result.getLowerBound(1), 0.0);
    Assert.assertEquals(result.getUpperBound(1), 0.0);
  }

  @Test
  public void intersectionExactMode() {
    UpdatableSketch<Double, DoubleSummary> sketch1 =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory(mode)).build();
    sketch1.update(1, 1.0);
    sketch1.update(1, 1.0);
    sketch1.update(2, 1.0);
    sketch1.update(2, 1.0);

    final UpdatableSketch<Double, DoubleSummary> sketch2 =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory(mode)).build();
    sketch2.update(2, 1.0);
    sketch2.update(2, 1.0);
    sketch2.update(3, 1.0);
    sketch2.update(3, 1.0);

    final Intersection<DoubleSummary> intersection =
        new Intersection<>(new DoubleSummarySetOperations(mode, mode));
    intersection.intersect(sketch1);
    intersection.intersect(sketch2);
    final CompactSketch<DoubleSummary> result = intersection.getResult();
    Assert.assertEquals(result.getRetainedEntries(), 1);
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.getEstimate(), 1.0);
    Assert.assertEquals(result.getLowerBound(1), 1.0);
    Assert.assertEquals(result.getUpperBound(1), 1.0);
    final SketchIterator<DoubleSummary> it = result.iterator();
    Assert.assertTrue(it.next());
    Assert.assertTrue(it.getHash() > 0);
    Assert.assertEquals(it.getSummary().getValue(), 4.0);
    Assert.assertFalse(it.next());

    intersection.reset();
    sketch1 = null;
    try { intersection.intersect(sketch1); fail();}
    catch (final SketchesArgumentException e) { }

}

  @Test
  public void intersectionDisjointEstimationMode() {
    int key = 0;
    final UpdatableSketch<Double, DoubleSummary> sketch1 =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory(mode)).build();
    for (int i = 0; i < 8192; i++) {
      sketch1.update(key++, 1.0);
    }

    final UpdatableSketch<Double, DoubleSummary> sketch2 =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory(mode)).build();
    for (int i = 0; i < 8192; i++) {
      sketch2.update(key++, 1.0);
    }

    final Intersection<DoubleSummary> intersection =
        new Intersection<>(new DoubleSummarySetOperations(mode, mode));
    intersection.intersect(sketch1);
    intersection.intersect(sketch2);
    CompactSketch<DoubleSummary> result = intersection.getResult();
    Assert.assertEquals(result.getRetainedEntries(), 0);
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.getEstimate(), 0.0);
    Assert.assertEquals(result.getLowerBound(1), 0.0);
    Assert.assertTrue(result.getUpperBound(1) > 0);

    // an intersection with no entries must survive more updates
    intersection.intersect(sketch1);
    result = intersection.getResult();
    Assert.assertEquals(result.getRetainedEntries(), 0);
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(result.getEstimate(), 0.0);
    Assert.assertEquals(result.getLowerBound(1), 0.0);
    Assert.assertTrue(result.getUpperBound(1) > 0);
  }

  @Test
  public void intersectionEstimationMode() {
    int key = 0;
    final UpdatableSketch<Double, DoubleSummary> sketch1 =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory(mode)).build();
    for (int i = 0; i < 8192; i++) {
      sketch1.update(key++, 1.0);
    }

    key -= 4096; // overlap half of the entries
    final UpdatableSketch<Double, DoubleSummary> sketch2 =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory(mode)).build();
    for (int i = 0; i < 8192; i++) {
      sketch2.update(key++, 1.0);
    }

    final Intersection<DoubleSummary> intersection =
        new Intersection<>(new DoubleSummarySetOperations(mode, mode));
    intersection.intersect(sketch1);
    intersection.intersect(sketch2);
    final CompactSketch<DoubleSummary> result = intersection.getResult();
    Assert.assertFalse(result.isEmpty());
    // crude estimate of RSE(95%) = 2 / sqrt(result.getRetainedEntries())
    Assert.assertEquals(result.getEstimate(), 4096.0, 4096 * 0.03);
    Assert.assertTrue(result.getLowerBound(1) <= result.getEstimate());
    Assert.assertTrue(result.getUpperBound(1) > result.getEstimate());
    final SketchIterator<DoubleSummary> it = result.iterator();
    while (it.next()) {
      Assert.assertEquals(it.getSummary().getValue(), 2.0);
    }
  }

  @Test
  public void checkExactIntersectionWithTheta() {
    final UpdateSketch thSkNull = null;
    final UpdateSketch thSkEmpty = new UpdateSketchBuilder().build();
    final UpdateSketch thSk10 = new UpdateSketchBuilder().build();
    final UpdateSketch thSk15 = new UpdateSketchBuilder().build();
    for (int i = 0; i < 10; i++) { thSk10.update(i); }
    for (int i = 0; i < 10; i++) { thSk15.update(i + 5); } //overlap = 5

    DoubleSummary dsum = new DoubleSummaryFactory(mode).newSummary();
    final Intersection<DoubleSummary> intersection =
        new Intersection<>(new DoubleSummarySetOperations(mode, mode));
    CompactSketch<DoubleSummary> result;

    try { intersection.getResult(); fail(); }
    catch (final SketchesStateException e ) { } //OK.

    try { intersection.intersect(thSkNull, dsum); fail(); }
    catch (final SketchesArgumentException e) { } //OK

    intersection.intersect(thSkEmpty, dsum);
    result = intersection.getResult();
    Assert.assertTrue(result.isEmpty()); //Empty after empty first call
    intersection.reset();

    intersection.intersect(thSk10, dsum);
    result = intersection.getResult();
    Assert.assertEquals(result.getEstimate(), 10.0); //Returns valid first call
    intersection.reset();

    intersection.intersect(thSk10, dsum);  // Valid first call
    intersection.intersect(thSkEmpty, dsum);
    result = intersection.getResult();
    Assert.assertTrue(result.isEmpty()); //Returns Empty after empty second call
    intersection.reset();

    intersection.intersect(thSk10, dsum);
    intersection.intersect(thSk15, dsum);
    result = intersection.getResult();
    Assert.assertEquals(result.getEstimate(), 5.0); //Returns intersection
    intersection.reset();

    dsum = null;
    try { intersection.intersect(thSk10, dsum); fail(); }
    catch (final SketchesArgumentException e) { }
  }

  @Test
  public void checkExactIntersectionWithThetaDisjoint() {
    final UpdateSketch thSkA = new UpdateSketchBuilder().setLogNominalEntries(10).build();
    final UpdateSketch thSkB = new UpdateSketchBuilder().setLogNominalEntries(10).build();
    int key = 0;
    for (int i = 0; i < 32;  i++) { thSkA.update(key++); }
    for (int i = 0; i < 32; i++) { thSkB.update(key++); }

    final DoubleSummary dsum = new DoubleSummaryFactory(mode).newSummary();
    final Intersection<DoubleSummary> intersection =
        new Intersection<>(new DoubleSummarySetOperations(mode, mode));
    CompactSketch<DoubleSummary> result;

    intersection.intersect(thSkA, dsum);
    intersection.intersect(thSkB, dsum);
    result = intersection.getResult();
    Assert.assertEquals(result.getRetainedEntries(), 0);

    // an intersection with no entries must survive more updates
    intersection.intersect(thSkA, dsum);
    result = intersection.getResult();
    Assert.assertEquals(result.getRetainedEntries(), 0);
    intersection.reset();
  }

  @Test
  public void checkEstimatingIntersectionWithThetaOverlapping() {
    final UpdateSketch thSkA = new UpdateSketchBuilder().setLogNominalEntries(4).build();
    final UpdateSketch thSkB = new UpdateSketchBuilder().setLogNominalEntries(10).build();
    for (int i = 0; i < 64;  i++) { thSkA.update(i); } //dense mode, low theta
    for (int i = 32; i < 96; i++) { thSkB.update(i); } //exact overlapping

    final DoubleSummary dsum = new DoubleSummaryFactory(mode).newSummary();
    final Intersection<DoubleSummary> intersection =
        new Intersection<>(new DoubleSummarySetOperations(mode, mode));
    CompactSketch<DoubleSummary> result;

    intersection.intersect(thSkA, dsum);
    intersection.intersect(thSkB, dsum);
    result = intersection.getResult();
    Assert.assertEquals(result.getRetainedEntries(), 14);

    thSkB.reset();
    for (int i = 100; i < 164; i++) { thSkB.update(i); } //exact, disjoint
    intersection.intersect(thSkB, dsum); //remove existing entries
    result = intersection.getResult();
    Assert.assertEquals(result.getRetainedEntries(), 0);
    intersection.intersect(thSkB, dsum);
    result = intersection.getResult();
    Assert.assertEquals(result.getRetainedEntries(), 0);
  }

  @Test
  public void intersectionEmpty() {
    final UpdatableSketch<Double, DoubleSummary> sketch =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory(mode)).build();
    final Intersection<DoubleSummary> intersection =
        new Intersection<>(new DoubleSummarySetOperations(mode, mode));
    intersection.intersect(sketch);
    final CompactSketch<DoubleSummary> result = intersection.getResult();
    Assert.assertEquals(result.getRetainedEntries(), 0);
    Assert.assertTrue(result.isEmpty());
    Assert.assertEquals(result.getEstimate(), 0.0);
    Assert.assertEquals(result.getLowerBound(1), 0.0);
    Assert.assertEquals(result.getUpperBound(1), 0.0);
  }

}

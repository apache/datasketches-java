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

import java.util.Random;

import org.apache.datasketches.tuple.Filter;
import org.apache.datasketches.tuple.Sketch;
import org.apache.datasketches.tuple.Sketches;
import org.apache.datasketches.tuple.UpdatableSketch;
import org.apache.datasketches.tuple.UpdatableSketchBuilder;
import org.apache.datasketches.tuple.adouble.DoubleSummary.Mode;
import org.testng.Assert;
import org.testng.annotations.Test;

@SuppressWarnings("javadoc")
public class FilterTest {
    private static final int numberOfElements = 100;
    private static final Random random = new Random(1);//deterministic for this class
    private final DoubleSummary.Mode mode = Mode.Sum;

    @Test
    public void emptySketch() {
        Sketch<DoubleSummary> sketch = Sketches.createEmptySketch();

        Filter<DoubleSummary> filter = new Filter<>(o -> true);

        Sketch<DoubleSummary> filteredSketch = filter.filter(sketch);

        Assert.assertEquals(filteredSketch.getEstimate(), 0.0);
        Assert.assertEquals(filteredSketch.getThetaLong(), sketch.getThetaLong());
        Assert.assertTrue(filteredSketch.isEmpty());
        Assert.assertEquals(filteredSketch.getLowerBound(1), 0.0);
        Assert.assertEquals(filteredSketch.getUpperBound(1), 0.0);
    }

    @Test
    public void nullSketch() {
        Filter<DoubleSummary> filter = new Filter<>(o -> true);

        Sketch<DoubleSummary> filteredSketch = filter.filter(null);

        Assert.assertEquals(filteredSketch.getEstimate(), 0.0);
        Assert.assertEquals(filteredSketch.getThetaLong(), Long.MAX_VALUE);
        Assert.assertTrue(filteredSketch.isEmpty());
        Assert.assertEquals(filteredSketch.getLowerBound(1), 0.0);
        Assert.assertEquals(filteredSketch.getUpperBound(1), 0.0);
    }

    @Test
    public void filledSketchShouldBehaveTheSame() {
        UpdatableSketch<Double, DoubleSummary> sketch =
            new UpdatableSketchBuilder<>(new DoubleSummaryFactory(mode)).build();

        fillSketch(sketch, numberOfElements, 0.0);

        Filter<DoubleSummary> filter = new Filter<>(o -> true);

        Sketch<DoubleSummary> filteredSketch = filter.filter(sketch);

        Assert.assertEquals(filteredSketch.getEstimate(), sketch.getEstimate());
        Assert.assertEquals(filteredSketch.getThetaLong(), sketch.getThetaLong());
        Assert.assertFalse(filteredSketch.isEmpty());
        Assert.assertEquals(filteredSketch.getLowerBound(1), sketch.getLowerBound(1));
        Assert.assertEquals(filteredSketch.getUpperBound(1), sketch.getUpperBound(1));
    }

    @Test
    public void filledSketchShouldFilterOutElements() {
        UpdatableSketch<Double, DoubleSummary> sketch =
            new UpdatableSketchBuilder<>(new DoubleSummaryFactory(mode)).build();

        fillSketch(sketch, numberOfElements, 0.0);
        fillSketch(sketch, 2 * numberOfElements, 1.0);

        Filter<DoubleSummary> filter = new Filter<>(o -> o.getValue() < 0.5);

        Sketch<DoubleSummary> filteredSketch = filter.filter(sketch);

        Assert.assertEquals(filteredSketch.getEstimate(), (double) numberOfElements);
        Assert.assertEquals(filteredSketch.getThetaLong(), sketch.getThetaLong());
        Assert.assertFalse(filteredSketch.isEmpty());
        Assert.assertTrue(filteredSketch.getLowerBound(1) <= filteredSketch.getEstimate());
        Assert.assertTrue(filteredSketch.getUpperBound(1) >= filteredSketch.getEstimate());
    }

    @Test
    public void filteringInEstimationMode() {
        UpdatableSketch<Double, DoubleSummary> sketch =
            new UpdatableSketchBuilder<>(new DoubleSummaryFactory(mode)).build();

        int n = 10000;
        fillSketch(sketch, n, 0.0);
        fillSketch(sketch, 2 * n, 1.0);

        Filter<DoubleSummary> filter = new Filter<>(o -> o.getValue() < 0.5);

        Sketch<DoubleSummary> filteredSketch = filter.filter(sketch);

        Assert.assertEquals(filteredSketch.getEstimate(), n, n * 0.05);
        Assert.assertEquals(filteredSketch.getThetaLong(), sketch.getThetaLong());
        Assert.assertFalse(filteredSketch.isEmpty());
        Assert.assertTrue(filteredSketch.getLowerBound(1) <= filteredSketch.getEstimate());
        Assert.assertTrue(filteredSketch.getUpperBound(1) >= filteredSketch.getEstimate());
    }

    @Test
    public void nonEmptySketchWithNoEntries() {
      UpdatableSketch<Double, DoubleSummary> sketch =
          new UpdatableSketchBuilder<>(
              new DoubleSummaryFactory(mode)).setSamplingProbability(0.0001f).build();
      sketch.update(0, 0.0);

      Assert.assertFalse(sketch.isEmpty());
      Assert.assertEquals(sketch.getRetainedEntries(), 0);

      Filter<DoubleSummary> filter = new Filter<>(o -> true);

      Sketch<DoubleSummary> filteredSketch = filter.filter(sketch);

      Assert.assertFalse(filteredSketch.isEmpty());
      Assert.assertEquals(filteredSketch.getEstimate(), sketch.getEstimate());
      Assert.assertEquals(filteredSketch.getThetaLong(), sketch.getThetaLong());
      Assert.assertEquals(filteredSketch.getLowerBound(1), sketch.getLowerBound(1));
      Assert.assertEquals(filteredSketch.getUpperBound(1), sketch.getUpperBound(1));
    }

    private static void fillSketch(UpdatableSketch<Double, DoubleSummary> sketch,
        int numberOfElements, Double sketchValue) {


      for (int cont = 0; cont < numberOfElements; cont++) {
          sketch.update(random.nextLong(), sketchValue);
      }
    }
}

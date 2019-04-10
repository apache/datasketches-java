package com.yahoo.sketches.tuple;

import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.sketches.tuple.adouble.DoubleSummary;
import com.yahoo.sketches.tuple.adouble.DoubleSummaryFactory;

public class FilterTest {

    private static final int numberOfElements = 100;
    private static final Random random = new Random(1);//deterministic for this class

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
            new UpdatableSketchBuilder<>(new DoubleSummaryFactory()).build();

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
            new UpdatableSketchBuilder<>(new DoubleSummaryFactory()).build();

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
            new UpdatableSketchBuilder<>(new DoubleSummaryFactory()).build();

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
              new DoubleSummaryFactory()).setSamplingProbability(0.0001f).build();
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
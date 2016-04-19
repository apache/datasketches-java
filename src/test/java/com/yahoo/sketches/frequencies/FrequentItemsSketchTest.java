package com.yahoo.sketches.frequencies;

import org.testng.annotations.Test;

import com.yahoo.sketches.memory.NativeMemory;

import org.testng.Assert;

public class FrequentItemsSketchTest {

  @Test
  public void empty() {
    FrequentItemsSketch<String> sketch = new FrequentItemsSketch<String>(8);
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertEquals(sketch.getNumActiveItems(), 0);
    Assert.assertEquals(sketch.getLowerBound("a"), 0);
    Assert.assertEquals(sketch.getUpperBound("a"), 0);
  }

  @Test
  public void nullInput() {
    FrequentItemsSketch<String> sketch = new FrequentItemsSketch<String>(8);
    sketch.update(null);
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertEquals(sketch.getNumActiveItems(), 0);
    Assert.assertEquals(sketch.getLowerBound(null), 0);
    Assert.assertEquals(sketch.getUpperBound(null), 0);
  }

  @Test
  public void oneItem() {
    FrequentItemsSketch<String> sketch = new FrequentItemsSketch<String>(8);
    sketch.update("a");
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertEquals(sketch.getNumActiveItems(), 1);
    Assert.assertEquals(sketch.getStreamLength(), 1);
    Assert.assertEquals(sketch.getEstimate("a"), 1);
    Assert.assertEquals(sketch.getLowerBound("a"), 1);
  }

  @Test
  public void severalItems() {
    FrequentItemsSketch<String> sketch = new FrequentItemsSketch<String>(8);
    sketch.update("a");
    sketch.update("b");
    sketch.update("c");
    sketch.update("d");
    sketch.update("b");
    sketch.update("c");
    sketch.update("b");
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertEquals(sketch.getNumActiveItems(), 4);
    Assert.assertEquals(sketch.getStreamLength(), 7);
    Assert.assertEquals(sketch.getEstimate("a"), 1);
    Assert.assertEquals(sketch.getEstimate("b"), 3);
    Assert.assertEquals(sketch.getEstimate("c"), 2);
    Assert.assertEquals(sketch.getEstimate("d"), 1);

    String[] items = sketch.getFrequentItems(2, FrequentItemsSketch.ErrorSpecification.NO_FALSE_POSITIVES);
    Assert.assertEquals(items.length, 2);
  }

  @Test
  public void estimationMode() {
    FrequentItemsSketch<Integer> sketch = new FrequentItemsSketch<Integer>(8);
    sketch.update(1);
    sketch.update(1);
    sketch.update(1);
    sketch.update(1);
    sketch.update(1);
    sketch.update(2);
    sketch.update(3);
    sketch.update(4);
    sketch.update(5);
    sketch.update(6);
    sketch.update(7);
    sketch.update(7);
    sketch.update(7);
    sketch.update(8);
    sketch.update(9);
    sketch.update(10);
    sketch.update(11);
    sketch.update(12);

    Assert.assertFalse(sketch.isEmpty());
    Assert.assertEquals(sketch.getStreamLength(), 18);

    Integer[] items = sketch.getFrequentItems(2, FrequentItemsSketch.ErrorSpecification.NO_FALSE_POSITIVES);
    Assert.assertEquals(items.length, 2);
  }

  @Test
  public void serializeDeserialize() {
    FrequentItemsSketch<String> sketch1 = new FrequentItemsSketch<String>(8);
    sketch1.update("a");
    sketch1.update("b");
    sketch1.update("c");
    sketch1.update("d");

    byte[] bytes = sketch1.serializeToByteArray(new StringArraySerDe());
    FrequentItemsSketch<String> sketch2 = FrequentItemsSketch.getInstance(new NativeMemory(bytes), new StringArraySerDe());
    sketch2.update("b");
    sketch2.update("c");
    sketch2.update("b");

    Assert.assertFalse(sketch2.isEmpty());
    Assert.assertEquals(sketch2.getNumActiveItems(), 4);
    Assert.assertEquals(sketch2.getStreamLength(), 7);
    Assert.assertEquals(sketch2.getEstimate("a"), 1);
    Assert.assertEquals(sketch2.getEstimate("b"), 3);
    Assert.assertEquals(sketch2.getEstimate("c"), 2);
    Assert.assertEquals(sketch2.getEstimate("d"), 1);
  }

  @Test
  public void mergeExact() {
    FrequentItemsSketch<String> sketch1 = new FrequentItemsSketch<String>(8);
    sketch1.update("a");
    sketch1.update("b");
    sketch1.update("c");
    sketch1.update("d");
    
    FrequentItemsSketch<String> sketch2 = new FrequentItemsSketch<String>(8);
    sketch2.update("b");
    sketch2.update("c");
    sketch2.update("b");

    sketch1.merge(sketch2);
    Assert.assertFalse(sketch1.isEmpty());
    Assert.assertEquals(sketch1.getNumActiveItems(), 4);
    Assert.assertEquals(sketch1.getStreamLength(), 7);
    Assert.assertEquals(sketch1.getEstimate("a"), 1);
    Assert.assertEquals(sketch1.getEstimate("b"), 3);
    Assert.assertEquals(sketch1.getEstimate("c"), 2);
    Assert.assertEquals(sketch1.getEstimate("d"), 1);
  }

}

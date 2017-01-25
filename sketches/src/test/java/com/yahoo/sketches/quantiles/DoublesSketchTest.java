package com.yahoo.sketches.quantiles;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.memory.NativeMemory;

public class DoublesSketchTest {

  @Test
  public void heapToDirect() {
    DoublesSketch heapSketch = DoublesSketch.builder().build();
    for (int i = 0; i < 1000; i++) {
      heapSketch.update(i);
    }
    DoublesSketch directSketch = DoublesSketch.wrap(new NativeMemory(heapSketch.toByteArray()));
    for (int i = 0; i < 1000; i++) {
      directSketch.update(i + 1000);
    }
    Assert.assertEquals(directSketch.getMinValue(), 0);
    Assert.assertEquals(directSketch.getMaxValue(), 1999);
    Assert.assertEquals(directSketch.getQuantile(0.5), 1000.0, 10.0);
  }

  @Test
  public void directToHeap() {
    int sizeBytes = 10000;
    DoublesSketch directSketch = DoublesSketch.builder().initMemory(new NativeMemory(new byte[sizeBytes])).build();
    for (int i = 0; i < 1000; i++) {
      directSketch.update(i);
    }
    DoublesSketch heapSketch = DoublesSketch.heapify(new NativeMemory(directSketch.toByteArray()));
    for (int i = 0; i < 1000; i++) {
      heapSketch.update(i + 1000);
    }
    Assert.assertEquals(heapSketch.getMinValue(), 0.0);
    Assert.assertEquals(heapSketch.getMaxValue(), 1999.0);
    Assert.assertEquals(heapSketch.getQuantile(0.5), 1000.0, 10.0);
  }

}

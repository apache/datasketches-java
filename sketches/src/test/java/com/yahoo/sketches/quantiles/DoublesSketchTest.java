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
    DoublesSketch directSketch = DoublesSketch.wrap(new NativeMemory(heapSketch.toByteArray(true, false)));

    Assert.assertEquals(directSketch.getMinValue(), 0.0);
    Assert.assertEquals(directSketch.getMaxValue(), 999.0);
    Assert.assertEquals(directSketch.getQuantile(0.5), 500.0, 4.0);
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

  @Test
  public void checkToByteArray() {
    DoublesSketch ds = DoublesSketch.builder().build(); //k = 128
    ds.update(1);
    ds.update(2);
    byte[] arr = ds.toByteArray(false, false);
    Assert.assertEquals(arr.length, 2080);
  }

  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    //System.out.println(s); //disable here
  }

}

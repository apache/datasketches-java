/*
 * Copyright 2019, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.Util.LS;

import java.util.HashSet;

import org.testng.annotations.Test;

import com.yahoo.memory.WritableDirectHandle;
import com.yahoo.memory.WritableMemory;

/**
 * @author Lee Rhodes
 */
public class DebugUnionTest {

  @SuppressWarnings("resource")
  @Test
  public void test() {
    //final Random rnd = new Random(1);
    final int n = 70_000;
    final int numSketches = 3;
    final int bytes = 10_000_000;
    WritableDirectHandle wdh = WritableMemory.allocateDirect(bytes);
    WritableMemory wmem = wdh.get();
    final DoublesUnion union = DoublesUnion.builder().setMaxK(8).build(wmem);
    //final DoublesUnion union = DoublesUnion.builder().build();
    final HashSet<Double> set = new HashSet<>();
    {
      for (int s = 0; s < numSketches; s++) {
        final UpdateDoublesSketch updateSketch = DoublesSketch.builder().setK(8).build();
        for (int i = 0; i < n; i++) {
          final double value = DoublesSketch.rand.nextInt(1000) + 1;
          updateSketch.update(value);
          set.add(value);
        }
        union.update(updateSketch);
        println(updateSketch.toString(true, true));
      }
    }
    DoublesSketch sketch = union.getResult();
    //double[] quantiles = sketch.getQuantiles(1000);
    int count = 0;
    int errors = 0;
    //DoublesSketchIterator it = sketch.iterator();
    DoublesSketchIterator it = ((DoublesUnionImplR)union).gadget_.iterator();
    while (it.next()) {
      count++;
      if (!set.contains(it.getValue())) {
        errors++;
        println("Weight: " + it.getWeight());
      }
    }

    println("Values: " + count + ", errors: " + errors);
    //assertEquals(count, 0);
    //System.out.println(sketch.toString());
    println(sketch.toString(true, true));
    wdh.close();
  }

  @Test
  public void printlnTest() {
    println("PRINTING: " + this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    print(s+LS);
  }

  /**
   * @param s value to print
   */
  static void print(String s) {
    System.out.print(s); //disable here
  }

}

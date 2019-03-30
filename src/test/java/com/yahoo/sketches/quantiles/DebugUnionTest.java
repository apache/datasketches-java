/*
 * Copyright 2019, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.Util.LS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.HashSet;
import java.util.Random;

import org.testng.annotations.Test;

import com.yahoo.memory.WritableDirectHandle;
import com.yahoo.memory.WritableMemory;

/**
 * @author Lee Rhodes
 */
public class DebugUnionTest {

  @Test
  public void test() {
    final int n = 70_000;
    final int valueLimit = 1000;
    final int numSketches = 3;
    final int sketchK = 8;
    final int unionK = 8;
    UpdateDoublesSketch[] sketchArr = new UpdateDoublesSketch[numSketches];

    //builds the input sketches, all on heap
    DoublesSketch.rand = new Random(1); //make deterministic for test
    final HashSet<Double> set = new HashSet<>(); //holds input values
    for (int s = 0; s < numSketches; s++) {
      sketchArr[s] = buildHeapSketch(sketchK, n, valueLimit, set);
    }

    //loads the on heap union
    DoublesSketch.rand = new Random(1); //make deterministic for test
    DoublesUnion hUnion = DoublesUnion.builder().setMaxK(unionK).build();
    for (int s = 0; s < numSketches; s++) { hUnion.update(sketchArr[s]); }
    DoublesSketch hSketch = hUnion.getResult();

    //loads the direct union
    DoublesSketch.rand = new Random(1); //make deterministic for test
    DoublesUnion dUnion;
    DoublesSketch dSketch;
    try ( WritableDirectHandle wdh = WritableMemory.allocateDirect(10_000_000) ) {
      WritableMemory wmem = wdh.get();
      dUnion = DoublesUnion.builder().setMaxK(8).build(wmem);
      for (int s = 0; s < numSketches; s++) { dUnion.update(sketchArr[s]); }
      dSketch = dUnion.getResult(); //result is on heap
    }

    //iterates and counts errors
    int hCount = hSketch.getRetainedItems();
    int dCount = dSketch.getRetainedItems();

    assertEquals(hCount, dCount); //Retained items must be the same

    double[] heapItems = new double[hCount];
    double[] directItems = new double[dCount];
    int hErrors = 0;
    int dErrors = 0;

    DoublesSketchIterator hit = hSketch.iterator();
    DoublesSketchIterator dit = dSketch.iterator();
    int i = 0;
    while (hit.next() && dit.next()) {
      double v = hit.getValue();
      heapItems[i] = v;
      if (!set.contains(v)) { hErrors++; }

      double w = dit.getValue();
      directItems[i] = w;
      if (!set.contains(w)) { dErrors++; }
      i++;
      assertEquals(v, w, 0); //Items must be returned in same order and be equal
    }
    assertTrue(hErrors == 0);
    assertTrue(dErrors == 0);

    //println("HeapUnion  : Values: " + hCount + ", errors: " + hErrors);
    //println(hSketch.toString(true, true));

    //println("DirectUnion: Values: " + dCount + ", errors: " + dErrors);
    //println(dSketch.toString(true, true));
  }

  private static UpdateDoublesSketch buildHeapSketch(final int k, final int n, final int valueLimit,
      final HashSet<Double> set) {
    final UpdateDoublesSketch uSk = DoublesSketch.builder().setK(k).build();
    for (int i = 0; i < n; i++) {
      final double value = DoublesSketch.rand.nextInt(valueLimit) + 1;
      uSk.update(value);
      set.add(value);
    }
    return uSk;
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
    //System.out.print(s); //disable here
  }

}

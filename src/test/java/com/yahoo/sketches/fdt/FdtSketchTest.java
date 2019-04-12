/*
 * Copyright 2019, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.fdt;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.tuple.SketchIterator;
import com.yahoo.sketches.tuple.strings.ArrayOfStringsSketch;
import com.yahoo.sketches.tuple.strings.ArrayOfStringsSummary;


/**
 * @author Lee Rhodes
 */
public class FdtSketchTest {
  private static final String LS = System.getProperty("line.separator");

  @Test
  public void checkFunSketch() {
    final int lgK = 14;
    final FdtSketch sketch = new FdtSketch(lgK);

    final String[] nodesArr = {"abc", "def" };
    sketch.update(nodesArr);

    final SketchIterator<ArrayOfStringsSummary> it = sketch.iterator();
    int count = 0;
    while (it.next()) {
      final String[] nodesArr2 = it.getSummary().getValue();
      assertEquals(nodesArr2, nodesArr);
      count++;
    }
    assertEquals(count, 1);

    //serialize
    final byte[] byteArr = sketch.toByteArray();
    //deserialize
    Memory mem = Memory.wrap(byteArr);
    FdtSketch sketch2 = new FdtSketch(new ArrayOfStringsSketch(mem));

    //check output
    final SketchIterator<ArrayOfStringsSummary> it2 = sketch2.iterator();
    int count2 = 0;
    while (it2.next()) {
      final String[] nodesArr2 = it2.getSummary().getValue();
      assertEquals(nodesArr2, nodesArr);
      count2++;
    }
    assertEquals(count, count2);
    assertEquals(sketch2.getEstimate(), sketch.getEstimate());
    assertEquals(sketch2.getLowerBound(2), sketch.getLowerBound(2));
    assertEquals(sketch2.getUpperBound(2), sketch.getUpperBound(2));
  }

  @Test
  public void checkAlternateLgK() {
    int lgK = FdtSketch.computeLgK(.01, .01);
    assertEquals(lgK, 20);
    try {
      lgK = FdtSketch.computeLgK(.01, .001);
      fail();
    } catch (SketchesArgumentException e) {
      //println("" + e);
    }
  }

  @Test
  public void checkFunSketchWithThreshold() {
    FdtSketch sk = new FdtSketch(.02, .05); //thresh, RSE
    println("LgK: " + sk.getLgK());
  }

  @Test
  public void checkGetPrimaryKey() {
    String[] arr = {"aaa", "bbb", "ccc"};
    int[] priKeyIndices = {0,2};
    String s = FdtSketch.getPrimaryKey(arr, priKeyIndices);
    println(s);
  }

  @Test
  public void simpleCheckPrepare() {
    FdtSketch sk = new FdtSketch(8);
    String[] arr1 = {"a", "1", "c"};
    String[] arr2 = {"a", "2", "c"};
    String[] arr3 = {"a", "3", "c"};
    String[] arr4 = {"a", "4", "c"};
    String[] arr5 = {"a", "1", "d"};
    String[] arr6 = {"a", "2", "d"};
    int[] priKeyIndices = {0,2};
    sk.update(arr1);
    sk.update(arr2);
    sk.update(arr3);
    sk.update(arr4);
    sk.update(arr5);
    sk.update(arr6);
    sk.prepare(priKeyIndices);
  }


  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    print(s + LS);
  }

  /**
   * @param s value to print
   */
  static void print(String s) {
    System.out.print(s);  //disable here
  }

}

/*
 * Copyright 2019, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.fdt;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.List;

import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.tuple.SketchIterator;
import com.yahoo.sketches.tuple.strings.ArrayOfStringsSummary;

/**
 * @author Lee Rhodes
 */
public class FdtSketchTest {
  private static final String LS = System.getProperty("line.separator");
  private static final char sep = '|'; //string separator

  @Test
  public void checkFdtSketch() {
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
    FdtSketch sketch2 = new FdtSketch(mem);

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
    lgK = FdtSketch.computeLgK(.02, .05);
    assertEquals(lgK, 15);
    try {
      lgK = FdtSketch.computeLgK(.01, .001);
      fail();
    } catch (SketchesArgumentException e) {
      //ok
    }
  }

  @Test
  public void checkFdtSketchWithThreshold() {
    FdtSketch sk = new FdtSketch(.02, .05); //thresh, RSE
    assertEquals(sk.getLgK(), 15);
    println("LgK: " + sk.getLgK());
  }

  @Test
  public void simpleCheckPostProcessing() {
    FdtSketch sk = new FdtSketch(8);
    int[] priKeyIndices = {0,2};
    String[] arr1 = {"a", "1", "c"};
    String[] arr2 = {"a", "2", "c"};
    String[] arr3 = {"a", "3", "c"};
    String[] arr4 = {"a", "4", "c"};
    String[] arr5 = {"a", "1", "d"};
    String[] arr6 = {"a", "2", "d"};
    sk.update(arr1);
    sk.update(arr2);
    sk.update(arr3);
    sk.update(arr4);
    sk.update(arr5);
    sk.update(arr6);
    //get results from PostProcessor directly
    Group gp = new Group(); //uninitialized
    PostProcessor post = new PostProcessor(sk, gp, sep);
    post = sk.getPostProcessor(gp, sep);
    post = sk.getPostProcessor(); //equivalent
    List<Group> list = post.getGroupList(priKeyIndices, 2, 0);
    assertEquals(list.size(), 2);
    assertEquals(post.getGroupCount(), 2);
    println(gp.getHeader());
    for (int i = 0; i < list.size(); i++) {
      println(list.get(i).toString());
    }
    list = post.getGroupList(priKeyIndices, 2, 1);
    assertEquals(list.size(), 1);

    //get results from sketch directly
    list = sk.getResult(priKeyIndices, 0, 2, sep);
    assertEquals(list.size(), 2);
  }

  @Test
  public void checkEstimatingPostProcessing() {
    FdtSketch sk = new FdtSketch(4);
    int[] priKeyIndices = {0};
    for (int i = 0; i < 32; i++) {
      String[] arr = {"a", Integer.toHexString(i)};
      sk.update(arr);
    }
    assertTrue(sk.isEstimationMode());
    List<Group> list = sk.getResult(priKeyIndices, 0, 2, sep);
    assertEquals(list.size(), 1);
    println(new Group().getHeader());
    for (int i = 0; i < list.size(); i++) {
      println(list.get(i).toString());
    }
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
    //System.out.print(s);  //disable here
  }

}

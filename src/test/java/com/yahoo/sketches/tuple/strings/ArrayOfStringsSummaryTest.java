/*
 * Copyright 2019, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.tuple.strings;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.tuple.DeserializeResult;

/**
 * @author Lee Rhodes
 */
public class ArrayOfStringsSummaryTest {

  @Test
  public void checkToByteArray() {
    String[] strArr =  new String[] {"abcd", "abcd", "abcd"};
    ArrayOfStringsSummary nsum = new ArrayOfStringsSummary(strArr);
    ArrayOfStringsSummary copy = nsum.copy();
    assertTrue(copy.equals(nsum));
    byte[] out = nsum.toByteArray();

    Memory mem = Memory.wrap(out);
    ArrayOfStringsSummary nsum2 = new ArrayOfStringsSummary(mem);
    String[] nodesArr = nsum2.getValue();
    for (String s : nodesArr) {
      println(s);
    }

    println("\nfromMemory(mem)");
    DeserializeResult<ArrayOfStringsSummary> dres = ArrayOfStringsSummary.fromMemory(mem);
    ArrayOfStringsSummary nsum3 = dres.getObject();
    nodesArr = nsum3.getValue();
    for (String s : nodesArr) {
      println(s);
    }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkNumNodes() {
    ArrayOfStringsSummary.checkNumNodes(200);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInBytes() {
    Memory mem = Memory.wrap(new byte[100]);
    ArrayOfStringsSummary.checkInBytes(mem, 200);
  }

  @SuppressWarnings("unlikely-arg-type")
  @Test
  public void checkHashCode() {
    String[] strArr =  new String[] {"abcd", "abcd", "abcd"};
    ArrayOfStringsSummary sum1 = new ArrayOfStringsSummary(strArr);
    ArrayOfStringsSummary sum2 = new ArrayOfStringsSummary(strArr);
    int hc1 = sum1.hashCode();
    int hc2 = sum2.hashCode();
    assertEquals(hc1, hc2);
    assertTrue(sum1.equals(sum2));
    assertFalse(sum1.equals(hc2));
    assertFalse(sum1.equals(null));
  }

  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    //System.out.println(s);
  }

}

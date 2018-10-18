/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.Family;

/**
 * @author Lee Rhodes
 */
public class IteratorTest {

  @Test
  public void checkDirectCompactSketch() {
    int k = 16;
    int maxBytes = Sketch.getMaxUpdateSketchBytes(k);
    WritableMemory wmem = WritableMemory.allocate(maxBytes);
    UpdateSketch sk1 = Sketches.updateSketchBuilder().setNominalEntries(k).build(wmem);
    println(sk1.getClass().getSimpleName());
    for (int i = 0; i < (k/2); i++) { sk1.update(i); }
    HashIterator itr1 = sk1.iterator();
    int count = 0;
    while (itr1.next()) {
      println(++count + "\t" + Long.toHexString(itr1.get()));
    }
    assertEquals(count, k/2);

    println("");
    Sketch sk2 = sk1.compact();
    println(sk2.getClass().getSimpleName());
    HashIterator itr2 = sk2.iterator();
    count = 0;
    while (itr2.next()) {
      println(++count + "\t" + Long.toHexString(itr2.get()));
    }
    assertEquals(count, k/2);

    println("");
    Sketch sk3 = sk1.compact(false, WritableMemory.allocate(maxBytes));
    println(sk3.getClass().getSimpleName());
    HashIterator itr3 = sk3.iterator();
    count = 0;
    while (itr3.next()) {
      println(++count + "\t" + Long.toHexString(itr3.get()));
    }
    assertEquals(count, k/2);
  }

  @Test
  public void checkHeapAlphaSketch() {
    int k = 512;
    int u = 8;
    UpdateSketch sk1 = Sketches.updateSketchBuilder().setNominalEntries(k).setFamily(Family.ALPHA)
        .build();
    println(sk1.getClass().getSimpleName());
    for (int i = 0; i < u; i++) { sk1.update(i); }
    HashIterator itr1 = sk1.iterator();
    int count = 0;
    while (itr1.next()) {
      println(++count + "\t" + Long.toHexString(itr1.get()));
    }
    assertEquals(count, u);
  }

  @Test
  public void checkHeapQSSketch() {
    int k = 16;
    int u = 8;
    UpdateSketch sk1 = Sketches.updateSketchBuilder().setNominalEntries(k)
        .build();
    println(sk1.getClass().getSimpleName());
    for (int i = 0; i < u; i++) { sk1.update(i); }
    HashIterator itr1 = sk1.iterator();
    int count = 0;
    while (itr1.next()) {
      println(++count + "\t" + Long.toHexString(itr1.get()));
    }
    assertEquals(count, u);
  }

  @Test
  public void checkSingleItemSketch() {
    int k = 16;
    int u = 1;
    UpdateSketch sk1 = Sketches.updateSketchBuilder().setNominalEntries(k)
        .build();

    for (int i = 0; i < u; i++) { sk1.update(i); }
    CompactSketch csk = sk1.compact();
    println(csk.getClass().getSimpleName());
    HashIterator itr1 = csk.iterator();
    int count = 0;
    while (itr1.next()) {
      println(++count + "\t" + Long.toHexString(itr1.get()));
    }
    assertEquals(count, u);
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

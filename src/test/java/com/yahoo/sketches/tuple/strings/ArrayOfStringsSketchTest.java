/*
 * Copyright 2019, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.tuple.strings;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.tuple.AnotB;
import com.yahoo.sketches.tuple.CompactSketch;
import com.yahoo.sketches.tuple.Intersection;
import com.yahoo.sketches.tuple.SketchIterator;
import com.yahoo.sketches.tuple.Union;

/**
 * @author Lee Rhodes
 */
public class ArrayOfStringsSketchTest {
  private static final String LS = System.getProperty("line.separator");

  @Test
  public void checkSketch() {
    ArrayOfStringsSketch sketch1 = new ArrayOfStringsSketch();
    String[][] strArrArr = {{"a","b"},{"c","d"},{"e","f"}};
    int len = strArrArr.length;
    for (int i = 0; i < len; i++) {
      sketch1.update(strArrArr[i], strArrArr[i]);
    }
    sketch1.update(strArrArr[0], strArrArr[0]); //insert duplicate
    //printSummaries(sketch1.iterator());
    byte[] array = sketch1.toByteArray();
    WritableMemory wmem = WritableMemory.wrap(array);
    ArrayOfStringsSketch sketch2 = new ArrayOfStringsSketch(wmem);
    //printSummaries(sketch2.iterator());
    checkSummaries(sketch2, sketch2);

    String[] strArr3 = {"g", "h" };
    sketch2.update(strArr3, strArr3);


    Union<ArrayOfStringsSummary> union = new Union<>(new ArrayOfStringsSummarySetOperations());
    union.update(sketch1);
    union.update(sketch2);
    CompactSketch<ArrayOfStringsSummary> csk = union.getResult();
    //printSummaries(csk.iterator());
    assertEquals(csk.getRetainedEntries(), 4);

    Intersection<ArrayOfStringsSummary> inter =
        new Intersection<>(new ArrayOfStringsSummarySetOperations());
    inter.update(sketch1);
    inter.update(sketch2);
    csk = inter.getResult();
    assertEquals(csk.getRetainedEntries(), 3);

    AnotB<ArrayOfStringsSummary> aNotB =  new AnotB<>();
    aNotB.update(sketch2, sketch1);
    csk = aNotB.getResult();
    assertEquals(csk.getRetainedEntries(), 1);

  }

  private static void checkSummaries(ArrayOfStringsSketch sk1, ArrayOfStringsSketch sk2) {
    SketchIterator<ArrayOfStringsSummary> it1 = sk1.iterator();
    SketchIterator<ArrayOfStringsSummary> it2 = sk2.iterator();
    while(it1.next() && it2.next()) {
      ArrayOfStringsSummary sum1 = it1.getSummary();
      ArrayOfStringsSummary sum2 = it2.getSummary();
      assertTrue(sum1.equals(sum2));
    }
  }

  static void printSummaries(SketchIterator<ArrayOfStringsSummary> it) {
    while (it.next()) {
      String[] strArr = it.getSummary().getValue();
      for (String s : strArr) {
        print(s + ", ");
      }
      println("");
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

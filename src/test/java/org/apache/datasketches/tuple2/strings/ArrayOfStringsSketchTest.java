/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.tuple2.strings;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.tuple2.AnotB;
import org.apache.datasketches.tuple2.CompactSketch;
import org.apache.datasketches.tuple2.Intersection;
import org.apache.datasketches.tuple2.TupleSketchIterator;
import org.apache.datasketches.tuple2.Union;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class ArrayOfStringsSketchTest {
  private static final String LS = System.getProperty("line.separator");

  @SuppressWarnings("deprecation")
  @Test
  public void checkSketch() {
    ArrayOfStringsSketch sketch1 = new ArrayOfStringsSketch();
    String[][] strArrArr = {{"a","b"},{"c","d"},{"e","f"}};
    int len = strArrArr.length;
    for (int i = 0; i < len; i++) {
      sketch1.update(strArrArr[i], strArrArr[i]);
    }
    sketch1.update(strArrArr[0], strArrArr[0]); //insert duplicate
    printSummaries(sketch1.iterator());
    byte[] array = sketch1.toByteArray();
    MemorySegment wseg = MemorySegment.ofArray(array);
    ArrayOfStringsSketch sketch2 = new ArrayOfStringsSketch(wseg);
    printSummaries(sketch2.iterator());
    checkSummaries(sketch2, sketch2);

    String[] strArr3 = {"g", "h" };
    sketch2.update(strArr3, strArr3);

    Union<ArrayOfStringsSummary> union = new Union<>(new ArrayOfStringsSummarySetOperations());
    union.union(sketch1);
    union.union(sketch2);
    CompactSketch<ArrayOfStringsSummary> csk = union.getResult();
    //printSummaries(csk.iterator());
    assertEquals(csk.getRetainedEntries(), 4);

    Intersection<ArrayOfStringsSummary> inter =
        new Intersection<>(new ArrayOfStringsSummarySetOperations());
    inter.intersect(sketch1);
    inter.intersect(sketch2);
    csk = inter.getResult();
    assertEquals(csk.getRetainedEntries(), 3);

    AnotB<ArrayOfStringsSummary> aNotB =  new AnotB<>();
    aNotB.setA(sketch2);
    aNotB.notB(sketch1);
    csk = aNotB.getResult(true);
    assertEquals(csk.getRetainedEntries(), 1);

  }

  private static void checkSummaries(ArrayOfStringsSketch sk1, ArrayOfStringsSketch sk2) {
    TupleSketchIterator<ArrayOfStringsSummary> it1 = sk1.iterator();
    TupleSketchIterator<ArrayOfStringsSummary> it2 = sk2.iterator();
    while(it1.next() && it2.next()) {
      ArrayOfStringsSummary sum1 = it1.getSummary();
      ArrayOfStringsSummary sum2 = it2.getSummary();
      assertTrue(sum1.equals(sum2));
    }
  }

  static void printSummaries(TupleSketchIterator<ArrayOfStringsSummary> it) {
    while (it.next()) {
      String[] strArr = it.getSummary().getValue();
      for (String s : strArr) {
        print(s + ", ");
      }
      println("");
    }
  }

  @Test
  public void checkCopyCtor() {
    ArrayOfStringsSketch sk1 = new ArrayOfStringsSketch();
    String[][] strArrArr = {{"a","b"},{"c","d"},{"e","f"}};
    int len = strArrArr.length;
    for (int i = 0; i < len; i++) {
      sk1.update(strArrArr[i], strArrArr[i]);
    }
    assertEquals(sk1.getRetainedEntries(), 3);
    final ArrayOfStringsSketch sk2 = sk1.copy();
    assertEquals(sk2.getRetainedEntries(), 3);
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

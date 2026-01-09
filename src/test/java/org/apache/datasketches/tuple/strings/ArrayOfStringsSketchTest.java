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

package org.apache.datasketches.tuple.strings;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.tuple.TupleAnotB;
import org.apache.datasketches.tuple.CompactTupleSketch;
import org.apache.datasketches.tuple.TupleIntersection;
import org.apache.datasketches.tuple.TupleSketchIterator;
import org.apache.datasketches.tuple.TupleUnion;
import org.apache.datasketches.tuple.strings.ArrayOfStringsTupleSketch;
import org.apache.datasketches.tuple.strings.ArrayOfStringsSummary;
import org.apache.datasketches.tuple.strings.ArrayOfStringsSummarySetOperations;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class ArrayOfStringsSketchTest {
  private static final String LS = System.getProperty("line.separator");

  @SuppressWarnings("deprecation")
  @Test
  public void checkSketch() {
    ArrayOfStringsTupleSketch sketch1 = new ArrayOfStringsTupleSketch();
    String[][] strArrArr = {{"a","b"},{"c","d"},{"e","f"}};
    int len = strArrArr.length;
    for (int i = 0; i < len; i++) {
      sketch1.update(i, strArrArr[i]);
    }
    println("Sketch1");
    printSummaries(sketch1.iterator());
    
    sketch1.update(0, strArrArr[0]); //insert duplicate
    println("Sketch1 updated with a duplicate");
    printSummaries(sketch1.iterator());
    
    MemorySegment wseg = MemorySegment.ofArray(sketch1.toByteArray());
    ArrayOfStringsTupleSketch sketch2 = new ArrayOfStringsTupleSketch(wseg);
    println("Sketch2 = Sketch1 via SerDe");
    printSummaries(sketch2.iterator());
    checkSummariesEqual(sketch1, sketch2);

    String[] strArr3 = {"g", "h" };
    sketch2.update(3, strArr3);
    println("Sketch2 with a new row");
    printSummaries(sketch2.iterator());
    
    TupleUnion<ArrayOfStringsSummary> union = new TupleUnion<>(new ArrayOfStringsSummarySetOperations());
    union.union(sketch1);
    union.union(sketch2);
    CompactTupleSketch<ArrayOfStringsSummary> csk = union.getResult();
    println("Result of union of Sketch1, Sketch2");
    printSummaries(csk.iterator());
    assertEquals(csk.getRetainedEntries(), 4);

    TupleIntersection<ArrayOfStringsSummary> inter =
        new TupleIntersection<>(new ArrayOfStringsSummarySetOperations());
    inter.intersect(sketch1);
    inter.intersect(sketch2);
    csk = inter.getResult();
    println("Intersect Sketch1, Sketch2");
    printSummaries(csk.iterator());
    assertEquals(csk.getRetainedEntries(), 3);

    TupleAnotB<ArrayOfStringsSummary> aNotB =  new TupleAnotB<>();
    aNotB.setA(sketch2);
    aNotB.notB(sketch1);
    csk = aNotB.getResult(true);
    println("AnotB(Sketch2, Sketch1)");
    printSummaries(csk.iterator());
    assertEquals(csk.getRetainedEntries(), 1);

  }

  private static void checkSummariesEqual(ArrayOfStringsTupleSketch sk1, ArrayOfStringsTupleSketch sk2) {
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
    println("");
  }

  @Test
  public void checkCopyCtor() {
    ArrayOfStringsTupleSketch sk1 = new ArrayOfStringsTupleSketch();
    String[][] strArrArr = {{"a","b"},{"c","d"},{"e","f"}};
    int len = strArrArr.length;
    for (int i = 0; i < len; i++) {
      sk1.update(i, strArrArr[i]);
    }
    assertEquals(sk1.getRetainedEntries(), 3);
    final ArrayOfStringsTupleSketch sk2 = sk1.copy();
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

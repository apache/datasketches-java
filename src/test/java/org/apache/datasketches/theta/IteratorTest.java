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

package org.apache.datasketches.theta;

import static org.testng.Assert.assertEquals;

import java.lang.foreign.MemorySegment;
import org.testng.annotations.Test;
import org.apache.datasketches.common.Family;
import org.apache.datasketches.theta.CompactSketch;
import org.apache.datasketches.theta.HashIterator;
import org.apache.datasketches.theta.Sketch;

import org.apache.datasketches.theta.UpdateSketch;


/**
 * @author Lee Rhodes
 */
public class IteratorTest {

  @Test
  public void checkDirectCompactSketch() {
    int k = 16;
    int maxBytes = Sketch.getMaxUpdateSketchBytes(k);
    MemorySegment wseg = MemorySegment.ofArray(new byte[maxBytes]);
    UpdateSketch sk1 = UpdateSketch.builder().setNominalEntries(k).build(wseg);
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
    Sketch sk3 = sk1.compact(false, MemorySegment.ofArray(new byte[maxBytes]));
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
    UpdateSketch sk1 = UpdateSketch.builder().setNominalEntries(k).setFamily(Family.ALPHA)
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
    UpdateSketch sk1 = UpdateSketch.builder().setNominalEntries(k)
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
    UpdateSketch sk1 = UpdateSketch.builder().setNominalEntries(k)
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

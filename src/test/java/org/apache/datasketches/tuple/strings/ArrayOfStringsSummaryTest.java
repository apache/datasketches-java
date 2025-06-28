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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.lang.foreign.MemorySegment;

import org.testng.annotations.Test;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.tuple.DeserializeResult;
import org.apache.datasketches.tuple.strings.ArrayOfStringsSummary;
import org.apache.datasketches.tuple.strings.ArrayOfStringsSummaryDeserializer;

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

    MemorySegment seg = MemorySegment.ofArray(out);
    ArrayOfStringsSummary nsum2 = new ArrayOfStringsSummary(seg);
    String[] nodesArr = nsum2.getValue();
    for (String s : nodesArr) {
      println(s);
    }

    println("\nfromMemorySegment(seg)");
    DeserializeResult<ArrayOfStringsSummary> dres = ArrayOfStringsSummaryDeserializer.fromMemorySegment(seg);
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
    MemorySegment seg = MemorySegment.ofArray(new byte[100]);
    ArrayOfStringsSummary.checkInBytes(seg, 200);
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

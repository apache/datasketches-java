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

package org.apache.datasketches.hll2;

import static org.apache.datasketches.hll2.TgtHllType.HLL_4;
import static org.apache.datasketches.hll2.TgtHllType.HLL_6;
import static org.apache.datasketches.hll2.TgtHllType.HLL_8;
import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
@SuppressWarnings("unused")
public class CrossCountingTest {
  static final String LS = System.getProperty("line.separator");

  @Test
  public void crossCountingChecks() {
    crossCountingCheck(4, 100);
    crossCountingCheck(4, 10000);
    crossCountingCheck(12, 7);
    crossCountingCheck(12, 384);
    crossCountingCheck(12, 10000);
  }

  void crossCountingCheck(final int lgK, final int n) {
    final HllSketch sk4 = buildSketch(n, lgK, HLL_4);
    final int s4csum = computeChecksum(sk4);
    //println(sk4.toString(true, true, true, true));
    int csum;

    final HllSketch sk6 = buildSketch(n, lgK, HLL_6);
    csum = computeChecksum(sk6);
    assertEquals(csum, s4csum);
    //println(sk6.toString(true, true, true, true));

    final HllSketch sk8 = buildSketch(n, lgK, HLL_8);
    csum = computeChecksum(sk8);
    assertEquals(csum, s4csum);
    //println(sk8.toString(true, true, true, true));

    //Conversions
//    println("\nConverted HLL_6 to HLL_4:");
    final HllSketch sk6to4 = sk6.copyAs(HLL_4);
    csum = computeChecksum(sk6to4);
    assertEquals(csum, s4csum);
//    println(sk6to4.toString(true, true, true, true));

//    println("\nConverted HLL_8 to HLL_4:");
    final HllSketch sk8to4 = sk8.copyAs(HLL_4);
    csum = computeChecksum(sk8to4);
    assertEquals(csum, s4csum);
//    println(sk8to4.toString(true, true, true, true));

//    println("\nConverted HLL_4 to HLL_6:");
    final HllSketch sk4to6 = sk4.copyAs(HLL_6);
    csum = computeChecksum(sk4to6);
    //println(sk4to6.toString(true, true, true, true));
    assertEquals(csum, s4csum);

//    println("\nConverted HLL_8 to HLL_6:");
    final HllSketch sk8to6 = sk8.copyAs(HLL_6);
    csum = computeChecksum(sk8to6);
    assertEquals(csum, s4csum);
//    println(sk8to6.toString(true, true, true, true));

//    println("\nConverted HLL_4 to HLL_8:");
    final HllSketch sk4to8 = sk4.copyAs(HLL_8);
    csum = computeChecksum(sk4to8);
    assertEquals(csum, s4csum);
//    println(sk4to8.toString(true, true, true, true));

//    println("\nConverted HLL_6 to HLL_8:");
    final HllSketch sk6to8 = sk6.copyAs(HLL_8);
    csum = computeChecksum(sk6to8);
    assertEquals(csum, s4csum);
//    println(sk6to8.toString(true, true, true, true));
  }

  private static HllSketch buildSketch(final int n, final int lgK, final TgtHllType tgtHllType) {
    final HllSketch sketch = new HllSketch(lgK, tgtHllType);
    for (int i = 0; i < n; i++) {
      sketch.update(i);
    }
    return sketch;
  }

  private static int computeChecksum(final HllSketch sketch) {
    final PairIterator itr = sketch.iterator();
    int checksum = 0;
    int key  = 0;
    while (itr.nextAll()) {
      checksum += itr.getPair();
      key = itr.getKey(); //dummy
    }
    return checksum;
  }

  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(final String s) {
    print(s + LS);
  }

  /**
   * @param s value to print
   */
  static void print(final String s) {
    //System.out.print(s); //disable here
  }
}

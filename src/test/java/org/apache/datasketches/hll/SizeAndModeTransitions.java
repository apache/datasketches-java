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

package org.apache.datasketches.hll;

import static org.testng.Assert.assertEquals;

import org.apache.datasketches.memory.WritableMemory;
import org.testng.annotations.Test;

public class SizeAndModeTransitions {
  static String hfmt = "%6s %7s %4s %10s %5s %10s %10s %10s %10s %10s %14s\n";
  static String dfmt = "%6s %7s %4d %,10d %5s %,10d %,10d %,10d %,10d %,10d %,14.3f\n";
  static String[] hdr = {"Type", "Store", "LgK", "N", "Mode", "ActCBytes", "CmpBytes", "ActUBytes", "UpdBytes", "MaxBytes", "Estimate"};

  @Test
  public void checkHLL8Heap() {
    TgtHllType tgtHllType = TgtHllType.HLL_8;
    boolean direct = false;
    int lgK = 10;
    int N = 97;
    printf(hfmt, (Object[]) hdr);
    int maxBytes = HllSketch.getMaxUpdatableSerializationBytes(lgK, tgtHllType);
    WritableMemory wmem = null;;
    HllSketch sk;
    if (direct) {
      wmem = WritableMemory.allocate(maxBytes);
      sk = new HllSketch(lgK, tgtHllType, wmem);
    } else {
      sk = new HllSketch(lgK, tgtHllType);
    }
    String store = direct ? "Memory" : "Heap";
    for (int i = 1; i <= N; i++) {
      sk.update(i);
      if (i == 7)  { checkAtN(sk, tgtHllType, store, lgK, i, "LIST",  36,   40, 1064); }
      if (i == 8)  { checkAtN(sk, tgtHllType, store, lgK, i, "SET",   44,  140, 1064); }
      if (i == 24) { checkAtN(sk, tgtHllType, store, lgK, i, "SET",  108,  140, 1064); }
      if (i == 25) { checkAtN(sk, tgtHllType, store, lgK, i, "SET",  112,  268, 1064); }
      if (i == 48) { checkAtN(sk, tgtHllType, store, lgK, i, "SET",  204,  268, 1064); }
      if (i == 49) { checkAtN(sk, tgtHllType, store, lgK, i, "SET",  208,  524, 1064); }
      if (i == 96) { checkAtN(sk, tgtHllType, store, lgK, i, "SET",  396,  524, 1064); }
      if (i == 97) { checkAtN(sk, tgtHllType, store, lgK, i, "HLL", 1064, 1064, 1064); }
    }
    println("");
  }

  private static void checkAtN(HllSketch sk, TgtHllType tgtHllType, String store, int lgK, int n, String trueMode,
      int cmpTrueBytes, int updTrueBytes, int maxTrueBytes) {
    int maxBytes = HllSketch.getMaxUpdatableSerializationBytes(lgK, tgtHllType);
    String type = tgtHllType.toString();
    int cmpBytes = sk.getCompactSerializationBytes();
    int updBytes = sk.getUpdatableSerializationBytes();
    byte[] actCBytes = sk.toCompactByteArray();
    byte[] actUBytes = sk.toUpdatableByteArray();
    String mode = sk.getCurMode().toString();
    double est = sk.getEstimate();
    printf(dfmt, type, store, lgK, n, mode,  actCBytes.length, cmpBytes, actUBytes.length, updBytes, maxBytes, est);
    assertEquals(mode, trueMode);
    assertEquals(cmpBytes, actCBytes.length);
    assertEquals(cmpBytes, cmpTrueBytes);
    assertEquals(updBytes, actUBytes.length);
    assertEquals(updBytes, updTrueBytes);
    assertEquals(maxBytes, maxTrueBytes);
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

  /**
   * @param fmt format
   * @param args arguments
   */
  static void printf(String fmt, Object...args) {
    //System.out.printf(fmt, args); //disable here
  }

}

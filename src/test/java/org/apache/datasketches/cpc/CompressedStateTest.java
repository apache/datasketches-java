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

package org.apache.datasketches.cpc;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import java.io.PrintStream;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
@SuppressWarnings("javadoc")
public class CompressedStateTest {
  static PrintStream ps = System.out;
  long vIn = 0;
  int lgK = 10;


  @SuppressWarnings("unused")
  private void updateStateUnion(final CpcSketch sk) {
    Format skFmt = sk.getFormat();
    CompressedState state = CompressedState.compress(sk);
    Flavor f = state.getFlavor();
    Format fmt = state.getFormat();
    assertEquals(fmt, skFmt);
    long c = state.numCoupons;
    WritableMemory wmem = WritableMemory.allocate((int)state.getRequiredSerializedBytes());
    state.exportToMemory(wmem);
    printf("%8d %8d %10s %35s\n", vIn, c, f.toString(), fmt.toString());
    CompressedState state2 = CompressedState.importFromMemory(wmem);

    final CpcUnion union = new CpcUnion(lgK);
    union.update(sk);
    final CpcSketch sk2 = union.getResult();
    skFmt = sk2.getFormat();
    state = CompressedState.compress(sk2);
    f = state.getFlavor();
    fmt = state.getFormat();
    assertEquals(fmt, skFmt);
    c = state.numCoupons;
    wmem = WritableMemory.allocate((int)state.getRequiredSerializedBytes());
    state.exportToMemory(wmem);
    printf("%8d %8d %10s %35s\n", vIn, c, f.toString(), fmt.toString());
    state2 = CompressedState.importFromMemory(wmem);
  }

  @Test
  public void checkLoadMemory() {
    printf("%8s %8s %10s %35s\n", "vIn", "c", "Flavor", "Format");
    final CpcSketch sk = new CpcSketch(lgK);
    final int k = 1 << lgK;

    //EMPTY_MERGED
    updateStateUnion(sk);

    //SPARSE
    sk.update(++vIn);
    updateStateUnion(sk);

    //HYBRID

    while ((sk.numCoupons << 5) < (3L * k)) { sk.update(++vIn); }
    updateStateUnion(sk);

    //PINNED
    while ((sk.numCoupons << 1) < k) { sk.update(++vIn); }
    updateStateUnion(sk); //here

    //SLIDING
    while ((sk.numCoupons << 3) < (27L * k)) { sk.update(++vIn); }
    updateStateUnion(sk);
  }

  @Test
  public void checkToString() {
    final CpcSketch sketch = new CpcSketch(10);
    CompressedState state = CompressedState.compress(sketch);
    println(state.toString());
    sketch.update(0);
    state = CompressedState.compress(sketch);
    println(CompressedState.toString(state, true));
    for (int i = 1; i < 600; i++) { sketch.update(i); }
    state = CompressedState.compress(sketch);
    println(CompressedState.toString(state, true));
  }

  @Test
  public void checkIsCompressed() {
    final CpcSketch sk = new CpcSketch(10);
    final byte[] byteArr = sk.toByteArray();
    byteArr[5] &= (byte) -3;
    try {
      CompressedState.importFromMemory(Memory.wrap(byteArr));
      fail();
    } catch (final AssertionError e) { }
  }

  /**
   * @param format the string to print
   * @param args the arguments
   */
  private static void printf(final String format, final Object... args) {
    //ps.printf(format, args);
  }

  /**
   * @param s the string to print
   */
  private static void println(final String s) {
    //ps.println(s);  //disable here
  }

}

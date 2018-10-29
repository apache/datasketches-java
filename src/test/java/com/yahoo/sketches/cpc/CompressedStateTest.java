/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cpc;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import java.io.PrintStream;

import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;

/**
 * @author Lee Rhodes
 */
public class CompressedStateTest {
  static PrintStream ps = System.out;
  long vIn = 0;
  int lgK = 10;


  @SuppressWarnings("unused")
  private void updateStateUnion(CpcSketch sk) {
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

    CpcUnion union = new CpcUnion(lgK);
    union.update(sk);
    CpcSketch sk2 = union.getResult();
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
    CpcSketch sk = new CpcSketch(lgK);
    int k = 1 << lgK;

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
    CpcSketch sketch = new CpcSketch(10);
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
    CpcSketch sk = new CpcSketch(10);
    byte[] byteArr = sk.toByteArray();
    byteArr[5] &= (byte) -3;
    try {
      CompressedState.importFromMemory(Memory.wrap(byteArr));
      fail();
    } catch (AssertionError e) {}
  }

  /**
   * @param format the string to print
   * @param args the arguments
   */
  private static void printf(String format, Object... args) {
    //ps.printf(format, args);
  }

  /**
   * @param s the string to print
   */
  private static void println(String s) {
    //ps.println(s);  //disable here
  }

}

/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.memory;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.testng.annotations.Test;

public class MemoryRegionRTest {

  private static final byte B0 = 0;
  private static final char C0 = 0;
  private static final double D0 = 0.0;
  private static final float F0 = 0.0F;
  private static final int I0 = 0;
  private static final long L0 = 0L;
  private static final short S0 = 0;
  private static final boolean[] BOARR = new boolean[0];
  private static final byte[] BYARR = new byte[0];
  private static final char[] CARR = new char[0];
  private static final double[] DARR = new double[0];
  private static final float[] FARR = new float[0];
  private static final int[] IARR = new int[0];
  private static final long[] LARR = new long[0];
  private static final short[] SARR = new short[0];

  @Test
  public void checkIllegalMethods() {
    NativeMemory mem = new NativeMemory(new byte[8]);
    MemoryRegion reg = new MemoryRegion(mem, 0, mem.getCapacity());
    MemoryRegionR ro = (MemoryRegionR) reg.asReadOnlyMemory();
    try {
      ro.clear();
      fail();
    } catch (ReadOnlyMemoryException e) {
      /* pass */
    }

    try {
      ro.clear(L0, L0);
      fail();
    } catch (ReadOnlyMemoryException e) {
      /* pass */
    }

    try {
      ro.clearBits(L0, B0);
      fail();
    } catch (ReadOnlyMemoryException e) {
      /* pass */
    }

    try {
      ro.copy(L0, ro, L0, L0);
      fail();
    } catch (ReadOnlyMemoryException e) {
      /* pass */
    }

    try {
      ro.fill(L0, L0, B0);
      fail();
    } catch (ReadOnlyMemoryException e) {
      /* pass */
    }

    try {
      ro.fill(B0);
      fail();
    } catch (ReadOnlyMemoryException e) {
      /* pass */
    }

    try {
      ro.putBoolean(L0, false);
      fail();
    } catch (ReadOnlyMemoryException e) {
      /* pass */
    }

    try {
      ro.putBooleanArray(L0, BOARR, I0, I0);
      fail();
    } catch (ReadOnlyMemoryException e) {
      /* pass */
    }

    try {
      ro.putByte(L0, B0);
      fail();
    } catch (ReadOnlyMemoryException e) {
      /* pass */
    }

    try {
      ro.putByteArray(L0, BYARR, I0, I0);
      fail();
    } catch (ReadOnlyMemoryException e) {
      /* pass */
    }

    try {
      ro.putChar(L0, C0);
      fail();
    } catch (ReadOnlyMemoryException e) {
      /* pass */
    }

    try {
      ro.putCharArray(L0, CARR, I0, I0);
      fail();
    } catch (ReadOnlyMemoryException e) {
      /* pass */
    }

    try {
      ro.putDouble(L0, D0);
      fail();
    } catch (ReadOnlyMemoryException e) {
      /* pass */
    }

    try {
      ro.putDoubleArray(L0, DARR, I0, I0);
      fail();
    } catch (ReadOnlyMemoryException e) {
      /* pass */
    }

    try {
      ro.putFloat(L0, F0);
      fail();
    } catch (ReadOnlyMemoryException e) {
      /* pass */
    }

    try {
      ro.putFloatArray(L0, FARR, I0, I0);
      fail();
    } catch (ReadOnlyMemoryException e) {
      /* pass */
    }

    try {
      ro.putFloat(L0, F0);
      fail();
    } catch (ReadOnlyMemoryException e) {
      /* pass */
    }

    try {
      ro.putFloatArray(L0, FARR, I0, I0);
      fail();
    } catch (ReadOnlyMemoryException e) {
      /* pass */
    }

    try {
      ro.putInt(L0, I0);
      fail();
    } catch (ReadOnlyMemoryException e) {
      /* pass */
    }

    try {
      ro.putIntArray(L0, IARR, I0, I0);
      fail();
    } catch (ReadOnlyMemoryException e) {
      /* pass */
    }

    try {
      ro.putLong(L0, L0);
      fail();
    } catch (ReadOnlyMemoryException e) {
      /* pass */
    }

    try {
      ro.putLongArray(L0, LARR, I0, I0);
      fail();
    } catch (ReadOnlyMemoryException e) {
      /* pass */
    }

    try {
      ro.putShort(L0, S0);
      fail();
    } catch (ReadOnlyMemoryException e) {
      /* pass */
    }

    try {
      ro.putShortArray(L0, SARR, I0, I0);
      fail();
    } catch (ReadOnlyMemoryException e) {
      /* pass */
    }

    try {
      ro.setBits(L0, B0);
      fail();
    } catch (ReadOnlyMemoryException e) {
      /* pass */
    }

    //Atomic methods

    try {
      ro.addAndGetInt(L0, I0);
      fail();
    } catch (ReadOnlyMemoryException e) {
      /* pass */
    }

    try {
      ro.addAndGetLong(L0, L0);
      fail();
    } catch (ReadOnlyMemoryException e) {
      /* pass */
    }

    try {
      ro.compareAndSwapInt(L0, I0, I0);
      fail();
    } catch (ReadOnlyMemoryException e) {
      /* pass */
    }

    try {
      ro.compareAndSwapLong(L0, L0, L0);
      fail();
    } catch (ReadOnlyMemoryException e) {
      /* pass */
    }

    try {
      ro.getAndSetInt(L0, I0);
      fail();
    } catch (ReadOnlyMemoryException e) {
      /* pass */
    }

    try {
      ro.getAndSetLong(L0, L0);
      fail();
    } catch (ReadOnlyMemoryException e) {
      /* pass */
    }

    //Non-primitive Memory interface methods

    try {
      ro.array();
      fail();
    } catch (ReadOnlyMemoryException e) {
      /* pass */
    }

    try {
      ro.byteBuffer();
      fail();
    } catch (ReadOnlyMemoryException e) {
      /* pass */
    }

    try {
      ro.getParent();
      fail();
    } catch (ReadOnlyMemoryException e) {
      /* pass */
    }

    try {
      ro.setMemoryRequest(null);
      fail();
    } catch (ReadOnlyMemoryException e) {
      /* pass */
    }
  }

  @Test
  public void checkLegalMethods() {
    NativeMemory mem = new NativeMemory(new byte[8]);
    MemoryRegion reg = new MemoryRegion(mem, 0, mem.getCapacity());
    MemoryRegionR ro = (MemoryRegionR) reg.asReadOnlyMemory();
    assertTrue(ro.isReadOnly());
  }

}

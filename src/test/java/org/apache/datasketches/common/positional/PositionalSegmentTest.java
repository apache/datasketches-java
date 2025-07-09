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

package org.apache.datasketches.common.positional;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_CHAR_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_DOUBLE_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_FLOAT_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_SHORT_UNALIGNED;
import static org.testng.Assert.assertEquals;

import java.lang.foreign.MemorySegment;
import java.util.List;

import org.testng.annotations.Test;
import org.testng.collections.Lists;

public class PositionalSegmentTest {

  @Test
  public void checkBytes() {
    final int n = 100; //elements
    final MemorySegment seg = MemorySegment.ofArray(new byte[n * Byte.BYTES]);
    final PositionalSegment posSeg = PositionalSegment.wrap(seg);
    for (int i = 0; i < (n/2); i++) { posSeg.setByte((byte)i); }
    for (int i = n/2; i < n; i++) { posSeg.setByte(i * Byte.BYTES, (byte)i); }
    posSeg.resetPosition();
    for (int i = 0; i < n; i++) {
      final byte v1 = posSeg.getByte(i * Byte.BYTES);
      final byte v2 = posSeg.getByte();
      assertEquals(v1, v2);
      assertEquals(v2, i);
    }
    posSeg.resetPosition();
    final byte[] arr = new byte[n];
    posSeg.getByteArray(arr, 0, n);

    posSeg.resetPosition();
    posSeg.setByteArray(arr, 0, n);

    final MemorySegment seg2 = posSeg.getMemorySegment();
    for (int i = 0; i < n; i++) {
      assertEquals(seg2.get(JAVA_BYTE, i * Byte.BYTES), (byte)i);
    }
  }

  @Test
  public void checkChars() {
    final int n = 128; //elements
    final MemorySegment seg = MemorySegment.ofArray(new byte[n * Character.BYTES]);
    final PositionalSegment posSeg = PositionalSegment.wrap(seg);
    for (int i = 0; i < (n/2); i++) { posSeg.setChar((char)i); }
    for (int i = n/2; i < n; i++) { posSeg.setChar(i * Character.BYTES, (char)i); }
    posSeg.resetPosition();
    for (int i = 0; i < n; i++) {
      final int v1 = posSeg.getChar(i *Character.BYTES);
      final int v2 = posSeg.getChar();
      assertEquals(v1, v2);
      assertEquals(v2, i);
    }
    posSeg.resetPosition();
    final char[] arr = new char[n];
    posSeg.getCharArray(arr, 0, n);

    posSeg.resetPosition();
    posSeg.setCharArray(arr, 0, n);

    final MemorySegment seg2 = posSeg.getMemorySegment();
    for (int i = 0; i < n; i++) {
      assertEquals(seg2.get(JAVA_CHAR_UNALIGNED, i * Character.BYTES), (char)i);
    }
  }

  @Test
  public void checkShorts() {
    final int n = 128; //elements
    final MemorySegment seg = MemorySegment.ofArray(new byte[n * Short.BYTES]);
    final PositionalSegment posSeg = PositionalSegment.wrap(seg);
    for (int i = 0; i < (n/2); i++) { posSeg.setShort((short)i); }
    for (int i = n/2; i < n; i++) { posSeg.setShort(i * Short.BYTES, (short)i); }
    posSeg.resetPosition();
    for (int i = 0; i < n; i++) {
      final int v1 = posSeg.getShort(i *Short.BYTES);
      final int v2 = posSeg.getShort();
      assertEquals(v1, v2);
      assertEquals(v2, i);
    }
    posSeg.resetPosition();
    final short[] arr = new short[n];
    posSeg.getShortArray(arr, 0, n);

    posSeg.resetPosition();
    posSeg.setShortArray(arr, 0, n);

    final MemorySegment seg2 = posSeg.getMemorySegment();
    for (int i = 0; i < n; i++) {
      assertEquals(seg2.get(JAVA_SHORT_UNALIGNED, i * Short.BYTES), (short)i);
    }
  }

  @Test
  public void checkInts() {
    final int n = 128; //elements
    final MemorySegment seg = MemorySegment.ofArray(new byte[n * Integer.BYTES]);
    final PositionalSegment posSeg = PositionalSegment.wrap(seg);
    for (int i = 0; i < (n/2); i++) { posSeg.setInt((int)i); }
    for (int i = n/2; i < n; i++) { posSeg.setInt(i * Integer.BYTES, (int)i); }
    posSeg.resetPosition();
    for (int i = 0; i < n; i++) {
      final int v1 = posSeg.getInt(i * Integer.BYTES);
      final int v2 = posSeg.getInt();
      assertEquals(v1, v2);
      assertEquals(v2, i);
    }
    posSeg.resetPosition();
    final int[] arr = new int[n];
    posSeg.getIntArray(arr, 0, n);

    posSeg.resetPosition();
    posSeg.setIntArray(arr, 0, n);

    final MemorySegment seg2 = posSeg.getMemorySegment();
    for (int i = 0; i < n; i++) {
      assertEquals(seg2.get(JAVA_INT_UNALIGNED, i * Integer.BYTES), (int)i);
    }
  }


  @Test
  public void checkLongs() {
    final int n = 128; //elements
    final MemorySegment seg = MemorySegment.ofArray(new byte[n * Long.BYTES]);
    final PositionalSegment posSeg = PositionalSegment.wrap(seg);
    for (int i = 0; i < (n/2); i++) { posSeg.setLong(i); }
    for (int i = n/2; i < n; i++) { posSeg.setLong(i * Long.BYTES, i); }
    posSeg.resetPosition();
    for (int i = 0; i < n; i++) {
      final long v1 = posSeg.getLong(i * Long.BYTES);
      final long v2 = posSeg.getLong();
      assertEquals(v1, v2);
      assertEquals(v2, i);
    }
    posSeg.resetPosition();
    final long[] arr = new long[n];
    posSeg.getLongArray(arr, 0, n);

    posSeg.resetPosition();
    posSeg.setLongArray(arr, 0, n);

    final MemorySegment seg2 = posSeg.getMemorySegment();
    for (int i = 0; i < n; i++) {
      assertEquals(seg2.get(JAVA_LONG_UNALIGNED, i * Long.BYTES), i);
    }
  }

  @Test
  public void checkFloats() {
    final int n = 128; //elements
    final MemorySegment seg = MemorySegment.ofArray(new byte[n * Float.BYTES]);
    final PositionalSegment posSeg = PositionalSegment.wrap(seg);
    for (int i = 0; i < (n/2); i++) { posSeg.setFloat(i); }
    for (int i = n/2; i < n; i++) { posSeg.setFloat(i * Float.BYTES, i); }
    posSeg.resetPosition();
    for (int i = 0; i < n; i++) {
      final float v1 = posSeg.getFloat(i * Float.BYTES);
      final float v2 = posSeg.getFloat();
      assertEquals(v1, v2);
      assertEquals(v2, i);
    }
    posSeg.resetPosition();
    final float[] arr = new float[n];
    posSeg.getFloatArray(arr, 0, n);

    posSeg.resetPosition();
    posSeg.setFloatArray(arr, 0, n);

    final MemorySegment seg2 = posSeg.getMemorySegment();
    for (int i = 0; i < n; i++) {
      assertEquals(seg2.get(JAVA_FLOAT_UNALIGNED, i * Float.BYTES), i);
    }
  }

  @Test
  public void checkDoubles() {
    final int n = 128; //elements
    final MemorySegment seg = MemorySegment.ofArray(new byte[n * Double.BYTES]);
    final PositionalSegment posSeg = PositionalSegment.wrap(seg);
    for (int i = 0; i < (n/2); i++) { posSeg.setDouble(i); }
    for (int i = n/2; i < n; i++) { posSeg.setDouble(i * Double.BYTES, i); }
    posSeg.resetPosition();
    for (int i = 0; i < n; i++) {
      final double v1 = posSeg.getDouble(i * Double.BYTES);
      final double v2 = posSeg.getDouble();
      assertEquals(v1, v2);
      assertEquals(v2, i);
    }
    posSeg.resetPosition();
    final double[] arr = new double[n];
    posSeg.getDoubleArray(arr, 0, n);

    posSeg.resetPosition();
    posSeg.setDoubleArray(arr, 0, n);

    final MemorySegment seg2 = posSeg.getMemorySegment();
    for (int i = 0; i < n; i++) {
      assertEquals(seg2.get(JAVA_DOUBLE_UNALIGNED, i * Double.BYTES), i);
    }
  }

  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
  }

  /**
   * @param s String to print
   */
  static void println(final String s) {
    //System.out.println(s);
  }

}

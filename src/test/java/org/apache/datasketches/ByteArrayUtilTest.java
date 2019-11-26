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

package org.apache.datasketches;

import static org.apache.datasketches.ByteArrayUtil.getDoubleBE;
import static org.apache.datasketches.ByteArrayUtil.getDoubleLE;
import static org.apache.datasketches.ByteArrayUtil.getFloatBE;
import static org.apache.datasketches.ByteArrayUtil.getFloatLE;
import static org.apache.datasketches.ByteArrayUtil.getIntBE;
import static org.apache.datasketches.ByteArrayUtil.getIntLE;
import static org.apache.datasketches.ByteArrayUtil.getLongBE;
import static org.apache.datasketches.ByteArrayUtil.getLongLE;
import static org.apache.datasketches.ByteArrayUtil.getShortBE;
import static org.apache.datasketches.ByteArrayUtil.getShortLE;
import static org.apache.datasketches.ByteArrayUtil.putDoubleBE;
import static org.apache.datasketches.ByteArrayUtil.putDoubleLE;
import static org.apache.datasketches.ByteArrayUtil.putFloatBE;
import static org.apache.datasketches.ByteArrayUtil.putFloatLE;
import static org.apache.datasketches.ByteArrayUtil.putIntBE;
import static org.apache.datasketches.ByteArrayUtil.putIntLE;
import static org.apache.datasketches.ByteArrayUtil.putLongBE;
import static org.apache.datasketches.ByteArrayUtil.putLongLE;
import static org.apache.datasketches.ByteArrayUtil.putShortBE;
import static org.apache.datasketches.ByteArrayUtil.putShortLE;
import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
@SuppressWarnings("javadoc")
public class ByteArrayUtilTest {

  @Test
  public void checkGetPutShortLE() {
    final byte[] arr = { 79, -93, 124, 117 };
    final short out1 = getShortLE(arr, 0);
    final short out2 = getShortLE(arr, 2);
    final byte[] arr2 = new byte[4];
    putShortLE(arr2, 0, out1);
    putShortLE(arr2, 2, out2);
    assertEquals(arr2, arr);
  }

  @Test
  public void checkGetPutShortBE() {
    final byte[] arr = { 79, -93, 124, 117 };
    final short out1 = getShortBE(arr, 0);
    final short out2 = getShortBE(arr, 2);
    final byte[] arr2 = new byte[4];
    putShortBE(arr2, 0, out1);
    putShortBE(arr2, 2, out2);
    assertEquals(arr2, arr);
  }

  @Test
  public void checkGetPutIntLE() {
    final byte[] arr = { 79, -93, 124, 117, -73, -100, -114, 77 };
    final int out1 = getIntLE(arr, 0);
    final int out2 = getIntLE(arr, 4);
    final byte[] arr2 = new byte[8];
    putIntLE(arr2, 0, out1);
    putIntLE(arr2, 4, out2);
    assertEquals(arr2, arr);
  }

  @Test
  public void checkGetPutIntBE() {
    final byte[] arr = { 79, -93, 124, 117, -73, -100, -114, 77 };
    final int out1 = getIntBE(arr, 0);
    final int out2 = getIntBE(arr, 4);
    final byte[] arr2 = new byte[8];
    putIntBE(arr2, 0, out1);
    putIntBE(arr2, 4, out2);
    assertEquals(arr2, arr);
  }


  @Test
  public void checkGetPutLongLE() {
    final byte[] arr = { 79, -93, 124, 117, -73, -100, -114, 77, 5, -95, -15, 41, -89, -124, -26, -87 };
    final long out1 = getLongLE(arr, 0);
    final long out2 = getLongLE(arr, 8);
    final byte[] arr2 = new byte[16];
    putLongLE(arr2, 0, out1);
    putLongLE(arr2, 8, out2);
    assertEquals(arr2, arr);
  }

  @Test
  public void checkGetPutLongBE() {
    final byte[] arr = { 79, -93, 124, 117, -73, -100, -114, 77, 5, -95, -15, 41, -89, -124, -26, -87 };
    final long out1 = getLongBE(arr, 0);
    final long out2 = getLongBE(arr, 8);
    final byte[] arr2 = new byte[16];
    putLongBE(arr2, 0, out1);
    putLongBE(arr2, 8, out2);
    assertEquals(arr2, arr);
  }

  @Test
  public void checkGetPutFloatLE() {
    final byte[] arr = { -37, 15, 73, 64, 84, -8, 45, 64 }; //PI, E
    final float out1 = getFloatLE(arr, 0);
    final float out2 = getFloatLE(arr, 4);
    final byte[] arr2 = new byte[8];
    putFloatLE(arr2, 0, out1);
    putFloatLE(arr2, 4, out2);
    assertEquals(arr2, arr);
    assertEquals(out1, (float)Math.PI);
    assertEquals(out2, (float)Math.E);
  }

  @Test
  public void checkGetPutFloatBE() {
    final byte[] arr = { -37, 15, 73, 64, 84, -8, 45, 64 }; //PI, E
    final float out1 = getFloatBE(arr, 0);
    final float out2 = getFloatBE(arr, 4);
    final byte[] arr2 = new byte[8];
    putFloatBE(arr2, 0, out1);
    putFloatBE(arr2, 4, out2);
    assertEquals(arr2, arr);
  }

  @Test
  public void checkGetPutDoubleLE() {
    final byte[] arr = { 24, 45, 68, 84, -5, 33, 9, 64, 105, 87, 20, -117, 10, -65, 5, 64 }; //PI, E
    final double out1 = getDoubleLE(arr, 0);
    final double out2 = getDoubleLE(arr, 8);
    final byte[] arr2 = new byte[16];
    putDoubleLE(arr2, 0, out1);
    putDoubleLE(arr2, 8, out2);
    assertEquals(arr2, arr);
    assertEquals(out1, Math.PI);
    assertEquals(out2, Math.E);
  }

  @Test
  public void checkGetPutDoubleBE() {
    final byte[] arr = { 24, 45, 68, 84, -5, 33, 9, 64, 105, 87, 20, -117, 10, -65, 5, 64 }; //PI, E
    final double out1 = getDoubleBE(arr, 0);
    final double out2 = getDoubleBE(arr, 8);
    final byte[] arr2 = new byte[16];
    putDoubleBE(arr2, 0, out1);
    putDoubleBE(arr2, 8, out2);
    assertEquals(arr2, arr);
  }

}

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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import java.lang.foreign.MemorySegment;

import java.nio.ByteBuffer;

/**
 * @author Lee Rhodes
 *
 */
public class BaseHllSketchTest {

  @Test
  public void checkUpdateTypes() {
    final HllSketch sk = new HllSketch(10);
    final byte[] byteArr = null;
    sk.update(byteArr);
    sk.update(new byte[] {});
    sk.update(new byte[] {0, 1, 2, 3});
    final ByteBuffer byteBuf = null;
    sk.update(byteBuf);
    sk.update(ByteBuffer.wrap(new byte[] {}));
    sk.update(ByteBuffer.wrap(new byte[] {0, 1, 2, 3}));
    final char[] charArr = null;
    sk.update(charArr);
    sk.update(new char[] {});
    sk.update(new char[] {0, 1, 2, 3});
    sk.update(1.0);
    sk.update(-0.0);
    final int[] intArr = null;
    sk.update(intArr);
    sk.update(new int[] {});
    sk.update(new int[] {0, 1, 2, 3});
    sk.update(1234L);
    final long[] longArr = null;
    sk.update(longArr);
    sk.update(new long[] {});
    sk.update(new long[] {0, 1, 2, 3});
    String s = null;
    sk.update(s);
    s = "";
    sk.update(s);
    sk.update("1234");

    final Union u = new Union(10);
    final byte[] byteArr1 = null;
    u.update(byteArr1);
    u.update(new byte[] {});
    u.update(new byte[] {0, 1, 2, 3});
    final ByteBuffer byteBuf1 = null;
    u.update(byteBuf1);
    u.update(ByteBuffer.wrap(new byte[] {}));
    u.update(ByteBuffer.wrap(new byte[] {0, 1, 2, 3}));
    final char[] charArr1 = null;
    u.update(charArr1);
    u.update(new char[] {});
    u.update(new char[] {0, 1, 2, 3});
    u.update(1.0);
    u.update(-0.0);
    final int[] intArr1 = null;
    u.update(intArr1);
    u.update(new int[] {});
    u.update(new int[] {0, 1, 2, 3});
    u.update(1234L);
    final long[] longArr1 = null;
    u.update(longArr1);
    u.update(new long[] {});
    u.update(new long[] {0, 1, 2, 3});
    String s1 = null;
    u.update(s1);
    s1 = "";
    u.update(s);
    u.update("1234");
  }

  @Test
  public void misc() {
    final HllSketch sk = new HllSketch(10, TgtHllType.HLL_4);
    assertTrue(sk.isEstimationMode());
    sk.reset();
    assertEquals(BaseHllSketch.getSerializationVersion(), PreambleUtil.SER_VER);
    final MemorySegment wseg = MemorySegment.ofArray(sk.toCompactByteArray());
    assertEquals(BaseHllSketch.getSerializationVersion(wseg), PreambleUtil.SER_VER);
  }

  @Test
  public void printlnTest() {
    println("PRINTING: " + this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(final String s) {
    //System.out.println(s); //disable here
  }
}

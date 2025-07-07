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

import java.lang.foreign.MemorySegment;
import java.io.PrintStream;

import org.testng.annotations.Test;
import org.apache.datasketches.common.Family;

/**
 * @author Lee Rhodes
 */
public class CpcWrapperTest {
  static PrintStream ps = System.out;

  @SuppressWarnings("unused")
  @Test
  public void check() {
    final int lgK = 10;
    final CpcSketch sk1 = new CpcSketch(lgK);
    final CpcSketch sk2 = new CpcSketch(lgK);
    final CpcSketch skD = new CpcSketch(lgK);
    final double dEst = skD.getEstimate();
    final double dlb = skD.getLowerBound(2);
    final double dub = skD.getUpperBound(2);

    final int n = 100000;
    for (int i = 0; i < n; i++) {
      sk1.update(i);
      sk2.update(i + n);
      skD.update(i);
      skD.update(i + n);
    }
    final byte[] concatArr = skD.toByteArray();

    final CpcUnion union = new CpcUnion(lgK);
    final CpcSketch result = union.getResult();
    final double uEst = result.getEstimate();
    final double ulb = result.getLowerBound(2);
    final double uub = result.getUpperBound(2);
    union.update(sk1);
    union.update(sk2);
    final CpcSketch merged = union.getResult();
    final byte[] mergedArr = merged.toByteArray();

    final MemorySegment concatSeg = MemorySegment.ofArray(concatArr);
    final CpcWrapper concatSk = new CpcWrapper(concatSeg);
    assertEquals(concatSk.getLgK(), lgK);

    printf("              %12s %12s %12s\n", "Lb", "Est", "Ub");
    final double ccEst = concatSk.getEstimate();
    final double ccLb = concatSk.getLowerBound(2);
    final double ccUb = concatSk.getUpperBound(2);
    printf("Concatenated: %12.0f %12.0f %12.0f\n", ccLb, ccEst, ccUb);

    //MemorySegment mergedSeg = MemorySegment.ofArray(mergedArr);
    final CpcWrapper mergedSk = new CpcWrapper(mergedArr);
    final double mEst = mergedSk.getEstimate();
    final double mLb = mergedSk.getLowerBound(2);
    final double mUb = mergedSk.getUpperBound(2);
    printf("Merged:       %12.0f %12.0f %12.0f\n", mLb, mEst, mUb);
    assertEquals(Family.CPC, CpcWrapper.getFamily());
  }

  @SuppressWarnings("unused")
  @Test
  public void checkIsCompressed() {
    final CpcSketch sk = new CpcSketch(10);
    final byte[] byteArr = sk.toByteArray();
    byteArr[5] &= (byte) -3;
    try {
      final CpcWrapper wrapper = new CpcWrapper(MemorySegment.ofArray(byteArr));
      fail();
    } catch (final AssertionError e) {}
  }

  /**
   * @param format the string to print
   * @param args the arguments
   */
  static void printf(final String format, final Object... args) {
    //ps.printf(format, args); //disable here
  }

  /**
   * @param s the string to print
   */
  static void println(final String s) {
    //ps.println(s);  //disable here
  }

}

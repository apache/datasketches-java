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

import static java.lang.Math.min;
import static org.apache.datasketches.hll.TgtHllType.HLL_4;
import static org.apache.datasketches.hll.TgtHllType.HLL_6;
import static org.apache.datasketches.hll.TgtHllType.HLL_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.foreign.MemorySegment;

import org.testng.annotations.Test;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.HllUtil;
import org.apache.datasketches.hll.RelativeErrorTables;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.datasketches.hll.Union;

/**
 * @author Lee Rhodes
 */
public class DirectUnionTest {
  static final String LS = System.getProperty("line.separator");

  static final int[] nArr = {1, 3, 10, 30, 100, 300, 1000, 3000, 10000, 30000};

  //   n1,...        lgK,...          tgtHll,    Mode         Ooo          Est
  static final String hdrFmt =
      "%6s%6s%6s" + "%8s%5s%5s%5s" + "%7s%6s" + "%7s%6s%6s" +"%3s%2s%2s"+ "%13s%12s";
  static final String hdr = String.format(hdrFmt,
      "n1", "n2", "tot",
      "lgMaxK", "lgK1", "lgK2", "lgKR",
      "tgt1", "tgt2",
      "Mode1", "Mode2", "ModeR",
      "1", "2", "R",
      "Est", "Err%");

  /**
   * The task here is to check the transition boundaries as the sketch morphs between LIST to
   * SET to HLL modes. The transition points vary as a function of lgConfigK. In addition,
   * this checks that the union operation is operating properly based on the order the
   * sketches are presented to the union.
   */
  @Test
  public void checkUnions() {
    //HLL_4: t=0,  HLL_6: t=1, HLL_8: t=2
    final int t1 = 2; //type = HLL_8
    final int t2 = 2;
    final int rt = 2; //result type
    println("TgtR: " + TgtHllType.values()[rt].toString());

    int lgK1 = 7;
    int lgK2 = 7;
    int lgMaxK = 7;
    int n1 = 7;
    int n2 = 7;
    basicUnion(n1, n2, lgK1, lgK2, lgMaxK, t1, t2, rt);
    n1 = 8;
    n2 = 7;
    basicUnion(n1, n2, lgK1, lgK2, lgMaxK, t1, t2, rt);
    n1 = 7;
    n2 = 8;
    basicUnion(n1, n2, lgK1, lgK2, lgMaxK, t1, t2, rt);
    n1 = 8;
    n2 = 8;
    basicUnion(n1, n2, lgK1, lgK2, lgMaxK, t1, t2, rt);
    n1 = 7;
    n2 = 14;
    basicUnion(n1, n2, lgK1, lgK2, lgMaxK, t1, t2, rt);
    println("++END BASE GROUP++");

    int i = 0;
    for (i = 7; i <= 13; i++)
    {
      lgK1 = i;
      lgK2 = i;
      lgMaxK = i;
      {
        n1 = ((1 << (i - 3)) * 3)/4; //compute the transition point
        n2 = n1;
        basicUnion(n1, n2, lgK1, lgK2, lgMaxK, t1, t2, rt);
        n1 += 2;
        basicUnion(n1, n2, lgK1, lgK2, lgMaxK, t1, t2, rt);
        n1 -= 2;
        n2 += 2;
        basicUnion(n1, n2, lgK1, lgK2, lgMaxK, t1, t2, rt);
        n1 += 2;
        basicUnion(n1, n2, lgK1, lgK2, lgMaxK, t1, t2, rt);
      }
      println("--END MINOR GROUP--");
      lgK1 = i;
      lgK2 = i + 1;
      lgMaxK = i;
      {
        n1 = ((1 << (i - 3)) * 3)/4;
        n2 = n1;
        basicUnion(n1, n2, lgK1, lgK2, lgMaxK, t1, t2, rt);
        n1 += 2;
        basicUnion(n1, n2, lgK1, lgK2, lgMaxK, t1, t2, rt);
        n1 -= 2;
        n2 += 2;
        basicUnion(n1, n2, lgK1, lgK2, lgMaxK, t1, t2, rt);
        n1 += 2;
        basicUnion(n1, n2, lgK1, lgK2, lgMaxK, t1, t2, rt);
      }
      println("--END MINOR GROUP--");
      lgK1 = i + 1;
      lgK2 = i;
      lgMaxK = i;
      {
        n1 = ((1 << (i - 3)) * 3)/4;
        n2 = n1;
        basicUnion(n1, n2, lgK1, lgK2, lgMaxK, t1, t2, rt);
        n1 += 2;
        basicUnion(n1, n2, lgK1, lgK2, lgMaxK, t1, t2, rt);
        n1 -= 2;
        n2 += 2;
        basicUnion(n1, n2, lgK1, lgK2, lgMaxK, t1, t2, rt);
        n1 += 2;
        basicUnion(n1, n2, lgK1, lgK2, lgMaxK, t1, t2, rt);
      }
      println("--END MINOR GROUP--");
      lgK1 = i + 1;
      lgK2 = i + 1;
      lgMaxK = i;
      {
        n1 = ((1 << (i - 3)) * 3)/4;
        n2 = n1;
        basicUnion(n1, n2, lgK1, lgK2, lgMaxK, t1, t2, rt);
        n1 += 2;
        basicUnion(n1, n2, lgK1, lgK2, lgMaxK, t1, t2, rt);
        n1 -= 2;
        n2 += 2;
        basicUnion(n1, n2, lgK1, lgK2, lgMaxK, t1, t2, rt);
        n1 += 2;
        basicUnion(n1, n2, lgK1, lgK2, lgMaxK, t1, t2, rt);
      }
      println("++END MAJOR GROUP++");
    } //End for
  }

  @Test
  public void check() { //n1=8, n2=7, lgK1=lgK2=lgMaxK=7, all HLL_8
    basicUnion(8, 7,   7, 7, 7,  2, 2,  2);
  }

  private static void basicUnion(final int n1, final int n2, final int lgK1, final int lgK2,
      final int lgMaxK, final int t1, final int t2, final int rt)  {
    long v = 0;
    final int tot = n1 + n2;
    final TgtHllType type1 = TgtHllType.values()[t1];
    final String t1str = type1.toString();
    final TgtHllType type2 = TgtHllType.values()[t2];
    final String t2str = type2.toString();
    final TgtHllType resultType = TgtHllType.values()[rt];
    //String rtStr = resultType.toString();

    final HllSketch h1 = new HllSketch(lgK1, type1);
    final HllSketch h2 = new HllSketch(lgK2, type2);
    final int lgControlK = min(min(lgK1, lgK2), lgMaxK); //min of all 3
    final HllSketch control = new HllSketch(lgControlK, resultType);
    final String dataFmt = "%6d%6d%6d," + "%7d%5d%5d%5d," + "%6s%6s," + "%6s%6s%6s,"
        +"%2s%2s%2s," + "%12f%12f%%";

    for (long i = 0; i < n1; i++) {
      h1.update(v + i);
      control.update(v + i);
    }
    v += n1;
    for (long i = 0; i < n2; i++) {
      h2.update(v + i);
      control.update(v + i);
    }
    v += n2;

    final String h1SketchStr = ("H1 SKETCH: \n" + h1.toString());
    final String h2SketchStr = ("H2 SKETCH: \n" + h2.toString());

    final Union union = newUnion(lgMaxK);
    union.update(h1);

    final String uH1SketchStr = ("Union after H1: \n" + union.getResult(resultType).toString());
    //println(uH1SketchStr);

    union.update(h2);
    final HllSketch result = union.getResult(resultType);
    final int lgKR = result.getLgConfigK();

    final String uSketchStr =("Union after H2: \n" + result.toString());

    final double uEst = result.getEstimate();
    final double uUb = result.getUpperBound(2);
    final double uLb = result.getLowerBound(2);
    final double rerr = ((uEst/tot) - 1.0) * 100;
    final String mode1 = h1.getCurMode().toString();
    final String mode2 = h2.getCurMode().toString();
    final String modeR = result.getCurMode().toString();

    //Control
    final String cSketchStr = ("CONTROL SKETCH: \n" + control.toString());
    final double controlEst = control.getEstimate();
    final double controlUb = control.getUpperBound(2);
    final double controlLb = control.getLowerBound(2);
    final String h1ooo = h1.isOutOfOrder() ? "T" : "F";
    final String h2ooo = h2.isOutOfOrder() ? "T" : "F";
    final String resultooo = result.isOutOfOrder() ?  "T" : "F";
    final String row = String.format(dataFmt,
        n1, n2, tot,
        lgMaxK, lgK1, lgK2, lgKR,
        t1str, t2str,
        mode1, mode2, modeR,
        h1ooo, h2ooo, resultooo,
        uEst, rerr);
    println(h1SketchStr);
    println(h2SketchStr);
    println(uH1SketchStr);
    println(uSketchStr);
    println(cSketchStr);
    println(hdr);
    println(row);
    assertTrue((controlUb - controlEst) >= 0);
    assertTrue((uUb - uEst) >= 0);
    assertTrue((controlEst - controlLb) >= 0);
    assertTrue((uEst -uLb) >= 0);
  }

  @Test
  public void checkToFromUnion1() {
    for (int i = 0; i < 10; i++) {
      final int n = nArr[i];
      for (int lgK = 4; lgK <= 13; lgK++) {
        toFrom1(lgK, HLL_4, n);
        toFrom1(lgK, HLL_6, n);
        toFrom1(lgK, HLL_8, n);
      }
      println("=======");
    }
  }

  private static void toFrom1(final int lgK, final TgtHllType tgtHllType, final int n) {
    final Union srcU = newUnion(lgK);
    final HllSketch srcSk = new HllSketch(lgK, tgtHllType);
    for (int i = 0; i < n; i++) {
      srcSk.update(i);
    }
    println("n: " + n + ", lgK: " + lgK + ", type: " + tgtHllType);
    //printSketch(src, "SRC");
    srcU.update(srcSk);

    final byte[] byteArr = srcU.toCompactByteArray();
    final MemorySegment seg = MemorySegment.ofArray(byteArr);
    final Union dstU = Union.heapify(seg);

    assertEquals(dstU.getEstimate(), srcU.getEstimate(), 0.0);
  }

  @Test
  public void checkToFromUnion2() {
    for (int i = 0; i < 10; i++) {
      final int n = nArr[i];
      for (int lgK = 4; lgK <= 13; lgK++) {
        toFrom2(lgK, HLL_4, n);
        toFrom2(lgK, HLL_6, n);
        toFrom2(lgK, HLL_8, n);
      }
      println("=======");
    }
  }

  private static void toFrom2(final int lgK, final TgtHllType tgtHllType, final int n) {
    final Union srcU = newUnion(lgK);
    final HllSketch srcSk = new HllSketch(lgK, tgtHllType);
    for (int i = 0; i < n; i++) {
      srcSk.update(i);
    }
    println("n: " + n + ", lgK: " + lgK + ", type: " + tgtHllType);
    //printSketch(src, "SRC");
    srcU.update(srcSk);

    final byte[] byteArr = srcU.toCompactByteArray();
    final Union dstU = Union.heapify(byteArr);

    assertEquals(dstU.getEstimate(), srcU.getEstimate(), 0.0);
  }

  @Test
  public void checkCompositeEst() {
    final Union u = newUnion(12);
    assertEquals(u.getCompositeEstimate(), 0, .03);
    for (int i = 1; i <= 15; i++) { u.update(i); }
    assertEquals(u.getCompositeEstimate(), 15, 15 *.03);
    for (int i = 15; i <= 1000; i++) { u.update(i); }
    assertEquals(u.getCompositeEstimate(), 1000, 1000 * .03);
  }

  @SuppressWarnings("unused")
  @Test
  public void checkMisc() {
    try {
      final Union u = newUnion(HllUtil.MIN_LOG_K - 1);
      fail();
    } catch (final SketchesArgumentException e) {
      //expected
    }
    try {
      final Union u = newUnion(HllUtil.MAX_LOG_K + 1);
      fail();
    } catch (final SketchesArgumentException e) {
      //expected
    }
    final Union u = newUnion(7);
    final HllSketch sk = u.getResult();
    assertTrue(sk.isEmpty());
  }

  @Test
  public void checkHeapify() {
    final Union u = newUnion(16);
    for (int i = 0; i < (1 << 20); i++) {
      u.update(i);
    }
    final double est1 = u.getEstimate();
    final byte[] byteArray = u.toUpdatableByteArray();
    final Union u2 = Union.heapify(byteArray);
    assertEquals(u2.getEstimate(), est1, 0.0);
  }

  @Test //for lgK <= 12
  public void checkUbLb() {
    final int lgK = 4;
    final int n = 1 << 20;
    final boolean oooFlag = false;
    println("LgK="+lgK+", UB3, " + ((getBound(lgK, true, oooFlag, 3, n) / n) - 1));
    println("LgK="+lgK+", UB2, " + ((getBound(lgK, true, oooFlag, 2, n) / n) - 1));
    println("LgK="+lgK+", UB1, " + ((getBound(lgK, true, oooFlag, 1, n) / n) - 1));
    println("LgK="+lgK+", LB1, " + ((getBound(lgK, false, oooFlag, 1, n) / n) - 1));
    println("LgK="+lgK+", LB2, " + ((getBound(lgK, false, oooFlag, 2, n) / n) - 1));
    println("LgK="+lgK+", LB3, " + ((getBound(lgK, false, oooFlag, 3, n) / n) - 1));
  }

  @Test
  public void checkEmptyCouponMisc() {
    final int lgK = 8;
    final Union union = newUnion(lgK);
    for (int i = 0; i < 20; i++) { union.update(i); } //SET mode
    union.couponUpdate(0);
    assertEquals(union.getEstimate(), 20.0, 0.001);
    assertEquals(union.getTgtHllType(), TgtHllType.HLL_8);
    assertTrue(union.hasMemorySegment());
    assertFalse(union.isOffHeap());
    final int bytes = union.getUpdatableSerializationBytes();
    assertTrue(bytes  <= Union.getMaxSerializationBytes(lgK));
    assertFalse(union.isCompact());
  }

  @Test
  public void checkUnionWithWrap() {
    final int lgConfigK = 4;
    final TgtHllType type = TgtHllType.HLL_4;
    final int n = 2;
    final HllSketch sk = new HllSketch(lgConfigK, type);
    for (int i = 0; i < n; i++) { sk.update(i); }
    final double est = sk.getEstimate();
    final byte[] skByteArr = sk.toCompactByteArray();

    final HllSketch sk2 = HllSketch.wrap(MemorySegment.ofArray(skByteArr));
    assertEquals(sk2.getEstimate(), est, 0.0);

    final Union union = newUnion(lgConfigK);
    union.update(HllSketch.wrap(MemorySegment.ofArray(skByteArr)));
    assertEquals(union.getEstimate(), est, 0.0);
  }

  @Test
  public void checkUnionWithWrap2() {
    final int lgConfigK = 10;
    final int n = 128;
    final HllSketch sk1 = new HllSketch(lgConfigK);
    for (int i = 0; i < n; i++) { sk1.update(i); }
    final double est1 = sk1.getEstimate();
    final byte[] byteArr1 = sk1.toCompactByteArray();

    final Union union = newUnion(lgConfigK);
    union.update(HllSketch.wrap(MemorySegment.ofArray(byteArr1)));
    final double est2 = union.getEstimate();
    assertEquals(est2, est1);
  }

  @Test
  public void checkWritableWrap() {
    final int lgConfigK = 10;
    final int n = 128;
    final Union union = newUnion(lgConfigK);
    for (int i = 0; i < n; i++) { union.update(i); }
    final double est = union.getEstimate();
    final Union union2 = Union.writableWrap(MemorySegment.ofArray(union.toUpdatableByteArray()));
    final double est2 = union2.getEstimate();
    assertEquals(est2, est, 0.0);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkWritableWrapThrows() {
    final int lgConfigK = 10;
    final int n = 128;
    final HllSketch sk = new HllSketch(lgConfigK, HLL_6);
    for (int i = 0; i < n; i++) {sk.update(i); }
    Union.writableWrap(MemorySegment.ofArray(sk.toUpdatableByteArray()));
  }

  private static Union newUnion(final int lgK) {
    final int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgK, TgtHllType.HLL_8);
    final MemorySegment wseg = MemorySegment.ofArray(new byte[bytes]);
    return new Union(lgK, wseg);
  }

  private static double getBound(final int lgK, final boolean ub, final boolean oooFlag, final int numStdDev, final double est) {
    final double re = RelativeErrorTables.getRelErr(ub, oooFlag, lgK, numStdDev);
    return est / (1.0 + re);
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

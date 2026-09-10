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

import static org.apache.datasketches.common.UtilityIO.CHECK_CPP_FILES;
import static org.apache.datasketches.common.UtilityIO.CHECK_GO_FILES;
import static org.apache.datasketches.common.UtilityIO.CHECK_RUST_FILES;
import static org.apache.datasketches.common.UtilityIO.CHECK_JAVA_FILES;
import static org.apache.datasketches.common.UtilityIO.GENERATE_JAVA_FILES;
import static org.apache.datasketches.common.UtilityIO.getFileBytes;
import static org.apache.datasketches.common.UtilityIO.putBytesToJavaPath;
import static org.apache.datasketches.hll.TgtHllType.HLL_4;
import static org.apache.datasketches.hll.TgtHllType.HLL_6;
import static org.apache.datasketches.hll.TgtHllType.HLL_8;
import static org.apache.datasketches.hll.PreambleUtil.COMPACT_FLAG_MASK;
import static org.apache.datasketches.hll.PreambleUtil.FLAGS_BYTE;
import static org.apache.datasketches.hll.PreambleUtil.REBUILD_CURMIN_NUM_KXQ_MASK;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.UtilityIO.GroupLanguage;
import org.testng.annotations.Test;

/**
 * Serialize binary sketches to be tested by C++ code.
 * Test deserialization of binary sketches serialized by C++ code.
 */
public class HllSketchCrossLanguageTest {

  private static final int LG_K = HllSketch.DEFAULT_LG_K;
  private static final int UNION_DISTINCT = 6000; //0..999 and 2000..6999; the SET-mode input overlaps

  @Test(groups = {GENERATE_JAVA_FILES}, priority = 0)
  public void generateBinariesForCompatibilityTesting() throws IOException {
    final int[] nArr = {0, 1, 10, 100, 1000, 10_000, 100_000, 1_000_000};
    for (final int n: nArr) {
      final HllSketch hll4 = new HllSketch(HllSketch.DEFAULT_LG_K, HLL_4);
      final HllSketch hll6 = new HllSketch(HllSketch.DEFAULT_LG_K, HLL_6);
      final HllSketch hll8 = new HllSketch(HllSketch.DEFAULT_LG_K, HLL_8);
      for (int i = 0; i < n; i++) {
        hll4.update(i);
      }
      for (int i = 0; i < n; i++) {
        hll6.update(i);
      }
      for (int i = 0; i < n; i++) {
        hll8.update(i);
      }
      putBytesToJavaPath("hll4_n" + n + "_java.sk", hll4.toCompactByteArray());
      putBytesToJavaPath("hll6_n" + n + "_java.sk", hll6.toCompactByteArray());
      putBytesToJavaPath("hll8_n" + n + "_java.sk", hll8.toCompactByteArray());
    }
  }

  @Test(groups = {CHECK_JAVA_FILES}, priority = 1)
  public void checkJava() {
    deserializeHll(GroupLanguage.JAVA);
  }

  @Test(groups = {CHECK_CPP_FILES})
  public void checkCpp() {
    deserializeHll(GroupLanguage.CPP);
  }

  @Test(groups = {CHECK_GO_FILES})
  public void checkGo() {
    deserializeHll(GroupLanguage.GO);
  }

  private static void deserializeHll(final GroupLanguage lang) {
    final String[] sArr = {"hll4", "hll6", "hll8"};
    final int[] nArr = {0, 1, 10, 100, 1000, 10000, 100000, 1000000};
    for (final String s: sArr) {
      for (final int n: nArr) {
        final String fileName = s + "_n" + n + lang.sfx + ".sk";
        final byte[] bytes = getFileBytes(lang.pth, fileName);
        if (bytes.length == 0) { continue;}
        //System.out.println(fileName);
        final HllSketch sketch = HllSketch.heapify(MemorySegment.ofArray(bytes));
        assertEquals(sketch.getLgConfigK(), 12);
        assertTrue(n == 0 ? sketch.isEmpty() : !sketch.isEmpty());
        assertEquals(sketch.getEstimate(), n, n * 0.02);
      }
    }
  }

  /**
   * The fixtures above are update sketches only, serialized compact, at a single lgK. That
   * leaves two gaps a cross-language reader can fall through, both of which produced real bugs
   * in 2026-09: the deferred KxQ rebuild is reachable only through a union gadget, and the
   * flags-byte bit 32 collision is only visible in the flags byte itself. These fixtures close
   * both. One lgK is deliberate; the defect classes are not lgK-dependent.
   */
  @Test(groups = {GENERATE_JAVA_FILES}, priority = 0)
  public void generateUnionBinariesForCompatibilityTesting() throws IOException {
    for (final TgtHllType type: new TgtHllType[] {HLL_4, HLL_6, HLL_8}) {
      final HllUnion union = mixedModeUnion();
      final String tag = tagOf(type);
      putBytesToJavaPath(tag + "_union_java.sk", union.getResult(type).toCompactByteArray());
      putBytesToJavaPath(tag + "_unionupd_java.sk", union.getResult(type).toUpdatableByteArray());
    }
  }

  @Test(groups = {CHECK_JAVA_FILES}, priority = 1)
  public void checkUnionJava() { deserializeUnionResults(GroupLanguage.JAVA); }

  @Test(groups = {CHECK_CPP_FILES})
  public void checkUnionCpp() { deserializeUnionResults(GroupLanguage.CPP); }

  @Test(groups = {CHECK_GO_FILES})
  public void checkUnionGo() { deserializeUnionResults(GroupLanguage.GO); }

  @Test(groups = {CHECK_RUST_FILES})
  public void checkUnionRust() { deserializeUnionResults(GroupLanguage.RUST); }

  /**
   * A union whose inputs span all three curModes, so the coupon-update path into a gadget with
   * a pending KxQ rebuild is exercised. The second sketch stays in SET mode and overlaps the
   * first entirely, so the true distinct count is UNION_DISTINCT.
   */
  private static HllUnion mixedModeUnion() {
    final HllUnion union = new HllUnion(LG_K);
    final HllSketch big = new HllSketch(LG_K, HLL_8);
    for (int i = 0; i < 1000; i++) { big.update(i); }
    final HllSketch small = new HllSketch(LG_K, HLL_8);
    for (int i = 0; i < 20; i++) { small.update(500 + i); } //stays in SET mode, fully overlapping
    final HllSketch other = new HllSketch(LG_K, HLL_8);
    for (int i = 0; i < 5000; i++) { other.update(2000 + i); }
    union.update(big);
    union.update(small);
    union.update(other);
    return union;
  }

  private static String tagOf(final TgtHllType type) {
    return type == HLL_4 ? "hll4" : type == HLL_6 ? "hll6" : "hll8";
  }

  /**
   * One body, reused by every language. Beyond deserializing, this asserts the two flags-byte
   * invariants the 2026-09 fixes established: bit 32 is reserved and must never be written by
   * any implementation, and the compact flag must reflect the form actually produced.
   */
  private static void deserializeUnionResults(final GroupLanguage lang) {
    final String[] tags = {"hll4", "hll6", "hll8"};
    final String[] forms = {"_union", "_unionupd"};
    double firstEstimate = Double.NaN;
    for (final String form: forms) {
      for (final String tag: tags) {
        final String fileName = tag + form + lang.sfx + ".sk";
        final byte[] bytes = getFileBytes(lang.pth, fileName);
        if (bytes.length == 0) { continue; }
        final int flags = bytes[FLAGS_BYTE] & 0xFF;

        //bit 32 collided across implementations: REBUILD_CURMIN_NUM_KXQ here, FULL_SIZE in C++.
        //Java clears it before serializing and C++ no longer writes it, so no image carries it.
        assertFalse((flags & REBUILD_CURMIN_NUM_KXQ_MASK) > 0,
            fileName + " has reserved flags bit 32 set");

        //HLL_6 and HLL_8 used to omit this on toCompactByteArray()
        assertEquals((flags & COMPACT_FLAG_MASK) > 0, form.equals("_union"),
            fileName + " compact flag does not match its serialized form");

        final HllSketch sketch = HllSketch.heapify(MemorySegment.ofArray(bytes));
        assertEquals(sketch.getLgConfigK(), LG_K);
        assertTrue(!sketch.isEmpty());
        assertEquals(sketch.getEstimate(), UNION_DISTINCT, UNION_DISTINCT * 0.02);

        //an incorrect KxQ rebuild shifts the estimate, and it must not depend on target type
        if (Double.isNaN(firstEstimate)) { firstEstimate = sketch.getEstimate(); }
        else { assertEquals(sketch.getEstimate(), firstEstimate, 1e-9, fileName); }
      }
    }
  }

}

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

package org.apache.datasketches.theta2;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_FLOAT_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static org.apache.datasketches.theta2.CompactOperations.checkIllegalCurCountAndEmpty;
import static org.apache.datasketches.theta2.CompactOperations.computeCompactPreLongs;
import static org.apache.datasketches.theta2.CompactOperations.correctThetaOnCompact;
import static org.apache.datasketches.theta2.PreambleUtil.FAMILY_BYTE;
import static org.apache.datasketches.theta2.PreambleUtil.LG_ARR_LONGS_BYTE;
import static org.apache.datasketches.theta2.PreambleUtil.LG_NOM_LONGS_BYTE;
import static org.apache.datasketches.theta2.PreambleUtil.LG_RESIZE_FACTOR_BIT;
import static org.apache.datasketches.theta2.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static org.apache.datasketches.theta2.PreambleUtil.P_FLOAT;
import static org.apache.datasketches.theta2.PreambleUtil.RETAINED_ENTRIES_INT;
import static org.apache.datasketches.theta2.PreambleUtil.THETA_LONG;
import static org.apache.datasketches.theta2.PreambleUtil.extractCurCount;
import static org.apache.datasketches.theta2.PreambleUtil.extractLgArrLongs;
import static org.apache.datasketches.theta2.PreambleUtil.extractLgNomLongs;
import static org.apache.datasketches.theta2.PreambleUtil.extractPreLongs;
import static org.apache.datasketches.theta2.PreambleUtil.extractThetaLong;
import static org.apache.datasketches.theta2.PreambleUtil.insertThetaLong;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.ResizeFactor;
import org.apache.datasketches.common.SketchesReadOnlyException;
import org.apache.datasketches.common.SuppressFBWarnings;
import org.apache.datasketches.common.Util;
import org.apache.datasketches.thetacommon2.ThetaUtil;

/**
 * The default Theta Sketch using the QuickSelect algorithm.
 * This is the read-only implementation with non-functional methods, which affect the state.
 *
 * <p>This implementation uses data in a given MemorySegment that is owned and managed by the caller.
 * This MemorySegment can be off-heap, which if managed properly will greatly reduce the need for
 * the JVM to perform garbage collection.</p>
 *
 * @author Lee Rhodes
 * @author Kevin Lang
 */
class DirectQuickSelectSketchR extends UpdateSketch {
  static final double DQS_RESIZE_THRESHOLD  = 15.0 / 16.0; //tuned for space
  final long seed_; //provided, kept only on heap, never serialized.
  int hashTableThreshold_; //computed, kept only on heap, never serialized.
  MemorySegment wseg_; //A MemorySegment for child class, but no write methods here

  //only called by DirectQuickSelectSketch and below
  DirectQuickSelectSketchR(final long seed, final MemorySegment wseg) {
    seed_ = seed;
    wseg_ = wseg;
  }

  /**
   * Wrap a sketch around the given source MemorySegment containing sketch data that originated from
   * this sketch.
   * @param srcSeg the source MemorySegment.
   * The given MemorySegment object must be in hash table form and not read only.
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>
   * @return instance of this sketch
   */
  static DirectQuickSelectSketchR readOnlyWrap(final MemorySegment srcSeg, final long seed) {
    final int preambleLongs = extractPreLongs(srcSeg);                  //byte 0
    final int lgNomLongs = extractLgNomLongs(srcSeg);                   //byte 3
    final int lgArrLongs = extractLgArrLongs(srcSeg);                   //byte 4

    UpdateSketch.checkUnionQuickSelectFamily(srcSeg, preambleLongs, lgNomLongs);
    checkSegIntegrity(srcSeg, seed, preambleLongs, lgNomLongs, lgArrLongs);

    final DirectQuickSelectSketchR dqssr =
        new DirectQuickSelectSketchR(seed, srcSeg);
    dqssr.hashTableThreshold_ = getOffHeapHashTableThreshold(lgNomLongs, lgArrLongs);
    return dqssr;
  }

  /**
   * Fast-wrap a sketch around the given source MemorySegment containing sketch data that originated from
   * this sketch.  This does NO validity checking of the given MemorySegment.
   * @param srcSeg The given MemorySegment object must be in hash table form and not read only.
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>
   * @return instance of this sketch
   */
  static DirectQuickSelectSketchR fastReadOnlyWrap(final MemorySegment srcSeg, final long seed) {
    final int lgNomLongs = srcSeg.get(JAVA_BYTE, LG_NOM_LONGS_BYTE) & 0XFF;
    final int lgArrLongs = srcSeg.get(JAVA_BYTE, LG_ARR_LONGS_BYTE) & 0XFF;

    final DirectQuickSelectSketchR dqss =
        new DirectQuickSelectSketchR(seed, srcSeg);
    dqss.hashTableThreshold_ = getOffHeapHashTableThreshold(lgNomLongs, lgArrLongs);
    return dqss;
  }

  //Sketch

  @Override
  public int getCurrentBytes() {
    //not compact
    final byte lgArrLongs = wseg_.get(JAVA_BYTE, LG_ARR_LONGS_BYTE);
    final int preLongs = wseg_.get(JAVA_BYTE, PREAMBLE_LONGS_BYTE) & 0X3F;
    final int lengthBytes = (preLongs + (1 << lgArrLongs)) << 3;
    return lengthBytes;
  }

  @Override
  public double getEstimate() {
    final int curCount = extractCurCount(wseg_);
    final long thetaLong = extractThetaLong(wseg_);
    return Sketch.estimate(thetaLong, curCount);
  }

  @Override
  public Family getFamily() {
    final int familyID = wseg_.get(JAVA_BYTE, FAMILY_BYTE) & 0XFF;
    return Family.idToFamily(familyID);
  }

  @Override
  public int getRetainedEntries(final boolean valid) { //always valid
    return wseg_.get(JAVA_INT_UNALIGNED, RETAINED_ENTRIES_INT);
  }

  @Override
  public long getThetaLong() {
    return isEmpty() ? Long.MAX_VALUE : wseg_.get(JAVA_LONG_UNALIGNED, THETA_LONG);
  }

  @Override
  public boolean hasMemorySegment() {
    return wseg_ != null && wseg_.scope().isAlive();
  }

  @Override
  public boolean isDirect() {
    return hasMemorySegment() && wseg_.isNative();
  }

  @Override
  public boolean isEmpty() {
    return PreambleUtil.isEmptyFlag(wseg_);
  }

  @Override
  public boolean isSameResource(final MemorySegment that) {
    return hasMemorySegment() && Util.isSameResource(wseg_, that);
  }

  @Override
  public HashIterator iterator() {
    return new MemorySegmentHashIterator(wseg_, 1 << getLgArrLongs(), getThetaLong());
  }

  @Override
  public byte[] toByteArray() { //MY_FAMILY is stored in wseg_
    checkIllegalCurCountAndEmpty(isEmpty(), extractCurCount(wseg_));
    final int lengthBytes = getCurrentBytes();
    final byte[] byteArray = new byte[lengthBytes];
    final MemorySegment seg = MemorySegment.ofArray(byteArray);
    MemorySegment.copy(wseg_, 0, seg, 0, lengthBytes);
    final long thetaLong =
        correctThetaOnCompact(isEmpty(), extractCurCount(wseg_), extractThetaLong(wseg_));
    insertThetaLong(wseg_, thetaLong);
    return byteArray;
  }

  //UpdateSketch

  @Override
  public final int getLgNomLongs() {
    return PreambleUtil.extractLgNomLongs(wseg_);
  }

  @Override
  float getP() {
    return wseg_.get(JAVA_FLOAT_UNALIGNED, P_FLOAT);
  }

  @Override
  public ResizeFactor getResizeFactor() {
    return ResizeFactor.getRF(getLgRF());
  }

  @Override
  long getSeed() {
    return seed_;
  }

  @Override
  public UpdateSketch rebuild() {
    throw new SketchesReadOnlyException();
  }

  @Override
  public void reset() {
    throw new SketchesReadOnlyException();
  }

  //restricted methods

  @Override
  long[] getCache() {
    final long lgArrLongs = wseg_.get(JAVA_BYTE, LG_ARR_LONGS_BYTE) & 0XFF;
    final int preambleLongs = wseg_.get(JAVA_BYTE, PREAMBLE_LONGS_BYTE) & 0X3F;
    final long[] cacheArr = new long[1 << lgArrLongs];
    MemorySegment.copy(wseg_, JAVA_LONG_UNALIGNED, preambleLongs << 3, cacheArr, 0, 1 << lgArrLongs);
    return cacheArr;
  }

  @Override
  int getCompactPreambleLongs() {
    return computeCompactPreLongs(isEmpty(), getRetainedEntries(true), getThetaLong());
  }

  @Override
  int getCurrentPreambleLongs() {
    return PreambleUtil.extractPreLongs(wseg_);
  }

  @Override
  MemorySegment getMemorySegment() {
    return wseg_;
  }

  @Override
  short getSeedHash() {
    return (short) PreambleUtil.extractSeedHash(wseg_);
  }

  @Override
  boolean isDirty() {
    return false; //Always false for QuickSelectSketch
  }

  @Override
  boolean isOutOfSpace(final int numEntries) {
    return numEntries > hashTableThreshold_;
  }

  @Override
  int getLgArrLongs() {
    return wseg_.get(JAVA_BYTE, LG_ARR_LONGS_BYTE) & 0XFF;
  }

  int getLgRF() { //only Direct needs this
    return (wseg_.get(JAVA_BYTE, PREAMBLE_LONGS_BYTE) >>> LG_RESIZE_FACTOR_BIT) & 0X3;
  }

  @Override
  UpdateReturnState hashUpdate(final long hash) {
    throw new SketchesReadOnlyException();
  }

  /**
   * Returns the cardinality limit given the current size of the hash table array.
   *
   * @param lgNomLongs <a href="{@docRoot}/resources/dictionary.html#lgNomLongs">See lgNomLongs</a>.
   * @param lgArrLongs <a href="{@docRoot}/resources/dictionary.html#lgArrLongs">See lgArrLongs</a>.
   * @return the hash table threshold
   */
  @SuppressFBWarnings(value = "DB_DUPLICATE_BRANCHES", justification = "False Positive, see the code comments")
  protected static final int getOffHeapHashTableThreshold(final int lgNomLongs, final int lgArrLongs) {
    //SpotBugs may complain (DB_DUPLICATE_BRANCHES) if DQS_RESIZE_THRESHOLD == REBUILD_THRESHOLD,
    //but this allows us to tune these constants for different sketches.
    final double fraction = (lgArrLongs <= lgNomLongs) ? DQS_RESIZE_THRESHOLD : ThetaUtil.REBUILD_THRESHOLD;
    return (int) (fraction * (1 << lgArrLongs));
  }

}

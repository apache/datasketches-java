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

package org.apache.datasketches.theta;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_FLOAT_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static org.apache.datasketches.theta.CompactOperations.checkIllegalCurCountAndEmpty;
import static org.apache.datasketches.theta.CompactOperations.computeCompactPreLongs;
import static org.apache.datasketches.theta.CompactOperations.correctThetaOnCompact;
import static org.apache.datasketches.theta.PreambleUtil.FAMILY_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.LG_ARR_LONGS_BYTE;
//import static org.apache.datasketches.theta.PreambleUtil.LG_NOM_LONGS_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.LG_RESIZE_FACTOR_BIT;
import static org.apache.datasketches.theta.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.P_FLOAT;
import static org.apache.datasketches.theta.PreambleUtil.RETAINED_ENTRIES_INT;
import static org.apache.datasketches.theta.PreambleUtil.THETA_LONG;
import static org.apache.datasketches.theta.PreambleUtil.extractCurCount;
import static org.apache.datasketches.theta.PreambleUtil.extractLgArrLongs;
import static org.apache.datasketches.theta.PreambleUtil.extractLgNomLongs;
import static org.apache.datasketches.theta.PreambleUtil.extractThetaLong;
import static org.apache.datasketches.theta.PreambleUtil.insertThetaLong;

import java.lang.foreign.MemorySegment;
import java.util.Objects;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.MemorySegmentStatus;
import org.apache.datasketches.common.ResizeFactor;
import org.apache.datasketches.common.SketchesReadOnlyException;

/**
 * The read-only Theta Sketch.
 *
 * <p>This implementation uses data in a given MemorySegment that is owned and managed by the caller.
 * This MemorySegment can be off-heap, which if managed properly will greatly reduce the need for
 * the JVM to perform garbage collection.</p>
 *
 * @author Lee Rhodes
 * @author Kevin Lang
 */
class DirectQuickSelectSketchR extends UpdateSketch {

  /**
   * This MemorySegment reference is also used by the writable child DirectQuickSelectSketch.
   *
   * <p>When this class is constructed with the writable constructor, called by the writable child DirectQuickSelectSketch,
   * this reference can be changed, its contents can be modified.</p>
   *
   * <p>When this class is constructed with the read-only constructor, called from local factories, this MemorySegment will
   * be placed in read-only mode.</p>
   */
  MemorySegment wseg_; //

  /**
   * This writable constructor is only called by the writable child DirectQuickSelectSketch and then this class provides the
   * read-only methods for the DirectQuickSelectSketch class.
   * @param wseg the writable MemorySegment used by the writable child DirectQuickSelectSketch.
   * @param seed the seed for the update function for the writable child DirectQuickSelectSketch.
   */
  DirectQuickSelectSketchR(final MemorySegment wseg, final long seed) {
    Objects.requireNonNull(wseg, "MemorySegment wseg must not be null");
    super(seed);
    wseg_ = wseg;
  }

  /**
   * This read-only constructor is only called by local factory methods which use this class as a read-only direct sketch.
   * @param seed the seed used to validate the internal hashes of the given source MemorySegment.
   * @param srcSeg the read-only MemorySegment used by this class in read-only mode.
   */
  private DirectQuickSelectSketchR(final long seed, final MemorySegment srcSeg) {
    Objects.requireNonNull(srcSeg, "MemorySegment srcSeg must not be null");
    super(seed);
    wseg_ = srcSeg.asReadOnly();
  }

  /**
   * Wrap a sketch around the given source MemorySegment containing sketch data that originated from this sketch.
   * @param srcSeg the source MemorySegment.
   * The given MemorySegment object must be in hash table form and not read only.
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>
   * @return instance of this sketch
   */
  static DirectQuickSelectSketchR readOnlyWrap(final MemorySegment srcSeg, final long seed) {
    final int preambleLongs = Sketch.getPreambleLongs(srcSeg);                  //byte 0
    final int lgNomLongs = extractLgNomLongs(srcSeg);                   //byte 3
    final int lgArrLongs = extractLgArrLongs(srcSeg);                   //byte 4

    UpdateSketch.checkUnionQuickSelectFamily(srcSeg, preambleLongs, lgNomLongs);
    checkSegIntegrity(srcSeg, seed, preambleLongs, lgNomLongs, lgArrLongs);
    return new DirectQuickSelectSketchR(seed, srcSeg);
  }

  /**
   * Fast-wrap a sketch around the given source MemorySegment containing sketch data that originated from
   * this sketch.  This does NO validity checking of the given MemorySegment.
   * Caller must ensure segment contents are a valid sketch image.
   * @param srcSeg The given MemorySegment object must be in hash table form and not read only.
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>
   * @return instance of this sketch
   */
  static DirectQuickSelectSketchR fastReadOnlyWrap(final MemorySegment srcSeg, final long seed) {
    return new DirectQuickSelectSketchR(seed, srcSeg);
  }

  //Sketch

  @Override
  public int getCurrentBytes() {
    //not compact
    final int lgArrLongs = wseg_.get(JAVA_BYTE, LG_ARR_LONGS_BYTE) & 0XFF; //mask to byte
    final int preLongs = wseg_.get(JAVA_BYTE, PREAMBLE_LONGS_BYTE) & 0X3F; //mask to 6 bits
    return preLongs + (1 << lgArrLongs) << 3;
  }

  @Override
  public double getEstimate() {
    final int curCount = extractCurCount(wseg_);
    final long thetaLong = extractThetaLong(wseg_);
    return Sketch.estimate(thetaLong, curCount);
  }

  @Override
  public Family getFamily() {
    final int familyID = wseg_.get(JAVA_BYTE, FAMILY_BYTE) & 0XFF; //mask to byte
    return Family.idToFamily(familyID);
  }

  @Override
  public int getRetainedEntries(final boolean valid) { //always valid for theta
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
  public boolean isOffHeap() {
    return hasMemorySegment() && wseg_.isNative();
  }

  @Override
  public boolean isEmpty() {
    return PreambleUtil.isEmptyFlag(wseg_);
  }

  @Override
  public boolean isSameResource(final MemorySegment that) {
    return hasMemorySegment() && MemorySegmentStatus.isSameResource(wseg_, that); //null checks done here
  }

  @Override
  public HashIterator iterator() {
    return new MemorySegmentHashIterator(wseg_, 1 << getLgArrLongs(), getThetaLong());
  }

  @Override
  public byte[] toByteArray() { //MY_FAMILY is stored in wseg_
    final int curCount = extractCurCount(wseg_);
    checkIllegalCurCountAndEmpty(isEmpty(), curCount);
    final int lengthBytes = getCurrentBytes();
    final byte[] byteArray = new byte[lengthBytes];
    final MemorySegment seg = MemorySegment.ofArray(byteArray);
    MemorySegment.copy(wseg_, 0, seg, 0, lengthBytes);
    final long thetaLong = correctThetaOnCompact(isEmpty(), curCount, extractThetaLong(wseg_));
    insertThetaLong(seg, thetaLong);
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
    final long lgArrLongs = wseg_.get(JAVA_BYTE, LG_ARR_LONGS_BYTE) & 0XFF; //mask to byte
    final int preambleLongs = wseg_.get(JAVA_BYTE, PREAMBLE_LONGS_BYTE) & 0X3F; //mask to 6 bits
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
    return Sketch.getPreambleLongs(wseg_);
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
  boolean isOutOfSpace(final int numEntries) { //overridden by writable DirectQuickSelectSketch
    return false;
  }

  @Override
  int getLgArrLongs() {
    return wseg_.get(JAVA_BYTE, LG_ARR_LONGS_BYTE) & 0XFF; //mask to byte
  }

  int getLgRF() { //only Direct needs this
    return wseg_.get(JAVA_BYTE, PREAMBLE_LONGS_BYTE) >>> LG_RESIZE_FACTOR_BIT & 0X3; //mask to 2 bits
  }

  @Override
  UpdateReturnState hashUpdate(final long hash) {
    throw new SketchesReadOnlyException();
  }

}

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

import static org.apache.datasketches.Util.REBUILD_THRESHOLD;
import static org.apache.datasketches.theta.CompactOperations.checkIllegalCurCountAndEmpty;
import static org.apache.datasketches.theta.CompactOperations.computeCompactPreLongs;
import static org.apache.datasketches.theta.CompactOperations.correctThetaOnCompact;
import static org.apache.datasketches.theta.PreambleUtil.FAMILY_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.LG_ARR_LONGS_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.LG_NOM_LONGS_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.LG_RESIZE_FACTOR_BIT;
import static org.apache.datasketches.theta.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.P_FLOAT;
import static org.apache.datasketches.theta.PreambleUtil.RETAINED_ENTRIES_INT;
import static org.apache.datasketches.theta.PreambleUtil.THETA_LONG;
import static org.apache.datasketches.theta.PreambleUtil.extractCurCount;
import static org.apache.datasketches.theta.PreambleUtil.extractLgArrLongs;
import static org.apache.datasketches.theta.PreambleUtil.extractLgNomLongs;
import static org.apache.datasketches.theta.PreambleUtil.extractPreLongs;
import static org.apache.datasketches.theta.PreambleUtil.extractThetaLong;
import static org.apache.datasketches.theta.PreambleUtil.insertThetaLong;

import org.apache.datasketches.Family;
import org.apache.datasketches.ResizeFactor;
import org.apache.datasketches.SketchesReadOnlyException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

/**
 * The default Theta Sketch using the QuickSelect algorithm.
 * This is the read-only implementation with non-functional methods, which affect the state.
 *
 * <p>This implementation uses data in a given Memory that is owned and managed by the caller.
 * This Memory can be off-heap, which if managed properly will greatly reduce the need for
 * the JVM to perform garbage collection.</p>
 *
 * @author Lee Rhodes
 * @author Kevin Lang
 */
class DirectQuickSelectSketchR extends UpdateSketch {
  static final double DQS_RESIZE_THRESHOLD  = 15.0 / 16.0; //tuned for space
  final long seed_; //provided, kept only on heap, never serialized.
  int hashTableThreshold_; //computed, kept only on heap, never serialized.
  WritableMemory wmem_; //A WritableMemory for child class, but no write methods here

  //only called by DirectQuickSelectSketch and below
  DirectQuickSelectSketchR(final long seed, final WritableMemory wmem) {
    seed_ = seed;
    wmem_ = wmem;
  }

  /**
   * Wrap a sketch around the given source Memory containing sketch data that originated from
   * this sketch.
   * @param srcMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * The given Memory object must be in hash table form and not read only.
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>
   * @return instance of this sketch
   */
  static DirectQuickSelectSketchR readOnlyWrap(final Memory srcMem, final long seed) {
    final int preambleLongs = extractPreLongs(srcMem);                  //byte 0
    final int lgNomLongs = extractLgNomLongs(srcMem);                   //byte 3
    final int lgArrLongs = extractLgArrLongs(srcMem);                   //byte 4

    UpdateSketch.checkUnionQuickSelectFamily(srcMem, preambleLongs, lgNomLongs);
    checkMemIntegrity(srcMem, seed, preambleLongs, lgNomLongs, lgArrLongs);

    final DirectQuickSelectSketchR dqssr =
        new DirectQuickSelectSketchR(seed, (WritableMemory) srcMem);
    dqssr.hashTableThreshold_ = setHashTableThreshold(lgNomLongs, lgArrLongs);
    return dqssr;
  }


  /**
   * Fast-wrap a sketch around the given source Memory containing sketch data that originated from
   * this sketch.  This does NO validity checking of the given Memory.
   * @param srcMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * The given Memory object must be in hash table form and not read only.
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>
   * @return instance of this sketch
   */
  static DirectQuickSelectSketchR fastReadOnlyWrap(final Memory srcMem, final long seed) {
    final int lgNomLongs = srcMem.getByte(LG_NOM_LONGS_BYTE) & 0XFF;
    final int lgArrLongs = srcMem.getByte(LG_ARR_LONGS_BYTE) & 0XFF;

    final DirectQuickSelectSketchR dqss =
        new DirectQuickSelectSketchR(seed, (WritableMemory) srcMem);
    dqss.hashTableThreshold_ = setHashTableThreshold(lgNomLongs, lgArrLongs);
    return dqss;
  }

  //Sketch

  @Override
  public int getCurrentBytes() {
    //not compact
    final byte lgArrLongs = wmem_.getByte(LG_ARR_LONGS_BYTE);
    final int preLongs = wmem_.getByte(PREAMBLE_LONGS_BYTE) & 0X3F;
    final int lengthBytes = (preLongs + (1 << lgArrLongs)) << 3;
    return lengthBytes;
  }

  @Override
  public double getEstimate() {
    final int curCount = extractCurCount(wmem_);
    final long thetaLong = extractThetaLong(wmem_);
    return Sketch.estimate(thetaLong, curCount);
  }

  @Override
  public Family getFamily() {
    final int familyID = wmem_.getByte(FAMILY_BYTE) & 0XFF;
    return Family.idToFamily(familyID);
  }

  @Override
  public int getRetainedEntries(final boolean valid) { //always valid
    return wmem_.getInt(RETAINED_ENTRIES_INT);
  }

  @Override
  public long getThetaLong() {
    return wmem_.getLong(THETA_LONG);
  }

  @Override
  public boolean hasMemory() {
    return true;
  }

  @Override
  public boolean isDirect() {
    return wmem_.isDirect();
  }

  @Override
  public boolean isEmpty() {
    return PreambleUtil.isEmptyFlag(wmem_);
  }

  @Override
  public boolean isSameResource(final Memory that) {
    return wmem_.isSameResource(that);
  }

  @Override
  public HashIterator iterator() {
    return new MemoryHashIterator(wmem_, 1 << getLgArrLongs(), getThetaLong());
  }

  @Override
  public byte[] toByteArray() { //MY_FAMILY is stored in wmem_
    checkIllegalCurCountAndEmpty(isEmpty(), extractCurCount(wmem_));
    final int lengthBytes = getCurrentBytes();
    final byte[] byteArray = new byte[lengthBytes];
    final WritableMemory mem = WritableMemory.wrap(byteArray);
    wmem_.copyTo(0, mem, 0, lengthBytes);
    final long thetaLong =
        correctThetaOnCompact(isEmpty(), extractCurCount(wmem_), extractThetaLong(wmem_));
    insertThetaLong(wmem_, thetaLong);
    return byteArray;
  }

  //UpdateSketch

  @Override
  public int getLgNomLongs() {
    return PreambleUtil.extractLgNomLongs(wmem_);
  }

  @Override
  float getP() {
    return wmem_.getFloat(P_FLOAT);
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
    final long lgArrLongs = wmem_.getByte(LG_ARR_LONGS_BYTE) & 0XFF;
    final int preambleLongs = wmem_.getByte(PREAMBLE_LONGS_BYTE) & 0X3F;
    final long[] cacheArr = new long[1 << lgArrLongs];
    final WritableMemory mem = WritableMemory.wrap(cacheArr);
    wmem_.copyTo(preambleLongs << 3, mem, 0, 8 << lgArrLongs);
    return cacheArr;
  }

  @Override
  int getCompactPreambleLongs() {
    return computeCompactPreLongs(isEmpty(), getRetainedEntries(true), getThetaLong());
  }

  @Override
  int getCurrentPreambleLongs() {
    return PreambleUtil.extractPreLongs(wmem_);
  }

  @Override
  WritableMemory getMemory() {
    return wmem_;
  }



  @Override
  short getSeedHash() {
    return (short) PreambleUtil.extractSeedHash(wmem_);
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
    return wmem_.getByte(LG_ARR_LONGS_BYTE) & 0XFF;
  }

  int getLgRF() { //only Direct needs this
    return (wmem_.getByte(PREAMBLE_LONGS_BYTE) >>> LG_RESIZE_FACTOR_BIT) & 0X3;
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
  static final int setHashTableThreshold(final int lgNomLongs, final int lgArrLongs) {
    //FindBugs may complain (DB_DUPLICATE_BRANCHES) if DQS_RESIZE_THRESHOLD == REBUILD_THRESHOLD,
    //but this allows us to tune these constants for different sketches.
    final double fraction = (lgArrLongs <= lgNomLongs) ? DQS_RESIZE_THRESHOLD : REBUILD_THRESHOLD;
    return (int) Math.floor(fraction * (1 << lgArrLongs));
  }

}

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
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.datasketches.common.Util.LONG_MAX_VALUE_AS_DOUBLE;
import static org.apache.datasketches.common.Util.checkBounds;
import static org.apache.datasketches.hash.MurmurHash3.hash;
import static org.apache.datasketches.theta.CompactOperations.componentsToCompact;
import static org.apache.datasketches.theta.PreambleUtil.COMPACT_FLAG_MASK;
import static org.apache.datasketches.theta.PreambleUtil.FAMILY_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.ORDERED_FLAG_MASK;
import static org.apache.datasketches.theta.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.READ_ONLY_FLAG_MASK;
import static org.apache.datasketches.theta.PreambleUtil.SER_VER;
import static org.apache.datasketches.theta.PreambleUtil.SER_VER_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.checkSegmentSeedHash;
import static org.apache.datasketches.theta.PreambleUtil.extractFamilyID;
import static org.apache.datasketches.theta.PreambleUtil.extractFlags;
import static org.apache.datasketches.theta.PreambleUtil.extractLgResizeFactor;
import static org.apache.datasketches.theta.PreambleUtil.extractP;
import static org.apache.datasketches.theta.PreambleUtil.extractSerVer;
import static org.apache.datasketches.theta.PreambleUtil.extractThetaLong;
import static org.apache.datasketches.theta.PreambleUtil.getSegBytes;
import static org.apache.datasketches.theta.UpdateReturnState.RejectedNullOrEmpty;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.Objects;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.ResizeFactor;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.Util;
import org.apache.datasketches.thetacommon.ThetaUtil;

/**
 * The parent class for the  Update Sketch families, such as QuickSelect and Alpha.
 * The primary task of an Update Sketch is to consider datums presented via the update() methods
 * for inclusion in its internal cache. This is the sketch building process.
 *
 * @author Lee Rhodes
 */
public abstract class UpdateSketch extends Sketch {
  private final long seed_;

  UpdateSketch(final long seed) {
    seed_ = seed; //kept only on heap, never serialized. Hoisted here for performance.
  }

  /**
  * Wrap takes the writable sketch image in MemorySegment and refers to it directly. There is no data copying onto
  * the java heap. Only "Direct" Serialization Version 3 (i.e, OpenSource) sketches that have
  * been explicitly stored as writable, direct objects can be wrapped. This method assumes the
  * {@link org.apache.datasketches.common.Util#DEFAULT_UPDATE_SEED}.
  * <a href="{@docRoot}/resources/dictionary.html#defaultUpdateSeed">Default Update Seed</a>.
  * @param srcWSeg an image of a writable sketch where the image seed hash matches the default seed hash.
  * It must have a size of at least 24 bytes.
  * @return an UpdateSketch backed by the given MemorySegment
  * @throws SketchesArgumentException if the provided MemorySegment
  * is invalid, corrupted, or incompatible with this sketch type.
  * Callers must treat this as a fatal error for that segment.
  */
  public static UpdateSketch wrap(final MemorySegment srcWSeg) {
    return wrap(srcWSeg, Util.DEFAULT_UPDATE_SEED);
  }

  /**
  * Wrap takes the sketch image in MemorySegment and refers to it directly. There is no data copying onto
  * the java heap. Only "Direct" Serialization Version 3 (i.e, OpenSource) sketches that have
  * been explicitly stored as writable direct objects can be wrapped.
  * An attempt to "wrap" earlier version sketches will result in a "heapified", normal
  * Java Heap version of the sketch where all data will be copied to the heap.
  * @param srcWSeg an image of a writable sketch where the image seed hash matches the given seed hash.
  * It must have a size of at least 24 bytes.
  * @param expectedSeed the seed used to validate the given MemorySegment image.
  * <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
  * Compact sketches store a 16-bit hash of the seed, but not the seed itself.
  * @return a UpdateSketch backed by the given MemorySegment
  * @throws SketchesArgumentException if the provided MemorySegment
  * is invalid, corrupted, or incompatible with this sketch type.
  * Callers must treat this as a fatal error for that segment.
  */
  public static UpdateSketch wrap(final MemorySegment srcWSeg, final long expectedSeed) {
    Objects.requireNonNull(srcWSeg, "Source MemorySegment must not be null");
    checkBounds(0, 24, srcWSeg.byteSize()); //need min 24 bytes
    final int  preLongs = srcWSeg.get(JAVA_BYTE, PREAMBLE_LONGS_BYTE) & 0X3F; //mask to 6 bits
    final int serVer = srcWSeg.get(JAVA_BYTE, SER_VER_BYTE) & 0XFF; //mask to byte
    final int familyID = srcWSeg.get(JAVA_BYTE, FAMILY_BYTE) & 0XFF; //mask to byte
    final Family family = Family.idToFamily(familyID);
    if (family != Family.QUICKSELECT) {
      throw new SketchesArgumentException(
        "A " + family + " sketch cannot be wrapped as an UpdateSketch.");
    }
    if (serVer == 3 && preLongs == 3) {
      return DirectQuickSelectSketch.writableWrap(srcWSeg, expectedSeed);
    } else {
      throw new SketchesArgumentException(
        "Corrupted: An UpdateSketch image must have SerVer = 3 and preLongs = 3");
    }
  }

  /**
   * Instantiates an on-heap UpdateSketch from a MemorySegment. This method assumes the
   * {@link org.apache.datasketches.common.Util#DEFAULT_UPDATE_SEED}.
   * @param srcSeg the given MemorySegment with a sketch image.
   * It must have a size of at least 24 bytes.
   * @return an UpdateSketch
   * @throws SketchesArgumentException if the provided MemorySegment
   * is invalid, corrupted, or incompatible with this sketch type.
   * Callers must treat this as a fatal error for that segment.
   */
  public static UpdateSketch heapify(final MemorySegment srcSeg) {
    return heapify(srcSeg, Util.DEFAULT_UPDATE_SEED);
  }

  /**
   * Instantiates an on-heap UpdateSketch from a MemorySegment.
   * @param srcSeg the given MemorySegment.
   * It must have a size of at least 24 bytes.
   * @param expectedSeed the seed used to validate the given MemorySegment image.
   * <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
   * @return an UpdateSketch
   * @throws SketchesArgumentException if the provided MemorySegment
   * is invalid, corrupted, or incompatible with this sketch type.
   * Callers must treat this as a fatal error for that segment.
   */
  public static UpdateSketch heapify(final MemorySegment srcSeg, final long expectedSeed) {
    Objects.requireNonNull(srcSeg, "Source MemorySegment must not be null");
    checkBounds(0, 24, srcSeg.byteSize()); //need min 24 bytes
    final Family family = Family.idToFamily(srcSeg.get(JAVA_BYTE, FAMILY_BYTE));
    if (family.equals(Family.ALPHA)) {
      return HeapAlphaSketch.heapifyInstance(srcSeg, expectedSeed);
    }
    return HeapQuickSelectSketch.heapifyInstance(srcSeg, expectedSeed);
  }

  //Sketch interface

  @Override
  public CompactSketch compact(final boolean dstOrdered, final MemorySegment dstWSeg) {
    return componentsToCompact(getThetaLong(), getRetainedEntries(true), getSeedHash(), isEmpty(),
        false, false, dstOrdered, dstWSeg, getCache());
  }

  @Override
  public int getCompactBytes() {
    final int preLongs = getCompactPreambleLongs();
    final int dataLongs = getRetainedEntries(true);
    return preLongs + dataLongs << 3;
  }

  @Override
  int getCurrentDataLongs() {
    return 1 << getLgArrLongs();
  }

  @Override
  public boolean hasMemorySegment() {
    return this instanceof final DirectQuickSelectSketchR dqssr && dqssr.hasMemorySegment();
  }

  @Override
  public boolean isCompact() {
    return false;
  }

  @Override
  public boolean isOffHeap() {
    return this instanceof final DirectQuickSelectSketchR dqssr && dqssr.isOffHeap();
  }

  @Override
  public boolean isOrdered() {
    return false;
  }

  @Override
  public boolean isSameResource(final MemorySegment that) {
    return this instanceof final DirectQuickSelectSketchR dqssr && dqssr.isSameResource(that);
  }

  //UpdateSketch interface

  /**
   * Returns a new builder
   * @return a new builder
   */
  public static final UpdateSketchBuilder builder() {
    return new UpdateSketchBuilder();
  }

  /**
   * Returns the configured ResizeFactor
   * @return the configured ResizeFactor
   */
  public abstract ResizeFactor getResizeFactor();

  /**
   * Gets the configured sampling probability, <i>p</i>.
   * <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability, <i>p</i></a>
   * @return the sampling probability, <i>p</i>
   */
  abstract float getP();

  /**
   * Gets the configured seed
   * @return the configured seed
   */
  public long getSeed() { return seed_; }

  /**
   * Resets this sketch back to a virgin empty state.
   */
  public abstract void reset();

  /**
   * Rebuilds the hash table to remove dirty values or to reduce the size
   * to nominal entries.
   * @return this sketch
   */
  public abstract UpdateSketch rebuild();

  /**
   * Present this sketch with a long.
   *
   * @param datum The given long datum.
   * @return
   * <a href="{@docRoot}/resources/dictionary.html#updateReturnState">See Update Return State</a>
   */
  public UpdateReturnState update(final long datum) {
    return hashUpdate(hash(datum, seed_)[0] >>> 1);
  }

  /**
   * Present this sketch with the given double (or float) datum.
   * The double will be converted to a long using Double.doubleToLongBits(datum),
   * which normalizes all NaN values to a single NaN representation.
   * Plus and minus zero will be normalized to plus zero.
   * The special floating-point values NaN and +/- Infinity are treated as distinct.
   *
   * @param datum The given double datum.
   * @return
   * <a href="{@docRoot}/resources/dictionary.html#updateReturnState">See Update Return State</a>
   */
  public UpdateReturnState update(final double datum) {
    final double d = datum == 0.0 ? 0.0 : datum; // canonicalize -0.0, 0.0
    final long data = Double.doubleToLongBits(d);// canonicalize all NaN & +/- infinity forms
    return hashUpdate(hash(data, seed_)[0] >>> 1);
  }

  /**
   * Present this sketch with the given String.
   * The string is converted to a byte array using UTF8 encoding.
   * If the string is null or empty no update attempt is made and the method returns.
   *
   * <p>Note: this will not produce the same output hash values as the {@link #update(char[])}
   * method and will generally be a little slower depending on the complexity of the UTF8 encoding.
   * </p>
   *
   * @param datum The given String.
   * @return
   * <a href="{@docRoot}/resources/dictionary.html#updateReturnState">See Update Return State</a>
   */
  public UpdateReturnState update(final String datum) {
    if (datum == null || datum.isEmpty()) {
      return RejectedNullOrEmpty;
    }
    final byte[] data = datum.getBytes(UTF_8);
    return hashUpdate(hash(data, seed_)[0] >>> 1);
  }

  /**
   * Present this sketch with the given byte array.
   * If the byte array is null or empty no update attempt is made and the method returns.
   *
   * @param data The given byte array.
   * @return
   * <a href="{@docRoot}/resources/dictionary.html#updateReturnState">See Update Return State</a>
   */
  public UpdateReturnState update(final byte[] data) {
    if (data == null || data.length == 0) {
      return RejectedNullOrEmpty;
    }
    return hashUpdate(hash(data, seed_)[0] >>> 1);
  }

  /**
   * Present this sketch with the given ByteBuffer
   * If the ByteBuffer is null or empty, no update attempt is made and the method returns.
   *
   * @param buffer the input ByteBuffer
   * @return
   * <a href="{@docRoot}/resources/dictionary.html#updateReturnState">See Update Return State</a>
   */
  public UpdateReturnState update(final ByteBuffer buffer) {
    if (buffer == null || !buffer.hasRemaining()) {
      return RejectedNullOrEmpty;
    }
    return hashUpdate(hash(buffer, seed_)[0] >>> 1);
  }

  /**
   * Present this sketch with the given char array.
   * If the char array is null or empty no update attempt is made and the method returns.
   *
   * <p>Note: this will not produce the same output hash values as the {@link #update(String)}
   * method but will be a little faster as it avoids the complexity of the UTF8 encoding.</p>
   *
   * @param data The given char array.
   * @return
   * <a href="{@docRoot}/resources/dictionary.html#updateReturnState">See Update Return State</a>
   */
  public UpdateReturnState update(final char[] data) {
    if (data == null || data.length == 0) {
      return RejectedNullOrEmpty;
    }
    return hashUpdate(hash(data, seed_)[0] >>> 1);
  }

  /**
   * Present this sketch with the given integer array.
   * If the integer array is null or empty no update attempt is made and the method returns.
   *
   * @param data The given int array.
   * @return
   * <a href="{@docRoot}/resources/dictionary.html#updateReturnState">See Update Return State</a>
   */
  public UpdateReturnState update(final int[] data) {
    if (data == null || data.length == 0) {
      return RejectedNullOrEmpty;
    }
    return hashUpdate(hash(data, seed_)[0] >>> 1);
  }

  /**
   * Present this sketch with the given long array.
   * If the long array is null or empty no update attempt is made and the method returns.
   *
   * @param data The given long array.
   * @return
   * <a href="{@docRoot}/resources/dictionary.html#updateReturnState">See Update Return State</a>
   */
  public UpdateReturnState update(final long[] data) {
    if (data == null || data.length == 0) {
      return RejectedNullOrEmpty;
    }
    return hashUpdate(hash(data, seed_)[0] >>> 1);
  }

  //restricted methods

  /**
   * All potential updates converge here.
   *
   * <p>Don't ever call this unless you really know what you are doing!</p>
   *
   * @param hash the given input hash value.  A hash of zero or Long.MAX_VALUE is ignored.
   * A negative hash value will throw an exception.
   * @return <a href="{@docRoot}/resources/dictionary.html#updateReturnState">See Update Return State</a>
   */
  abstract UpdateReturnState hashUpdate(long hash);

  /**
   * Gets the Log base 2 of the current size of the internal cache
   * @return the Log base 2 of the current size of the internal cache
   */
  abstract int getLgArrLongs();

  /**
   * Gets the Log base 2 of the configured nominal entries
   * @return the Log base 2 of the configured nominal entries
   */
  public abstract int getLgNomLongs();

  /**
   * Returns true if the internal cache contains "dirty" values that are greater than or equal
   * to thetaLong.
   * @return true if the internal cache is dirty.
   */
  abstract boolean isDirty();

  /**
   * Returns true if numEntries (curCount) is greater than the hashTableThreshold.
   * @param numEntries the given number of entries (or current count).
   * @return true if numEntries (curCount) is greater than the hashTableThreshold.
   */
  abstract boolean isOutOfSpace(int numEntries);

  static void checkUnionQuickSelectFamily(final MemorySegment seg, final int preambleLongs,
      final int lgNomLongs) {
    //Check Family
    final int familyID = extractFamilyID(seg);                       //byte 2
    final Family family = Family.idToFamily(familyID);
    if (family.equals(Family.UNION)) {
      if (preambleLongs != Family.UNION.getMinPreLongs()) {
        throw new SketchesArgumentException(
            "Possible corruption: Invalid PreambleLongs value for UNION: " + preambleLongs);
      }
    }
    else if (family.equals(Family.QUICKSELECT)) {
      if (preambleLongs != Family.QUICKSELECT.getMinPreLongs()) {
        throw new SketchesArgumentException(
            "Possible corruption: Invalid PreambleLongs value for QUICKSELECT: " + preambleLongs);
      }
    } else {
      throw new SketchesArgumentException(
          "Possible corruption: Invalid Family: " + family.toString());
    }

    //Check lgNomLongs
    if (lgNomLongs < ThetaUtil.MIN_LG_NOM_LONGS) {
      throw new SketchesArgumentException(
          "Possible corruption: Current MemorySegment lgNomLongs < min required size: "
              + lgNomLongs + " < " + ThetaUtil.MIN_LG_NOM_LONGS);
    }
  }

  static void checkSegIntegrity(final MemorySegment srcSeg, final long expectedSeed, final int preambleLongs,
      final int lgNomLongs, final int lgArrLongs) {

    //Check SerVer
    final int serVer = extractSerVer(srcSeg);                           //byte 1
    if (serVer != SER_VER) {
      throw new SketchesArgumentException(
          "Possible corruption: Invalid Serialization Version: " + serVer);
    }

    //Check flags
    final int flags = extractFlags(srcSeg);                             //byte 5
    final int badFlagsMask = ORDERED_FLAG_MASK | COMPACT_FLAG_MASK | READ_ONLY_FLAG_MASK;
    if ((flags & badFlagsMask) > 0) {
      throw new SketchesArgumentException(
        "Possible corruption: Input srcSeg cannot be: compact, ordered, nor read-only");
    }

    //Check seed hashes
    final short seedHash = checkSegmentSeedHash(srcSeg, expectedSeed);              //byte 6,7
    Util.checkSeedHashes(seedHash, Util.computeSeedHash(expectedSeed));

    //Check seg capacity, lgArrLongs
    final long curCapBytes = srcSeg.byteSize();
    final int minReqBytes = getSegBytes(lgArrLongs, preambleLongs);
    if (curCapBytes < minReqBytes) {
      throw new SketchesArgumentException(
          "Possible corruption: Current MemorySegment size < min required size: "
              + curCapBytes + " < " + minReqBytes);
    }
    //check Theta, p
    final float p = extractP(srcSeg);                                   //bytes 12-15
    final long thetaLong = extractThetaLong(srcSeg);                    //bytes 16-23
    final double theta = thetaLong / LONG_MAX_VALUE_AS_DOUBLE;
    //if (lgArrLongs <= lgNomLongs) the sketch is still resizing, thus theta cannot be < p.
    if (lgArrLongs <= lgNomLongs && theta < p ) {
      throw new SketchesArgumentException(
        "Possible corruption: Theta cannot be < p and lgArrLongs <= lgNomLongs. "
            + lgArrLongs + " <= " + lgNomLongs + ", Theta: " + theta + ", p: " + p);
    }
  }

  /**
   * This checks to see if the MemorySegment RF factor was set correctly as early versions may not
   * have set it.
   * @param srcSeg the source MemorySegment
   * @param lgNomLongs the current lgNomLongs
   * @param lgArrLongs the current lgArrLongs
   * @return true if the the MemorySegment RF factor is incorrect and the caller can either
   * correct it or throw an error.
   */
  static boolean isResizeFactorIncorrect(final MemorySegment srcSeg, final int lgNomLongs,
      final int lgArrLongs) {
    final int lgT = lgNomLongs + 1;
    final int lgA = lgArrLongs;
    final int lgR = extractLgResizeFactor(srcSeg);
    if (lgR == 0) { return lgA != lgT; }
    return (lgT - lgA) % lgR != 0;
  }

}

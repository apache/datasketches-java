/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;
import static com.yahoo.sketches.Util.MIN_LG_NOM_LONGS;
import static com.yahoo.sketches.hash.MurmurHash3.hash;
import static com.yahoo.sketches.theta.PreambleUtil.BIG_ENDIAN_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.COMPACT_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.FAMILY_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.MAX_THETA_LONG_AS_DOUBLE;
import static com.yahoo.sketches.theta.PreambleUtil.ORDERED_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.READ_ONLY_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.SER_VER;
import static com.yahoo.sketches.theta.PreambleUtil.SER_VER_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.extractFamilyID;
import static com.yahoo.sketches.theta.PreambleUtil.extractFlags;
import static com.yahoo.sketches.theta.PreambleUtil.extractP;
import static com.yahoo.sketches.theta.PreambleUtil.extractSeedHash;
import static com.yahoo.sketches.theta.PreambleUtil.extractSerVer;
import static com.yahoo.sketches.theta.PreambleUtil.extractThetaLong;
import static com.yahoo.sketches.theta.PreambleUtil.getMemBytes;
import static com.yahoo.sketches.theta.UpdateReturnState.RejectedNullOrEmpty;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.Util;

/**
 * The parent class for the  Update Sketch families, such as QuickSelect and Alpha.
 * The primary task of an Update Sketch is to consider datums presented via the update() methods
 * for inclusion in its internal cache. This is the sketch building process.
 *
 * @author Lee Rhodes
 */
public abstract class UpdateSketch extends Sketch {

  UpdateSketch() {}

  /**
  * Wrap takes the sketch image in Memory and refers to it directly. There is no data copying onto
  * the java heap. Only "Direct" Serialization Version 3 (i.e, OpenSource) sketches that have
  * been explicitly stored as direct objects can be wrapped. This method assumes the
  * {@link Util#DEFAULT_UPDATE_SEED}.
  * <a href="{@docRoot}/resources/dictionary.html#defaultUpdateSeed">Default Update Seed</a>.
  * @param srcMem an image of a Sketch where the image seed hash matches the default seed hash.
  * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
  * @return a Sketch backed by the given Memory
  */
  public static UpdateSketch wrap(final WritableMemory srcMem) {
    return wrap(srcMem, DEFAULT_UPDATE_SEED);
  }

  /**
  * Wrap takes the sketch image in Memory and refers to it directly. There is no data copying onto
  * the java heap. Only "Direct" Serialization Version 3 (i.e, OpenSource) sketches that have
  * been explicitly stored as direct objects can be wrapped.
  * An attempt to "wrap" earlier version sketches will result in a "heapified", normal
  * Java Heap version of the sketch where all data will be copied to the heap.
  * @param srcMem an image of a Sketch where the image seed hash matches the given seed hash.
  * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
  * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
  * Compact sketches store a 16-bit hash of the seed, but not the seed itself.
  * @return a UpdateSketch backed by the given Memory
  */
  public static UpdateSketch wrap(final WritableMemory srcMem, final long seed) {
    final int  preLongs = srcMem.getByte(PREAMBLE_LONGS_BYTE) & 0X3F;
    final int serVer = srcMem.getByte(SER_VER_BYTE) & 0XFF;
    final int familyID = srcMem.getByte(FAMILY_BYTE) & 0XFF;
    final Family family = Family.idToFamily(familyID);
    if (family != Family.QUICKSELECT) {
      throw new SketchesArgumentException(
        "A " + family + " sketch cannot be wrapped as an UpdateSketch.");
    }
    if ((serVer == 3) && (preLongs == 3)) {
      return DirectQuickSelectSketch.writableWrap(srcMem, seed);
    } else {
      throw new SketchesArgumentException(
        "Corrupted: An UpdateSketch image: must have SerVer = 3 and preLongs = 3");
    }
  }

  /**
   * Instantiates an on-heap UpdateSketch from Memory. This method assumes the
   * {@link Util#DEFAULT_UPDATE_SEED}.
   * @param srcMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @return an UpdateSketch
   */
  public static UpdateSketch heapify(final Memory srcMem) {
    return heapify(srcMem, DEFAULT_UPDATE_SEED);
  }

  /**
   * Instantiates an on-heap UpdateSketch from Memory.
   * @param srcMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
   * @return an UpdateSketch
   */
  public static UpdateSketch heapify(final Memory srcMem, final long seed) {
    final Family family = Family.idToFamily(srcMem.getByte(FAMILY_BYTE));
    if (family.equals(Family.ALPHA)) {
      return HeapAlphaSketch.heapifyInstance(srcMem, seed);
    }
    return HeapQuickSelectSketch.heapifyInstance(srcMem, seed);
  }

  //Sketch interface

  @Override
  public CompactSketch compact() {
    return compact(true, null);
  }

  @Override
  public CompactSketch compact(final boolean dstOrdered, final WritableMemory dstMem) {
    CompactSketch sketchOut = null;
    final int sw = (dstOrdered ? 2 : 0) | ((dstMem != null) ? 1 : 0);
    switch (sw) {
      case 0: { //dst not ordered, dstMem == null
        sketchOut = HeapCompactUnorderedSketch.compact(this);
        break;
      }
      case 1: { //dst not ordered, dstMem == valid
        sketchOut = DirectCompactUnorderedSketch.compact(this, dstMem);
        break;
      }
      case 2: { //dst ordered, dstMem == null
        sketchOut = HeapCompactOrderedSketch.compact(this);
        break;
      }
      case 3: { //dst ordered, dstMem == valid
        sketchOut = DirectCompactOrderedSketch.compact(this, dstMem);
        break;
      }
      //default: //This cannot happen and cannot be tested
    }
    return sketchOut;
  }

  @Override
  public boolean isCompact() {
    return false;
  }

  @Override
  public boolean isOrdered() {
    return false;
  }

  //UpdateSketch interface

  /**
   * Returns a new builder
   *
   * @return a new builder
   */
  public static final UpdateSketchBuilder builder() {
    return new UpdateSketchBuilder();
  }

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
   * Returns the configured ResizeFactor
   * @return the configured ResizeFactor
   */
  public abstract ResizeFactor getResizeFactor();

  /**
   * Present this sketch with a long.
   *
   * @param datum The given long datum.
   * @return
   * <a href="{@docRoot}/resources/dictionary.html#updateReturnState">See Update Return State</a>
   */
  public UpdateReturnState update(final long datum) {
    final long[] data = { datum };
    return hashUpdate(hash(data, getSeed())[0] >>> 1);
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
    final double d = (datum == 0.0) ? 0.0 : datum; // canonicalize -0.0, 0.0
    final long[] data = { Double.doubleToLongBits(d) };// canonicalize all NaN forms
    return hashUpdate(hash(data, getSeed())[0] >>> 1);
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
    if ((datum == null) || datum.isEmpty()) {
      return RejectedNullOrEmpty;
    }
    final byte[] data = datum.getBytes(UTF_8);
    return hashUpdate(hash(data, getSeed())[0] >>> 1);
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
    if ((data == null) || (data.length == 0)) {
      return RejectedNullOrEmpty;
    }
    return hashUpdate(hash(data, getSeed())[0] >>> 1);
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
    if ((data == null) || (data.length == 0)) {
      return RejectedNullOrEmpty;
    }
    return hashUpdate(hash(data, getSeed())[0] >>> 1);
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
    if ((data == null) || (data.length == 0)) {
      return RejectedNullOrEmpty;
    }
    return hashUpdate(hash(data, getSeed())[0] >>> 1);
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
    if ((data == null) || (data.length == 0)) {
      return RejectedNullOrEmpty;
    }
    return hashUpdate(hash(data, getSeed())[0] >>> 1);
  }

  //restricted methods

  /**
   * All potential updates converge here.
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
   * Gets the configured sampling probability, <i>p</i>.
   * <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability, <i>p</i></a>
   * @return the sampling probability, <i>p</i>
   */
  abstract float getP();

  /**
   * Gets the configured seed
   * @return the configured seed
   */
  abstract long getSeed();

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

  static void checkUnionQuickSelectFamily(final Memory mem, final int preambleLongs,
      final int lgNomLongs) {
    //Check Family
    final int familyID = extractFamilyID(mem);                       //byte 2
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
    if (lgNomLongs < MIN_LG_NOM_LONGS) {
      throw new SketchesArgumentException(
          "Possible corruption: Current Memory lgNomLongs < min required size: "
              + lgNomLongs + " < " + MIN_LG_NOM_LONGS);
    }
  }

  static void checkMemIntegrity(final Memory srcMem, final long seed, final int preambleLongs,
      final int lgNomLongs, final int lgArrLongs) {

    //Check SerVer
    final int serVer = extractSerVer(srcMem);                           //byte 1
    if (serVer != SER_VER) {
      throw new SketchesArgumentException(
          "Possible corruption: Invalid Serialization Version: " + serVer);
    }

    //Check flags
    final int flags = extractFlags(srcMem);                             //byte 5
    final int flagsMask =
        ORDERED_FLAG_MASK | COMPACT_FLAG_MASK | READ_ONLY_FLAG_MASK | BIG_ENDIAN_FLAG_MASK;
    if ((flags & flagsMask) > 0) {
      throw new SketchesArgumentException(
        "Possible corruption: Input srcMem cannot be: big-endian, compact, ordered, or read-only");
    }

    //Check seed hashes
    final short seedHash = (short)extractSeedHash(srcMem);              //byte 6,7
    Util.checkSeedHashes(seedHash, Util.computeSeedHash(seed));

    //Check mem capacity, lgArrLongs
    final long curCapBytes = srcMem.getCapacity();
    final int minReqBytes = getMemBytes(lgArrLongs, preambleLongs);
    if (curCapBytes < minReqBytes) {
      throw new SketchesArgumentException(
          "Possible corruption: Current Memory size < min required size: "
              + curCapBytes + " < " + minReqBytes);
    }
    //check Theta, p
    final float p = extractP(srcMem);                                   //bytes 12-15
    final long thetaLong = extractThetaLong(srcMem);                    //bytes 16-23
    final double theta = thetaLong / MAX_THETA_LONG_AS_DOUBLE;
    if ((lgArrLongs <= lgNomLongs) && (theta < p) ) {
      throw new SketchesArgumentException(
        "Possible corruption: Theta cannot be < p and lgArrLongs <= lgNomLongs. "
            + lgArrLongs + " <= " + lgNomLongs + ", Theta: " + theta + ", p: " + p);
    }
  }

}

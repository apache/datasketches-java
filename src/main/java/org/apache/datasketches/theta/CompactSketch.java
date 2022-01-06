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

import static org.apache.datasketches.Family.idToFamily;
import static org.apache.datasketches.Util.DEFAULT_UPDATE_SEED;
import static org.apache.datasketches.theta.PreambleUtil.COMPACT_FLAG_MASK;
import static org.apache.datasketches.theta.PreambleUtil.EMPTY_FLAG_MASK;
import static org.apache.datasketches.theta.PreambleUtil.FAMILY_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.FLAGS_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.ORDERED_FLAG_MASK;
import static org.apache.datasketches.theta.PreambleUtil.READ_ONLY_FLAG_MASK;
import static org.apache.datasketches.theta.PreambleUtil.SER_VER_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.extractSeedHash;
import static org.apache.datasketches.theta.SingleItemSketch.otherCheckForSingleItem;

import org.apache.datasketches.Family;
import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.Util;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

/**
 * The parent class of all the CompactSketches. CompactSketches are never created directly.
 * They are created as a result of the compact() method of an UpdateSketch, a result of a
 * getResult() of a SetOperation, or from a heapify method.
 *
 * <p>A CompactSketch is the simplest form of a Theta Sketch. It consists of a compact list
 * (i.e., no intervening spaces) of hash values, which may be ordered or not, a value for theta
 * and a seed hash.  A CompactSketch is immutable (read-only),
 * and the space required when stored is only the space required for the hash values and 8 to 24
 * bytes of preamble. An empty CompactSketch consumes only 8 bytes.</p>
 *
 * @author Lee Rhodes
 */
public abstract class CompactSketch extends Sketch {
  private static final short defaultSeedHash = Util.computeSeedHash(DEFAULT_UPDATE_SEED);

  /**
   * Heapify takes a CompactSketch image in Memory and instantiates an on-heap CompactSketch.
   *
   * <p>The resulting sketch will not retain any link to the source Memory and all of its data will be
   * copied to the heap CompactSketch.</p>
   *
   * <p>This method assumes that the sketch image was created with the correct hash seed, so it is not checked.
   * The resulting on-heap CompactSketch will be given the seedHash derived from the given sketch image.
   * However, Serial Version 1 sketch images do not have a seedHash field,
   * so the resulting heapified CompactSketch will be given the hash of the DEFAULT_UPDATE_SEED.</p>
   *
   * @param srcMem an image of a CompactSketch.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>.
   * @return a CompactSketch on the heap.
   */
  public static CompactSketch heapify(final Memory srcMem) {
    final int serVer = srcMem.getByte(SER_VER_BYTE) & 0XFF;
    final int familyID = srcMem.getByte(FAMILY_BYTE) & 0XFF;
    final Family family = Family.idToFamily(familyID);
    if (family != Family.COMPACT) {
      throw new IllegalArgumentException("Corrupted: " + family + " is not Compact!");
    }
    if (serVer == 3) { //no seed check
      final int flags = PreambleUtil.extractFlags(srcMem);
      final boolean srcOrdered = (flags & ORDERED_FLAG_MASK) != 0;
      return CompactOperations.memoryToCompact(srcMem, srcOrdered, null);
    }
    //not SerVer 3, assume compact stored form
    if (serVer == 1) {
      return ForwardCompatibility.heapify1to3(srcMem, defaultSeedHash);
    }
    if (serVer == 2) {
      final short srcSeedHash = (short) extractSeedHash(srcMem);
      return ForwardCompatibility.heapify2to3(srcMem, srcSeedHash);
    }
    throw new SketchesArgumentException("Unknown Serialization Version: " + serVer);
  }

  /**
   * Heapify takes a CompactSketch image in Memory and instantiates an on-heap CompactSketch.
   *
   * <p>The resulting sketch will not retain any link to the source Memory and all of its data will be
   * copied to the heap CompactSketch.</p>
   *
   * <p>This method checks if the given expectedSeed was used to create the source Memory image.
   * However, SerialVersion 1 sketch images cannot be checked as they don't have a seedHash field,
   * so the resulting heapified CompactSketch will be given the hash of the expectedSeed.</p>
   *
   * @param srcMem an image of a CompactSketch that was created using the given expectedSeed.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>.
   * @param expectedSeed the seed used to validate the given Memory image.
   * <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
   * @return a CompactSketch on the heap.
   */
  public static CompactSketch heapify(final Memory srcMem, final long expectedSeed) {
    final int serVer = srcMem.getByte(SER_VER_BYTE);
    final byte familyID = srcMem.getByte(FAMILY_BYTE);

    final Family family = idToFamily(familyID);
    if (family != Family.COMPACT) {
      throw new IllegalArgumentException("Corrupted: " + family + " is not Compact!");
    }
    if (serVer == 3) {
      final int flags = PreambleUtil.extractFlags(srcMem);
      final boolean srcOrdered = (flags & ORDERED_FLAG_MASK) != 0;
      final boolean empty = (flags & EMPTY_FLAG_MASK) != 0;
      if (!empty) { PreambleUtil.checkMemorySeedHash(srcMem, expectedSeed); }
      return CompactOperations.memoryToCompact(srcMem, srcOrdered, null);
    }
    //not SerVer 3, assume compact stored form
    final short seedHash = Util.computeSeedHash(expectedSeed);
    if (serVer == 1) {
      return ForwardCompatibility.heapify1to3(srcMem, seedHash);
    }
    if (serVer == 2) {
      return ForwardCompatibility.heapify2to3(srcMem, seedHash);
    }
    throw new SketchesArgumentException("Unknown Serialization Version: " + serVer);
  }

  /**
   * Wrap takes the CompactSketch image in given Memory and refers to it directly.
   * There is no data copying onto the java heap.
   * The wrap operation enables fast read-only merging and access to all the public read-only API.
   *
   * <p>Only "Direct" Serialization Version 3 (i.e, OpenSource) sketches that have
   * been explicitly stored as direct sketches can be wrapped.
   * Wrapping earlier serial version sketches will result in a heapify operation.
   * These early versions were never designed to "wrap".</p>
   *
   * <p>Wrapping any subclass of this class that is empty or contains only a single item will
   * result in heapified forms of empty and single item sketch respectively.
   * This is actually faster and consumes less overall memory.</p>
   *
   * <p>This method assumes that the sketch image was created with the correct hash seed, so it is not checked.
   * However, Serial Version 1 sketch images do not have a seedHash field,
   * so the resulting on-heap CompactSketch will be given the hash of the DEFAULT_UPDATE_SEED.</p>
   *
   * @param srcMem an image of a Sketch.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>.
   * @return a CompactSketch backed by the given Memory except as above.
   */
  public static CompactSketch wrap(final Memory srcMem) {
    final int serVer = srcMem.getByte(SER_VER_BYTE) & 0XFF;
    final int familyID = srcMem.getByte(FAMILY_BYTE) & 0XFF;
    final Family family = Family.idToFamily(familyID);
    if (family != Family.COMPACT) {
      throw new IllegalArgumentException("Corrupted: " + family + " is not Compact!");
    }
    if (serVer == 3) {
      if (PreambleUtil.isEmptyFlag(srcMem)) {
        return EmptyCompactSketch.getHeapInstance(srcMem);
      }
      final short memSeedHash = (short) extractSeedHash(srcMem);
      if (otherCheckForSingleItem(srcMem)) { //SINGLEITEM?
        return SingleItemSketch.heapify(srcMem, memSeedHash);
      }
      //not empty & not singleItem
      final int flags = srcMem.getByte(FLAGS_BYTE);
      final boolean compactFlag = (flags & COMPACT_FLAG_MASK) > 0;
      if (!compactFlag) {
        throw new SketchesArgumentException(
            "Corrupted: COMPACT family sketch image must have compact flag set");
      }
      final boolean readOnly = (flags & READ_ONLY_FLAG_MASK) > 0;
      if (!readOnly) {
        throw new SketchesArgumentException(
            "Corrupted: COMPACT family sketch image must have Read-Only flag set");
      }
      return DirectCompactSketch.wrapInstance(srcMem, memSeedHash);
    } //end of serVer 3
    else if (serVer == 1) {
      return ForwardCompatibility.heapify1to3(srcMem, defaultSeedHash);
    }
    else if (serVer == 2) {
      final short memSeedHash = (short) extractSeedHash(srcMem);
      return ForwardCompatibility.heapify2to3(srcMem, memSeedHash);
    }
    throw new SketchesArgumentException(
        "Corrupted: Serialization Version " + serVer + " not recognized.");
  }

  /**
   * Wrap takes the sketch image in the given Memory and refers to it directly.
   * There is no data copying onto the java heap.
   * The wrap operation enables fast read-only merging and access to all the public read-only API.
   *
   * <p>Only "Direct" Serialization Version 3 (i.e, OpenSource) sketches that have
   * been explicitly stored as direct sketches can be wrapped.
   * Wrapping earlier serial version sketches will result in a heapify operation.
   * These early versions were never designed to "wrap".</p>
   *
   * <p>Wrapping any subclass of this class that is empty or contains only a single item will
   * result in heapified forms of empty and single item sketch respectively.
   * This is actually faster and consumes less overall memory.</p>
   *
   * <p>This method checks if the given expectedSeed was used to create the source Memory image.
   * However, SerialVersion 1 sketches cannot be checked as they don't have a seedHash field,
   * so the resulting heapified CompactSketch will be given the hash of the expectedSeed.</p>
   *
   * @param srcMem an image of a Sketch that was created using the given expectedSeed.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param expectedSeed the seed used to validate the given Memory image.
   * <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
   * @return a CompactSketch backed by the given Memory except as above.
   */
  public static CompactSketch wrap(final Memory srcMem, final long expectedSeed) {
    final int serVer = srcMem.getByte(SER_VER_BYTE) & 0XFF;
    final int familyID = srcMem.getByte(FAMILY_BYTE) & 0XFF;
    final Family family = Family.idToFamily(familyID);
    if (family != Family.COMPACT) {
      throw new IllegalArgumentException("Corrupted: " + family + " is not Compact!");
    }
    final short seedHash = Util.computeSeedHash(expectedSeed);

    if (serVer == 3) {
      if (PreambleUtil.isEmptyFlag(srcMem)) {
        return EmptyCompactSketch.getHeapInstance(srcMem);
      }
      if (otherCheckForSingleItem(srcMem)) { //SINGLEITEM?
        return SingleItemSketch.heapify(srcMem, seedHash);
      }
      //not empty & not singleItem
      final int flags = srcMem.getByte(FLAGS_BYTE);
      final boolean compactFlag = (flags & COMPACT_FLAG_MASK) > 0;
      if (!compactFlag) {
        throw new SketchesArgumentException(
            "Corrupted: COMPACT family sketch image must have compact flag set");
      }
      final boolean readOnly = (flags & READ_ONLY_FLAG_MASK) > 0;
      if (!readOnly) {
        throw new SketchesArgumentException(
            "Corrupted: COMPACT family sketch image must have Read-Only flag set");
      }
      return DirectCompactSketch.wrapInstance(srcMem, seedHash);
    } //end of serVer 3
    else if (serVer == 1) {
      return ForwardCompatibility.heapify1to3(srcMem, seedHash);
    }
    else if (serVer == 2) {
      return ForwardCompatibility.heapify2to3(srcMem, seedHash);
    }
    throw new SketchesArgumentException(
        "Corrupted: Serialization Version " + serVer + " not recognized.");

  }


  //Sketch Overrides

  @Override
  public abstract CompactSketch compact(final boolean dstOrdered, final WritableMemory dstMem);

  @Override
  public int getCompactBytes() {
    return getCurrentBytes();
  }

  @Override
  int getCurrentDataLongs() {
    return getRetainedEntries(true);
  }

  @Override
  public Family getFamily() {
    return Family.COMPACT;
  }

  @Override
  public boolean isCompact() {
    return true;
  }

}

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
import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_SHORT_UNALIGNED;
import static org.apache.datasketches.common.ByteArrayUtil.getShortLE;
import static org.apache.datasketches.common.Family.idToFamily;
import static org.apache.datasketches.theta.PreambleUtil.COMPACT_FLAG_MASK;
import static org.apache.datasketches.theta.PreambleUtil.EMPTY_FLAG_MASK;
import static org.apache.datasketches.theta.PreambleUtil.FLAGS_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.ORDERED_FLAG_MASK;
import static org.apache.datasketches.theta.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.READ_ONLY_FLAG_MASK;
import static org.apache.datasketches.theta.PreambleUtil.SEED_HASH_SHORT;
import static org.apache.datasketches.theta.PreambleUtil.extractEntryBitsV4;
import static org.apache.datasketches.theta.PreambleUtil.extractFamilyID;
import static org.apache.datasketches.theta.PreambleUtil.extractFlags;
import static org.apache.datasketches.theta.PreambleUtil.extractNumEntriesBytesV4;
import static org.apache.datasketches.theta.PreambleUtil.extractPreLongs;
import static org.apache.datasketches.theta.PreambleUtil.extractSeedHash;
import static org.apache.datasketches.theta.PreambleUtil.extractSerVer;
import static org.apache.datasketches.theta.PreambleUtil.extractThetaLongV4;
import static org.apache.datasketches.theta.PreambleUtil.wholeBytesToHoldBits;
import static org.apache.datasketches.theta.SingleItemSketch.otherCheckForSingleItem;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.Util;

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

  /**
   * Heapify takes a CompactSketch image in a MemorySegment and instantiates an on-heap CompactSketch.
   *
   * <p>The resulting sketch will not retain any link to the source MemorySegment and all of its data will be
   * copied to the heap CompactSketch.</p>
   *
   * <p>This method assumes that the sketch image was created with the correct hash seed, so it is not checked.
   * The resulting on-heap CompactSketch will be given the seedHash derived from the given sketch image.
   * However, Serial Version 1 sketch images do not have a seedHash field,
   * so the resulting heapified CompactSketch will be given the hash of the DEFAULT_UPDATE_SEED.</p>
   *
   * @param srcSeg an image of a CompactSketch.
   * @return a CompactSketch on the heap.
   */
  public static CompactSketch heapify(final MemorySegment srcSeg) {
    return heapify(srcSeg, Util.DEFAULT_UPDATE_SEED, false);
  }

  /**
   * Heapify takes a CompactSketch image in a MemorySegment and instantiates an on-heap CompactSketch.
   *
   * <p>The resulting sketch will not retain any link to the source MemorySegment and all of its data will be
   * copied to the heap CompactSketch.</p>
   *
   * <p>This method checks if the given expectedSeed was used to create the source MemorySegment image.
   * However, SerialVersion 1 sketch images cannot be checked as they don't have a seedHash field,
   * so the resulting heapified CompactSketch will be given the hash of the expectedSeed.</p>
   *
   * @param srcSeg an image of a CompactSketch that was created using the given expectedSeed.
   * @param expectedSeed the seed used to validate the given MemorySegment image.
   * <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
   * @return a CompactSketch on the heap.
   */
  public static CompactSketch heapify(final MemorySegment srcSeg, final long expectedSeed) {
    return heapify(srcSeg, expectedSeed, true);
  }

  private static CompactSketch heapify(final MemorySegment srcSeg, final long seed, final boolean enforceSeed) {
    final int serVer = extractSerVer(srcSeg);
    final int familyID = extractFamilyID(srcSeg);
    final Family family = idToFamily(familyID);
    if (family != Family.COMPACT) {
      throw new SketchesArgumentException("Corrupted: " + family + " is not Compact!");
    }
    if (serVer == 4) {
       return heapifyV4(srcSeg, seed, enforceSeed);
    }
    if (serVer == 3) {
      final int flags = extractFlags(srcSeg);
      final boolean srcOrdered = (flags & ORDERED_FLAG_MASK) != 0;
      final boolean empty = (flags & EMPTY_FLAG_MASK) != 0;
      if (enforceSeed && !empty) { PreambleUtil.checkSegmentSeedHash(srcSeg, seed); }
      return CompactOperations.segmentToCompact(srcSeg, srcOrdered, null);
    }
    //not SerVer 3, assume compact stored form
    final short seedHash = Util.computeSeedHash(seed);
    if (serVer == 1) {
      return ForwardCompatibility.heapify1to3(srcSeg, seedHash);
    }
    if (serVer == 2) {
      return ForwardCompatibility.heapify2to3(srcSeg,
          enforceSeed ? seedHash : (short) extractSeedHash(srcSeg));
    }
    throw new SketchesArgumentException("Unknown Serialization Version: " + serVer);
  }

  /**
   * Wrap takes the CompactSketch image in given MemorySegment and refers to it directly.
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
   * This is actually faster and consumes less overall space.</p>
   *
   * <p>This method assumes that the sketch image was created with the correct hash seed, so it is not checked.
   * However, Serial Version 1 sketch images do not have a seedHash field,
   * so the resulting on-heap CompactSketch will be given the hash of the DEFAULT_UPDATE_SEED.</p>
   *
   * @param srcSeg an image of a Sketch.
   * @return a CompactSketch backed by the given MemorySegment except as above.
   */
  public static CompactSketch wrap(final MemorySegment srcSeg) {
    return wrap(srcSeg, Util.DEFAULT_UPDATE_SEED, false);
  }

  /**
   * Wrap takes the sketch image in the given MemorySegment and refers to it directly.
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
   * This is actually faster and consumes less overall space.</p>
   *
   * <p>This method checks if the given expectedSeed was used to create the source MemorySegment image.
   * However, SerialVersion 1 sketches cannot be checked as they don't have a seedHash field,
   * so the resulting heapified CompactSketch will be given the hash of the expectedSeed.</p>
   *
   * @param srcSeg an image of a Sketch that was created using the given expectedSeed.
   * @param expectedSeed the seed used to validate the given MemorySegment image.
   * <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
   * @return a CompactSketch backed by the given MemorySegment except as above.
   */
  public static CompactSketch wrap(final MemorySegment srcSeg, final long expectedSeed) {
    return wrap(srcSeg, expectedSeed, true);
  }

  private static CompactSketch wrap(final MemorySegment srcSeg, final long seed, final boolean enforceSeed) {
    final int serVer = extractSerVer(srcSeg);
    final int familyID = extractFamilyID(srcSeg);
    final Family family = Family.idToFamily(familyID);
    if (family != Family.COMPACT) {
      throw new SketchesArgumentException("Corrupted: " + family + " is not Compact!");
    }
    final short seedHash = Util.computeSeedHash(seed);

    switch (serVer) {
      case 1: {
        return ForwardCompatibility.heapify1to3(srcSeg, seedHash);
      }
      case 2: {
        return ForwardCompatibility.heapify2to3(srcSeg,
            enforceSeed ? seedHash : (short) extractSeedHash(srcSeg));
      }
      case 3: {
        if (PreambleUtil.isEmptyFlag(srcSeg)) {
          return EmptyCompactSketch.getHeapInstance(srcSeg);
        }
        if (otherCheckForSingleItem(srcSeg)) {
          return SingleItemSketch.heapify(srcSeg, enforceSeed ? seedHash : (short) extractSeedHash(srcSeg));
        }
        //not empty & not singleItem
        final int flags = extractFlags(srcSeg);
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
        return DirectCompactSketch.wrapInstance(srcSeg,
            enforceSeed ? seedHash : (short) extractSeedHash(srcSeg));
      }
      case 4: {
        return DirectCompactCompressedSketch.wrapInstance(srcSeg,
            enforceSeed ? seedHash : (short) extractSeedHash(srcSeg));
      }
      default: {
        throw new SketchesArgumentException(
            "Corrupted: Serialization Version " + serVer + " not recognized.");
      }
    }
  }

  /**
   * Wrap takes the sketch image in the given byte array and refers to it directly.
   * There is no data copying onto the java heap.
   * The wrap operation enables fast read-only merging and access to all the public read-only API.
   *
   * <p>Only "Direct" Serialization Versions 3 and 4 (i.e, OpenSource) sketches that have
   * been explicitly stored as direct sketches can be wrapped.
   * Wrapping earlier serial version sketches will result in a heapify operation.
   * These early versions were never designed to "wrap".</p>
   *
   * <p>Wrapping any subclass of this class that is empty or contains only a single item will
   * result in heapified forms of empty and single item sketch respectively.
   * This is actually faster and consumes less overall space.</p>
   *
   * <p>This method checks if the DEFAULT_UPDATE_SEED was used to create the source byte array image.
   * Note that SerialVersion 1 (pre-open-source) sketches cannot be checked as they don't have a seedHash field,
   * so the resulting heapified CompactSketch will be given the hash of DEFAULT_UPDATE_SEED.</p>
   *
   * @param bytes a byte array image of a Sketch that was created using the DEFAULT_UPDATE_SEED.
   *
   * @return a CompactSketch backed by the given byte array except as above.
   */
  public static CompactSketch wrap(final byte[] bytes) {
    return wrap(bytes, Util.DEFAULT_UPDATE_SEED, false);
  }

  /**
   * Wrap takes the sketch image in the given  byte array and refers to it directly.
   * There is no data copying onto the java heap.
   * The wrap operation enables fast read-only merging and access to all the public read-only API.
   *
   * <p>Only "Direct" Serialization Versions 3 and 4 (i.e, OpenSource) sketches that have
   * been explicitly stored as direct sketches can be wrapped.
   * Wrapping earlier serial version sketches will result in a heapify operation.
   * These early versions were never designed to "wrap".</p>
   *
   * <p>Wrapping any subclass of this class that is empty or contains only a single item will
   * result in heapified forms of empty and single item sketch respectively.
   * This is actually faster and consumes less overall space.</p>
   *
   * <p>This method checks if the given expectedSeed was used to create the source byte array image.
   * Note that SerialVersion 1 sketches cannot be checked as they don't have a seedHash field,
   * so the resulting heapified CompactSketch will be given the hash of the expectedSeed.</p>
   *
   * @param bytes a byte array image of a Sketch that was created using the given expectedSeed.
   * @param expectedSeed the seed used to validate the given byte array image.
   * <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
   * @return a CompactSketch backed by the given byte array except as above.
   */
  public static CompactSketch wrap(final byte[] bytes, final long expectedSeed) {
    return wrap(bytes, expectedSeed, true);
  }

  private static CompactSketch wrap(final byte[] bytes, final long seed, final boolean enforceSeed) {
    final int serVer = bytes[PreambleUtil.SER_VER_BYTE];
    final int familyId = bytes[PreambleUtil.FAMILY_BYTE];
    final Family family = Family.idToFamily(familyId);
    if (family != Family.COMPACT) {
      throw new SketchesArgumentException("Corrupted: " + family + " is not Compact!");
    }
    final short seedHash = Util.computeSeedHash(seed);

    switch (serVer) {
      case 1: {
        return ForwardCompatibility.heapify1to3(MemorySegment.ofArray(bytes), seedHash);
      }
      case 2: {
        return ForwardCompatibility.heapify2to3(MemorySegment.ofArray(bytes),
            enforceSeed ? seedHash : getShortLE(bytes, SEED_HASH_SHORT));
      }
      case 3: {
        final int flags = bytes[FLAGS_BYTE];
        if ((flags & EMPTY_FLAG_MASK) > 0) {
          return EmptyCompactSketch.getHeapInstance(MemorySegment.ofArray(bytes));
        }
        final int preLongs = bytes[PREAMBLE_LONGS_BYTE];
        if (otherCheckForSingleItem(preLongs, serVer, familyId, flags)) {
          return SingleItemSketch.heapify(MemorySegment.ofArray(bytes), enforceSeed ? seedHash : getShortLE(bytes, SEED_HASH_SHORT));
        }
        //not empty & not singleItem
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
        return WrappedCompactSketch.wrapInstance(bytes,
            enforceSeed ? seedHash : getShortLE(bytes, SEED_HASH_SHORT));
      }
      case 4: {
        return WrappedCompactCompressedSketch.wrapInstance(bytes, seedHash);
      }
      default: {
        throw new SketchesArgumentException(
            "Corrupted: Serialization Version " + serVer + " not recognized.");
      }
    }
  }

  //Sketch Overrides

  @Override
  public abstract CompactSketch compact(final boolean dstOrdered, final MemorySegment dstSeg);

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
  public boolean hasMemorySegment() {
    return ((this instanceof DirectCompactSketch) &&  ((DirectCompactSketch)this).hasMemorySegment());
  }

  @Override
  public boolean isCompact() {
    return true;
  }

  @Override
  public boolean isOffHeap() {
    return ((this instanceof DirectCompactSketch) && ((DirectCompactSketch)this).isOffHeap());
  }

  @Override
  public boolean isSameResource(final MemorySegment that) {
    return ((this instanceof DirectCompactSketch) &&  ((DirectCompactSketch)this).isSameResource(that));
  }

  @Override
  public double getEstimate() {
    return Sketch.estimate(getThetaLong(), getRetainedEntries());
  }

  /**
   * gets the sketch as a compressed byte array
   * @return the sketch as a compressed byte array
   */
  public byte[] toByteArrayCompressed() {
    if (!isOrdered() || (getRetainedEntries() == 0) || ((getRetainedEntries() == 1) && !isEstimationMode())) {
      return toByteArray();
    }
    return toByteArrayV4();
  }

  private int computeMinLeadingZeros() {
    // compression is based on leading zeros in deltas between ordered hash values
    // assumes ordered sketch
    long previous = 0;
    long ored = 0;
    final HashIterator it = iterator();
    while (it.next()) {
      final long delta = it.get() - previous;
      ored |= delta;
      previous = it.get();
    }
    return Long.numberOfLeadingZeros(ored);
  }

  private byte[] toByteArrayV4() {
    final int preambleLongs = isEstimationMode() ? 2 : 1;
    final int entryBits = 64 - computeMinLeadingZeros();
    final int compressedBits = entryBits * getRetainedEntries();

    // store num_entries as whole bytes since whole-byte blocks will follow (most probably)
    final int numEntriesBytes = wholeBytesToHoldBits(32 - Integer.numberOfLeadingZeros(getRetainedEntries()));

    final int sizeBytes = (preambleLongs * Long.BYTES) + numEntriesBytes + wholeBytesToHoldBits(compressedBits);
    final byte[] bytes = new byte[sizeBytes];
    final MemorySegment wseg = MemorySegment.ofArray(bytes);
    int offsetBytes = 0;
    wseg.set(JAVA_BYTE, offsetBytes++, (byte) preambleLongs);
    wseg.set(JAVA_BYTE, offsetBytes++, (byte) 4); // to do: add constant
    wseg.set(JAVA_BYTE, offsetBytes++, (byte) Family.COMPACT.getID());
    wseg.set(JAVA_BYTE, offsetBytes++, (byte) entryBits);
    wseg.set(JAVA_BYTE, offsetBytes++, (byte) numEntriesBytes);
    wseg.set(JAVA_BYTE, offsetBytes++, (byte) (COMPACT_FLAG_MASK | READ_ONLY_FLAG_MASK | ORDERED_FLAG_MASK));
    wseg.set(JAVA_SHORT_UNALIGNED, offsetBytes, getSeedHash());
    offsetBytes += Short.BYTES;
    if (isEstimationMode()) {
      wseg.set(JAVA_LONG_UNALIGNED, offsetBytes, getThetaLong());
      offsetBytes += Long.BYTES;
    }
    int numEntries = getRetainedEntries();
    for (int i = 0; i < numEntriesBytes; i++) {
      wseg.set(JAVA_BYTE, offsetBytes++, (byte) (numEntries & 0xff));
      numEntries >>>= 8;
    }
    long previous = 0;
    final long[] deltas = new long[8];
    final HashIterator it = iterator();
    int i;
    for (i = 0; (i + 7) < getRetainedEntries(); i += 8) {
      for (int j = 0; j < 8; j++) {
        it.next();
        deltas[j] = it.get() - previous;
        previous = it.get();
      }
      BitPacking.packBitsBlock8(deltas, 0, bytes, offsetBytes, entryBits);
      offsetBytes += entryBits;
    }
    int offsetBits = 0;
    for (; i < getRetainedEntries(); i++) {
      it.next();
      final long delta = it.get() - previous;
      previous = it.get();
      BitPacking.packBits(delta, entryBits, bytes, offsetBytes, offsetBits);
      offsetBytes += (offsetBits + entryBits) >>> 3;
      offsetBits = (offsetBits + entryBits) & 7;
    }
    return bytes;
  }

  private static CompactSketch heapifyV4(final MemorySegment srcSeg, final long seed, final boolean enforceSeed) {
    final int preLongs = extractPreLongs(srcSeg);
    final int entryBits = extractEntryBitsV4(srcSeg);
    final int numEntriesBytes = extractNumEntriesBytesV4(srcSeg);
    final short seedHash = (short) extractSeedHash(srcSeg);
    if (enforceSeed) { PreambleUtil.checkSegmentSeedHash(srcSeg, seed); }
    int offsetBytes = 8;
    long theta = Long.MAX_VALUE;
    if (preLongs > 1) {
      theta = extractThetaLongV4(srcSeg);
      offsetBytes += Long.BYTES;
    }
    int numEntries = 0;
    for (int i = 0; i < numEntriesBytes; i++) {
      numEntries |= Byte.toUnsignedInt(srcSeg.get(JAVA_BYTE, offsetBytes++)) << (i << 3);
    }
    final long[] entries = new long[numEntries];
    final byte[] bytes = new byte[entryBits]; // temporary buffer for unpacking
    int i;
    for (i = 0; (i + 7) < numEntries; i += 8) {
      MemorySegment.copy(srcSeg, JAVA_BYTE, offsetBytes, bytes, 0, entryBits);
      BitPacking.unpackBitsBlock8(entries, i, bytes, 0, entryBits);
      offsetBytes += entryBits;
    }
    if (i < numEntries) {
      MemorySegment.copy(srcSeg, JAVA_BYTE, offsetBytes, bytes, 0, wholeBytesToHoldBits((numEntries - i) * entryBits));
      int offsetBits = 0;
      offsetBytes = 0;
      for (; i < numEntries; i++) {
        BitPacking.unpackBits(entries, i, entryBits, bytes, offsetBytes, offsetBits);
        offsetBytes += (offsetBits + entryBits) >>> 3;
        offsetBits = (offsetBits + entryBits) & 7;
      }
    }
    // undo deltas
    long previous = 0;
    for (i = 0; i < numEntries; i++) {
      entries[i] += previous;
      previous = entries[i];
    }
    return new HeapCompactSketch(entries, false, seedHash, numEntries, theta, true);
  }

}

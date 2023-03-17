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

import static org.apache.datasketches.common.Family.idToFamily;
import static org.apache.datasketches.theta.PreambleUtil.COMPACT_FLAG_MASK;
import static org.apache.datasketches.theta.PreambleUtil.EMPTY_FLAG_MASK;
import static org.apache.datasketches.theta.PreambleUtil.ORDERED_FLAG_MASK;
import static org.apache.datasketches.theta.PreambleUtil.READ_ONLY_FLAG_MASK;
import static org.apache.datasketches.theta.PreambleUtil.extractFamilyID;
import static org.apache.datasketches.theta.PreambleUtil.extractFlags;
import static org.apache.datasketches.theta.PreambleUtil.extractPreLongs;
import static org.apache.datasketches.theta.PreambleUtil.extractSeedHash;
import static org.apache.datasketches.theta.PreambleUtil.extractSerVer;
import static org.apache.datasketches.theta.PreambleUtil.extractEntryBitsV4;
import static org.apache.datasketches.theta.PreambleUtil.extractNumEntriesBytesV4;
import static org.apache.datasketches.theta.PreambleUtil.extractThetaLongV4;
import static org.apache.datasketches.theta.SingleItemSketch.otherCheckForSingleItem;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.thetacommon.ThetaUtil;

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
    return heapify(srcMem, ThetaUtil.DEFAULT_UPDATE_SEED, false);
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
    return heapify(srcMem, expectedSeed, true);
  }

  private static CompactSketch heapify(final Memory srcMem, final long seed, final boolean enforceSeed) {
    final int serVer = extractSerVer(srcMem);
    final int familyID = extractFamilyID(srcMem);
    final Family family = idToFamily(familyID);
    if (family != Family.COMPACT) {
      throw new IllegalArgumentException("Corrupted: " + family + " is not Compact!");
    }
    if (serVer == 4) {
       return heapifyV4(srcMem, seed, enforceSeed);
    }
    if (serVer == 3) {
      final int flags = extractFlags(srcMem);
      final boolean srcOrdered = (flags & ORDERED_FLAG_MASK) != 0;
      final boolean empty = (flags & EMPTY_FLAG_MASK) != 0;
      if (enforceSeed && !empty) { PreambleUtil.checkMemorySeedHash(srcMem, seed); }
      return CompactOperations.memoryToCompact(srcMem, srcOrdered, null);
    }
    //not SerVer 3, assume compact stored form
    final short seedHash = ThetaUtil.computeSeedHash(seed);
    if (serVer == 1) {
      return ForwardCompatibility.heapify1to3(srcMem, seedHash);
    }
    if (serVer == 2) {
      return ForwardCompatibility.heapify2to3(srcMem,
          enforceSeed ? seedHash : (short) extractSeedHash(srcMem));
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
    return wrap(srcMem, ThetaUtil.DEFAULT_UPDATE_SEED, false);
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
    return wrap(srcMem, expectedSeed, true);
  }

  private static CompactSketch wrap(final Memory srcMem, final long seed, final boolean enforceSeed) {
    final int serVer = extractSerVer(srcMem);
    final int familyID = extractFamilyID(srcMem);
    final Family family = Family.idToFamily(familyID);
    if (family != Family.COMPACT) {
      throw new IllegalArgumentException("Corrupted: " + family + " is not Compact!");
    }
    final short seedHash = ThetaUtil.computeSeedHash(seed);

    if (serVer == 4) {
      // not wrapping the compressed format since currently we cannot take advantage of
      // decompression during iteration because set operations reach into memory directly
      return heapifyV4(srcMem, seed, enforceSeed);
    }
    else if (serVer == 3) {
      if (PreambleUtil.isEmptyFlag(srcMem)) {
        return EmptyCompactSketch.getHeapInstance(srcMem);
      }
      if (otherCheckForSingleItem(srcMem)) {
        return SingleItemSketch.heapify(srcMem, enforceSeed ? seedHash : (short) extractSeedHash(srcMem));
      }
      //not empty & not singleItem
      final int flags = extractFlags(srcMem);
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
      return DirectCompactSketch.wrapInstance(srcMem,
          enforceSeed ? seedHash : (short) extractSeedHash(srcMem));
    } //end of serVer 3
    else if (serVer == 1) {
      return ForwardCompatibility.heapify1to3(srcMem, seedHash);
    }
    else if (serVer == 2) {
      return ForwardCompatibility.heapify2to3(srcMem,
          enforceSeed ? seedHash : (short) extractSeedHash(srcMem));
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

  public byte[] toByteArrayCompressed() {
    if (!isOrdered() || getRetainedEntries() == 0 || (getRetainedEntries() == 1 && !isEstimationMode())) {
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

  private static int wholeBytesToHoldBits(final int bits) {
    return (bits >>> 3) + ((bits & 7) > 0 ? 1 : 0);
  }

  private byte[] toByteArrayV4() {
    final int preambleLongs = isEstimationMode() ? 2 : 1;
    final int entryBits = 64 - computeMinLeadingZeros();
    final int compressedBits = entryBits * getRetainedEntries();

    // store num_entries as whole bytes since whole-byte blocks will follow (most probably)
    final int numEntriesBytes = wholeBytesToHoldBits(32 - Integer.numberOfLeadingZeros(getRetainedEntries()));

    final int size = preambleLongs * Long.BYTES + numEntriesBytes + wholeBytesToHoldBits(compressedBits);
    final byte[] bytes = new byte[size];
    final WritableMemory mem = WritableMemory.writableWrap(bytes);
    int offsetBytes = 0;
    mem.putByte(offsetBytes++, (byte) preambleLongs);
    mem.putByte(offsetBytes++, (byte) 4); // to do: add constant
    mem.putByte(offsetBytes++, (byte) Family.COMPACT.getID());
    mem.putByte(offsetBytes++, (byte) entryBits);
    mem.putByte(offsetBytes++, (byte) numEntriesBytes);
    mem.putByte(offsetBytes++, (byte) (COMPACT_FLAG_MASK | READ_ONLY_FLAG_MASK | ORDERED_FLAG_MASK));
    mem.putShort(offsetBytes, getSeedHash());
    offsetBytes += Short.BYTES;
    if (isEstimationMode()) {
      mem.putLong(offsetBytes, getThetaLong());
      offsetBytes += Long.BYTES;
    }
    int numEntries = getRetainedEntries();
    for (int i = 0; i < numEntriesBytes; i++) {
      mem.putByte(offsetBytes++, (byte) (numEntries & 0xff));
      numEntries >>>= 8;
    }
    long previous = 0;
    final long[] deltas = new long[8];
    final HashIterator it = iterator();
    int i;
    for (i = 0; i + 7 < getRetainedEntries(); i += 8) {
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

  private static CompactSketch heapifyV4(final Memory srcMem, final long seed, final boolean enforceSeed) {
    final int preLongs = extractPreLongs(srcMem);
    final int flags = extractFlags(srcMem);
    final int entryBits = extractEntryBitsV4(srcMem);
    final int numEntriesBytes = extractNumEntriesBytesV4(srcMem);
    final short seedHash = (short) extractSeedHash(srcMem);
    final boolean isEmpty = (flags & EMPTY_FLAG_MASK) > 0;
    if (enforceSeed && !isEmpty) { PreambleUtil.checkMemorySeedHash(srcMem, seed); }
    int offsetBytes = 8;
    long theta = Long.MAX_VALUE;
    if (preLongs > 1) {
      theta = extractThetaLongV4(srcMem);
      offsetBytes += Long.BYTES;
    }
    int numEntries = 0;
    for (int i = 0; i < numEntriesBytes; i++) {
      numEntries |= Byte.toUnsignedInt(srcMem.getByte(offsetBytes++)) << (i << 3);
    }
    final long[] entries = new long[numEntries];
    final byte[] bytes = new byte[entryBits]; // temporary buffer for unpacking
    int i;
    for (i = 0; i + 7 < numEntries; i += 8) {
      srcMem.getByteArray(offsetBytes, bytes, 0, entryBits);
      BitPacking.unpackBitsBlock8(entries, i, bytes, 0, entryBits);
      offsetBytes += entryBits;
    }
    if (i < numEntries) {
      srcMem.getByteArray(offsetBytes, bytes, 0, wholeBytesToHoldBits((numEntries - i) * entryBits));
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
    return new HeapCompactSketch(entries, isEmpty, seedHash, numEntries, theta, true);
  }

}

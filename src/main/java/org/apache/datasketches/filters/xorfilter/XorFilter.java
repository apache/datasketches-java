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

package org.apache.datasketches.filters.xorfilter;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_SHORT_UNALIGNED;
import static org.apache.datasketches.common.Util.LS;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.MemorySegmentStatus;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.SketchesStateException;
import org.apache.datasketches.common.positional.PositionalSegment;
import org.apache.datasketches.hash.XxHash;
import org.apache.datasketches.hash.XxHash64;

/**
 * A xor filter is an immutable data structure that can be used for probabilistic
 * set membership, much like a Bloom filter but smaller and faster.
 *
 * <p>When querying a xor filter, there are no false negatives. Specifically:
 * When querying an item that was presented to the filter when it was built, the filter will
 * always indicate that the item is present. There is a chance of false positives, where
 * querying an item that was never presented to the filter will indicate that the
 * item has been seen. Consequently, any query should be interpreted as
 * "might have seen."</p>
 *
 * <p>Unlike a Bloom filter, a xor filter is built once from the full set of items and cannot
 * be updated afterwards. The filter stores a small fingerprint per item in an array whose
 * size is about 1.23 times the number of distinct items, giving a false positive probability
 * of approximately 1 / 2^bits, where bits is the configured fingerprint width (8 or 16). This
 * is close to the information-theoretic lower bound and uses less memory than a Bloom filter
 * for the same false positive probability.</p>
 *
 * <p>See the XorFilterBuilder class for methods to accumulate items and build a filter.</p>
 *
 * <p>This implementation uses xxHash64 to reduce items to 64-bit keys and follows the approach in
 * Graf and Lemire, "Xor Filters: Faster and Smaller Than Bloom and Cuckoo Filters,"
 * ACM Journal of Experimental Algorithmics, 2020.</p>
 */
public final class XorFilter implements MemorySegmentStatus {
  private static final int SER_VER = 1;
  private static final int PREAMBLE_LONGS = 3;
  private static final long PREAMBLE_SIZE_BYTES = (long) PREAMBLE_LONGS * Long.BYTES;

  // number of hash functions / segments used by the three-partite construction
  static final int NUM_HASHES = 3;
  // fixed seed used to reduce items to 64-bit keys, distinct from the construction seed
  static final long HASH_SEED = 0L;

  private static final double LOAD_FACTOR = 1.23;
  private static final int CAPACITY_OFFSET = 32;
  private static final int MAX_ITERATIONS = 100;

  // MurmurHash3 64-bit finalizer constants (also used by the paper's Algorithm 5)
  private static final long MURMUR_C1 = 0xff51afd7ed558ccdL;
  private static final long MURMUR_C2 = 0xc4ceb9fe1a85ec53L;

  // splitmix64 constants, used to draw candidate construction seeds
  private static final long SPLITMIX_GAMMA = 0x9e3779b97f4a7c15L;
  private static final long SPLITMIX_MUL1 = 0xbf58476d1ce4e5b9L;
  private static final long SPLITMIX_MUL2 = 0x94d049bb133111ebL;

  private final int bitsPerFingerprint_;    // fingerprint width in bits, 8 or 16
  private final int segmentLength_;          // number of fingerprint slots per hash segment
  private final int numKeys_;                // number of distinct keys used to build the filter
  private final long seed_;                  // winning construction seed for the murmur mix
  private final MemorySegment fpSeg_;        // holds the raw fingerprint payload
  private final MemorySegment wseg_;         // used only when wrapping a serialized image

  /**
   * Builds a xor filter from the provided distinct keys and a base construction seed.
   * The keys must already be distinct; duplicates cause construction to fail.
   *
   * @param bitsPerFingerprint The fingerprint width in bits, either 8 or 16
   * @param seed A base seed used to draw candidate construction seeds
   * @param keys An array of distinct 64-bit keys, using only the first numKeys entries
   * @param numKeys The number of distinct keys to read from the keys array
   */
  XorFilter(final int bitsPerFingerprint, final long seed, final long[] keys, final int numKeys) {
    bitsPerFingerprint_ = bitsPerFingerprint;
    numKeys_ = numKeys;

    final int capacity = computeCapacity(numKeys);
    segmentLength_ = capacity / NUM_HASHES;
    final int bytesPerFingerprint = bitsPerFingerprint >>> 3;
    final long fingerprintBytes = (long) capacity * bytesPerFingerprint;
    if (fingerprintBytes > Integer.MAX_VALUE) {
      throw new SketchesArgumentException("Requested xor filter is too large to allocate: " + fingerprintBytes
          + " fingerprint bytes exceeds " + Integer.MAX_VALUE);
    }
    fpSeg_ = MemorySegment.ofArray(new byte[(int) fingerprintBytes]);
    wseg_ = null;

    // peeling accumulators, indexed by absolute slot in [0, capacity)
    final long[] xorMask = new long[capacity];
    final int[] count = new int[capacity];
    final int[] queue = new int[capacity];
    final long[] stackHash = new long[numKeys];
    final int[] stackIndex = new int[numKeys];

    long rngState = seed;
    long buildSeed = 0L;
    int stackSize = 0;

    // repeatedly map the keys onto the slots and peel until every key owns a unique slot,
    // reseeding on the rare occasion that a two-core remains
    for (int attempt = 0; attempt < MAX_ITERATIONS; ++attempt) {
      rngState += SPLITMIX_GAMMA;
      buildSeed = splitMix64(rngState);
      stackSize = map(keys, numKeys, buildSeed, xorMask, count, queue, stackHash, stackIndex);
      if (stackSize == numKeys) { break; }
      if (attempt == (MAX_ITERATIONS - 1)) {
        throw new SketchesArgumentException("Xor filter construction failed after " + MAX_ITERATIONS
            + " attempts; the input likely contains duplicate keys or is degenerate.");
      }
    }

    seed_ = buildSeed;
    assign(stackHash, stackIndex, stackSize);
  }

  // Constructor used with internalHeapifyOrWrap()
  XorFilter(final int bitsPerFingerprint, final int segmentLength, final int numKeys, final long seed,
      final MemorySegment fpSeg, final MemorySegment wseg) {
    bitsPerFingerprint_ = bitsPerFingerprint;
    segmentLength_ = segmentLength;
    numKeys_ = numKeys;
    seed_ = seed;
    fpSeg_ = fpSeg;
    wseg_ = wseg;
  }

  /**
   * Reads a serialized image of a XorFilter from the provided MemorySegment.
   * @param seg MemorySegment containing a previously serialized XorFilter
   * @return a XorFilter object
   */
  public static XorFilter heapify(final MemorySegment seg) {
    return internalHeapifyOrWrap(seg, false);
  }

  /**
   * Wraps the given MemorySegment into this filter class. The class itself only contains a few metadata items and holds
   * a reference to the MemorySegment object, which contains all the data. The wrapped filter is read-only.
   * @param seg the given MemorySegment object
   * @return the wrapping XorFilter class.
   */
  public static XorFilter wrap(final MemorySegment seg) {
    return internalHeapifyOrWrap(seg, true);
  }

  private static XorFilter internalHeapifyOrWrap(final MemorySegment seg, final boolean isWrap) {
    checkArgument(seg.byteSize() < PREAMBLE_SIZE_BYTES,
        "Possible corruption: MemorySegment capacity is smaller than the required preamble size");

    final PositionalSegment posSeg = PositionalSegment.wrap(seg);
    final int preLongs = posSeg.getByte();
    final int serVer = posSeg.getByte();
    final int familyID = posSeg.getByte();
    posSeg.getByte(); // flags, reserved

    checkArgument((preLongs < Family.XORFILTER.getMinPreLongs()) || (preLongs > Family.XORFILTER.getMaxPreLongs()),
        "Possible corruption: Incorrect number of preamble bytes specified in header");
    checkArgument(serVer != SER_VER, "Possible corruption: Unrecognized serialization version: " + serVer);
    checkArgument(familyID != Family.XORFILTER.getID(), "Possible corruption: Incorrect FamilyID for xor filter. Found: " + familyID);

    final int bitsPerFingerprint = posSeg.getByte();
    checkArgument((bitsPerFingerprint != 8) && (bitsPerFingerprint != 16),
        "Possible corruption: Fingerprint width must be 8 or 16. Found: " + bitsPerFingerprint);
    final int numHashes = posSeg.getByte();
    checkArgument(numHashes != NUM_HASHES, "Possible corruption: Number of hash functions must be " + NUM_HASHES
        + ". Found: " + numHashes);
    posSeg.getShort(); // unused

    final long seed = posSeg.getLong();
    final int segmentLength = posSeg.getInt();
    final int numKeys = posSeg.getInt();
    checkArgument(segmentLength < 1, "Possible corruption: Segment length must be strictly positive. Found: " + segmentLength);
    checkArgument(numKeys < 0, "Possible corruption: Number of keys must be non-negative. Found: " + numKeys);

    final long fingerprintBytes = (long) numHashes * segmentLength * (bitsPerFingerprint >>> 3);
    checkArgument(seg.byteSize() < (PREAMBLE_SIZE_BYTES + fingerprintBytes),
        "Possible corruption: MemorySegment capacity insufficient to hold the fingerprint array");

    final MemorySegment payload = seg.asSlice(PREAMBLE_SIZE_BYTES, fingerprintBytes);
    if (isWrap) {
      return new XorFilter(bitsPerFingerprint, segmentLength, numKeys, seed, payload, seg);
    }
    // heapify copies the payload onto the Java heap
    checkArgument(fingerprintBytes > Integer.MAX_VALUE, "Filter is too large to heapify; use wrap() instead");
    final byte[] fingerprints = new byte[(int) fingerprintBytes];
    MemorySegment.copy(payload, JAVA_BYTE, 0, fingerprints, 0, (int) fingerprintBytes);
    return new XorFilter(bitsPerFingerprint, segmentLength, numKeys, seed, MemorySegment.ofArray(fingerprints), null);
  }

  /**
   * Checks if the XorFilter was built from any items.
   * @return True if the XorFilter holds no items, otherwise False
   */
  public boolean isEmpty() { return numKeys_ == 0; }

  /**
   * Returns the number of distinct items used to build this XorFilter.
   * @return The number of distinct items in this filter
   */
  public int getNumItems() { return numKeys_; }

  /**
   * Returns the fixed number of hash functions used by the three-partite xor construction.
   * @return The number of hash functions used by this filter
   */
  public int getNumHashes() { return NUM_HASHES; }

  /**
   * Returns the fingerprint width, in bits, for this XorFilter.
   * @return The number of bits stored per fingerprint
   */
  public int getBitsPerFingerprint() { return bitsPerFingerprint_; }

  /**
   * Returns the total number of fingerprint slots in this XorFilter.
   * @return The number of fingerprint slots in the filter
   */
  public long getCapacity() { return (long) NUM_HASHES * segmentLength_; }

  /**
   * Returns the construction seed for this XorFilter.
   * @return The construction seed for this filter
   */
  public long getSeed() { return seed_; }

  /**
   * Returns the average number of bits stored per item by this XorFilter, a measure of its
   * space efficiency. This is approximately 1.23 times the fingerprint width.
   * @return The number of bits used per item, or 0 for an empty filter
   */
  public double getBitsPerItem() {
    if (numKeys_ == 0) { return 0.0; }
    return ((double) getCapacity() * bitsPerFingerprint_) / numKeys_;
  }

  @Override
  public boolean hasMemorySegment() { return wseg_ != null; }

  /**
   * Returns whether the filter is in read-only mode. That is possible
   * only if there is a backing MemorySegment, as a wrapped filter is never modified.
   * @return true if read-only, otherwise false
   */
  public boolean isReadOnly() { return wseg_ != null; }

  @Override
  public boolean isOffHeap() {
    return hasMemorySegment() && wseg_.isNative();
  }

  @Override
  public boolean isSameResource(final MemorySegment that) {
    return MemorySegmentStatus.isSameResource(wseg_, that);
  }

  // QUERY METHODS
  /**
   * Queries the filter with the provided long and returns whether the
   * value <em>might</em> have been seen previously. The filter's expected
   * False Positive Probability determines the chances of a true result being
   * a false positive. False negatives are never possible.
   * @param item an item with which to query the filter
   * @return The result of querying the filter with the given item
   */
  public boolean query(final long item) {
    return queryInternal(XxHash.hashLong(item, HASH_SEED));
  }

  /**
   * Queries the filter with the provided double and returns whether the
   * value <em>might</em> have been seen previously. The filter's expected
   * False Positive Probability determines the chances of a true result being
   * a false positive. False negatives are never possible. Double values are
   * canonicalized (NaN and +/- infinity) before querying.
   * @param item an item with which to query the filter
   * @return The result of querying the filter with the given item
   */
  public boolean query(final double item) {
    // canonicalize all NaN & +/- infinity forms
    final long[] data = { Double.doubleToLongBits(item) };
    return queryInternal(XxHash.hashLongArr(data, 0, 1, HASH_SEED));
  }

  /**
   * Queries the filter with the provided String and returns whether the
   * value <em>might</em> have been seen previously. The filter's expected
   * False Positive Probability determines the chances of a true result being
   * a false positive. False negatives are never possible.
   * The string is converted to a byte array using UTF8 encoding.
   *
   * <p>Note: this will not produce the same output hash values as the {@link #query(char[])}
   * method and will generally be a little slower depending on the complexity of the UTF8 encoding.
   * </p>
   *
   * @param item an item with which to query the filter
   * @return The result of querying the filter with the given item, or false if item is null
   */
  public boolean query(final String item) {
    if ((item == null) || item.isEmpty()) { return false; }
    final byte[] strBytes = item.getBytes(StandardCharsets.UTF_8);
    return queryInternal(XxHash.hashByteArr(strBytes, 0, strBytes.length, HASH_SEED));
  }

  /**
   * Queries the filter with the provided byte[] and returns whether the
   * array <em>might</em> have been seen previously. The filter's expected
   * False Positive Probability determines the chances of a true result being
   * a false positive. False negatives are never possible.
   * @param data an array with which to query the filter
   * @return The result of querying the filter with the given data, or false if data is null
   */
  public boolean query(final byte[] data) {
    if (data == null) { return false; }
    return queryInternal(XxHash.hashByteArr(data, 0, data.length, HASH_SEED));
  }

  /**
   * Queries the filter with the provided char[] and returns whether the
   * array <em>might</em> have been seen previously. The filter's expected
   * False Positive Probability determines the chances of a true result being
   * a false positive. False negatives are never possible.
   * @param data an array with which to query the filter
   * @return The result of querying the filter with the given data, or false if data is null
   */
  public boolean query(final char[] data) {
    if (data == null) { return false; }
    return queryInternal(XxHash.hashCharArr(data, 0, data.length, HASH_SEED));
  }

  /**
   * Queries the filter with the provided short[] and returns whether the
   * array <em>might</em> have been seen previously. The filter's expected
   * False Positive Probability determines the chances of a true result being
   * a false positive. False negatives are never possible.
   * @param data an array with which to query the filter
   * @return The result of querying the filter with the given data, or false if data is null
   */
  public boolean query(final short[] data) {
    if (data == null) { return false; }
    return queryInternal(XxHash.hashShortArr(data, 0, data.length, HASH_SEED));
  }

  /**
   * Queries the filter with the provided int[] and returns whether the
   * array <em>might</em> have been seen previously. The filter's expected
   * False Positive Probability determines the chances of a true result being
   * a false positive. False negatives are never possible.
   * @param data an array with which to query the filter
   * @return The result of querying the filter with the given data, or false if data is null
   */
  public boolean query(final int[] data) {
    if (data == null) { return false; }
    return queryInternal(XxHash.hashIntArr(data, 0, data.length, HASH_SEED));
  }

  /**
   * Queries the filter with the provided long[] and returns whether the
   * array <em>might</em> have been seen previously. The filter's expected
   * False Positive Probability determines the chances of a true result being
   * a false positive. False negatives are never possible.
   * @param data an array with which to query the filter
   * @return The result of querying the filter with the given data, or false if data is null
   */
  public boolean query(final long[] data) {
    if (data == null) { return false; }
    return queryInternal(XxHash.hashLongArr(data, 0, data.length, HASH_SEED));
  }

  /**
   * Queries the filter with the provided MemorySegment and returns whether the
   * data <em>might</em> have been seen previously. The filter's expected
   * False Positive Probability determines the chances of a true result being
   * a false positive. False negatives are never possible.
   * @param seg a MemorySegment with which to query the filter
   * @return The result of querying the filter with the given MemorySegment, or false if seg is null
   */
  public boolean query(final MemorySegment seg) {
    if (seg == null) { return false; }
    return queryInternal(XxHash64.hash(seg, 0, seg.byteSize(), HASH_SEED));
  }

  // Internal method to query the filter given a precomputed 64-bit key
  private boolean queryInternal(final long key) {
    final long hash = mix(key, seed_);
    final int fingerprint = fingerprint(hash);
    final int blockLength = segmentLength_;
    final int h0 = reduce((int) hash, blockLength);
    final int h1 = reduce((int) Long.rotateLeft(hash, 21), blockLength) + blockLength;
    final int h2 = reduce((int) Long.rotateLeft(hash, 42), blockLength) + (2 * blockLength);
    return fingerprint == (readFingerprint(h0) ^ readFingerprint(h1) ^ readFingerprint(h2));
  }

  // CONSTRUCTION HELPERS

  // Returns the number of fingerprint slots for a filter holding the given number of distinct keys.
  private static int computeCapacity(final int numKeys) {
    long capacity = CAPACITY_OFFSET + (long) (LOAD_FACTOR * numKeys);
    capacity = (capacity / NUM_HASHES) * NUM_HASHES; // round down to a multiple of the segment count
    if (capacity < NUM_HASHES) { capacity = NUM_HASHES; }
    if (capacity > Integer.MAX_VALUE) {
      throw new SketchesArgumentException("Requested xor filter capacity exceeds maximum. Requested keys: " + numKeys);
    }
    return (int) capacity;
  }

  // Maps every key onto its three slots then peels singletons onto a stack. Returns the stack size,
  // which equals numKeys when the mapping succeeds.
  private int map(final long[] keys, final int numKeys, final long buildSeed, final long[] xorMask,
      final int[] count, final int[] queue, final long[] stackHash, final int[] stackIndex) {
    final int blockLength = segmentLength_;
    Arrays.fill(count, 0);
    Arrays.fill(xorMask, 0L);

    for (int i = 0; i < numKeys; ++i) {
      final long hash = mix(keys[i], buildSeed);
      final int h0 = reduce((int) hash, blockLength);
      final int h1 = reduce((int) Long.rotateLeft(hash, 21), blockLength) + blockLength;
      final int h2 = reduce((int) Long.rotateLeft(hash, 42), blockLength) + (2 * blockLength);
      xorMask[h0] ^= hash; ++count[h0];
      xorMask[h1] ^= hash; ++count[h1];
      xorMask[h2] ^= hash; ++count[h2];
    }

    int queueLength = 0;
    for (int i = 0; i < count.length; ++i) {
      if (count[i] == 1) { queue[queueLength++] = i; }
    }

    int stackSize = 0;
    while (queueLength > 0) {
      final int index = queue[--queueLength];
      if (count[index] != 1) { continue; } // stale entry whose count has since dropped to zero
      final long hash = xorMask[index];
      stackHash[stackSize] = hash;
      stackIndex[stackSize] = index;
      ++stackSize;

      final int h0 = reduce((int) hash, blockLength);
      final int h1 = reduce((int) Long.rotateLeft(hash, 21), blockLength) + blockLength;
      final int h2 = reduce((int) Long.rotateLeft(hash, 42), blockLength) + (2 * blockLength);
      queueLength = removeKey(hash, h0, count, xorMask, queue, queueLength);
      queueLength = removeKey(hash, h1, count, xorMask, queue, queueLength);
      queueLength = removeKey(hash, h2, count, xorMask, queue, queueLength);
    }
    return stackSize;
  }

  // Removes a key's hash from a single slot, enqueuing the slot if it becomes a singleton.
  private static int removeKey(final long hash, final int index, final int[] count, final long[] xorMask,
      final int[] queue, final int queueLength) {
    --count[index];
    xorMask[index] ^= hash;
    if (count[index] == 1) {
      queue[queueLength] = index;
      return queueLength + 1;
    }
    return queueLength;
  }

  // Writes the fingerprints in reverse peel order so that each owned slot is finalized before it is
  // read as one of the two "other" slots of an earlier-peeled key.
  private void assign(final long[] stackHash, final int[] stackIndex, final int stackSize) {
    final int blockLength = segmentLength_;
    for (int s = stackSize - 1; s >= 0; --s) {
      final long hash = stackHash[s];
      final int index = stackIndex[s];
      final int h0 = reduce((int) hash, blockLength);
      final int h1 = reduce((int) Long.rotateLeft(hash, 21), blockLength) + blockLength;
      final int h2 = reduce((int) Long.rotateLeft(hash, 42), blockLength) + (2 * blockLength);
      // the owned slot is one of h0, h1, h2 and is still zero, so folding it in is harmless
      final int value = fingerprint(hash) ^ readFingerprint(h0) ^ readFingerprint(h1) ^ readFingerprint(h2);
      writeFingerprint(index, value);
    }
  }

  // HASHING HELPERS

  // MurmurHash3 64-bit finalizer applied to the key combined with the construction seed
  private static long mix(final long key, final long seed) {
    long h = key + seed;
    h ^= (h >>> 33);
    h *= MURMUR_C1;
    h ^= (h >>> 33);
    h *= MURMUR_C2;
    h ^= (h >>> 33);
    return h;
  }

  // splitmix64 scrambler used to derive a candidate construction seed from the running state
  private static long splitMix64(final long state) {
    long z = state;
    z = (z ^ (z >>> 30)) * SPLITMIX_MUL1;
    z = (z ^ (z >>> 27)) * SPLITMIX_MUL2;
    return z ^ (z >>> 31);
  }

  // Lemire's multiply-shift map of a 32-bit hash word into [0, n) without a division
  private static int reduce(final int hash, final int n) {
    return (int) (((hash & 0xffffffffL) * n) >>> 32);
  }

  // Folds the 64-bit hash into the configured number of fingerprint bits
  private int fingerprint(final long hash) {
    final int fold = (int) (hash ^ (hash >>> 32));
    return fold & ((1 << bitsPerFingerprint_) - 1);
  }

  // Reads a fingerprint slot from the payload, honoring the configured width
  private int readFingerprint(final int index) {
    if (bitsPerFingerprint_ == 8) {
      return fpSeg_.get(JAVA_BYTE, index) & 0xff;
    }
    return fpSeg_.get(JAVA_SHORT_UNALIGNED, (long) index << 1) & 0xffff;
  }

  // Writes a fingerprint slot into the payload, honoring the configured width
  private void writeFingerprint(final int index, final int value) {
    if (bitsPerFingerprint_ == 8) {
      fpSeg_.set(JAVA_BYTE, index, (byte) value);
    } else {
      fpSeg_.set(JAVA_SHORT_UNALIGNED, (long) index << 1, (short) value);
    }
  }

  // SERIALIZATION

  /**
   * Returns the length of this XorFilter when serialized, in bytes.
   * @return The length of this XorFilter when serialized, in bytes
   */
  public long getSerializedSizeBytes() {
    return PREAMBLE_SIZE_BYTES + fpSeg_.byteSize();
  }

/*
 * An Xor Filter's serialized image always uses 3 longs of preamble, as the filter is
 * immutable and therefore has no empty or growable state:
 *
 * <pre>
 * Long || Start Byte Adr:
 * Adr:
 *      ||       0        |    1   |    2   |    3   |    4   |    5   |    6   |    7   |
 *  0   || Preamble_Longs | SerVer | FamID  |  Flags | BitsPFP|NumHash |-----Unused------|
 *
 *      ||       8        |    9   |   10   |   11   |   12   |   13   |   14   |   15   |
 *  1   ||-------------------------------Construction Seed-------------------------------|
 *
 *      ||      16        |   17   |   18   |   19   |   20   |   21   |   22   |   23   |
 *  2   ||----------Segment Length (int)-------------|-----------Num Keys (int)----------|
 *  </pre>
 *
 * The raw fingerprint array starts at byte 24.
 */

  /**
   * Serializes the current XorFilter to an array of bytes.
   *
   * <p>Note: Method throws if the serialized size exceeds <code>Integer.MAX_VALUE</code>.</p>
   * @return A serialized image of the current XorFilter as byte[]
   */
  public byte[] toByteArray() {
    final long sizeBytes = getSerializedSizeBytes();
    if (sizeBytes > Integer.MAX_VALUE) {
      throw new SketchesStateException("Cannot serialize a XorFilter of this size using toByteArray(); use toLongArray() instead.");
    }

    final byte[] bytes = new byte[(int) sizeBytes];
    final MemorySegment seg = MemorySegment.ofArray(bytes);
    writeToSegment(seg);
    return bytes;
  }

  /**
   * Serializes the current XorFilter to an array of longs. Unlike {@link #toByteArray()},
   * this method can handle any size filter.
   *
   * @return A serialized image of the current XorFilter as long[]
   */
  public long[] toLongArray() {
    final long sizeBytes = getSerializedSizeBytes();
    final int numLongs = (int) ((sizeBytes + Long.BYTES - 1) >>> 3);
    final long[] longs = new long[numLongs];
    writeToSegment(MemorySegment.ofArray(longs));
    return longs;
  }

  // Writes the preamble followed by the fingerprint payload into the provided segment
  private void writeToSegment(final MemorySegment seg) {
    final PositionalSegment posSeg = PositionalSegment.wrap(seg);
    posSeg.setByte((byte) PREAMBLE_LONGS);
    posSeg.setByte((byte) SER_VER);
    posSeg.setByte((byte) Family.XORFILTER.getID());
    posSeg.setByte((byte) 0); // flags
    posSeg.setByte((byte) bitsPerFingerprint_);
    posSeg.setByte((byte) NUM_HASHES);
    posSeg.setShort((short) 0); // unused
    posSeg.setLong(seed_);
    posSeg.setInt(segmentLength_);
    posSeg.setInt(numKeys_);
    MemorySegment.copy(fpSeg_, JAVA_BYTE, 0, seg, JAVA_BYTE, PREAMBLE_SIZE_BYTES, fpSeg_.byteSize());
  }

  // Throws an exception with the provided message if the given condition is true
  private static void checkArgument(final boolean condition, final String message) {
    if (condition) { throw new SketchesArgumentException(message); }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();

    sb.append(LS);
    final String thisSimpleName = this.getClass().getSimpleName();
    sb.append("### ").append(thisSimpleName).append(" SUMMARY: ").append(LS);
    sb.append("   numItems     : ").append(numKeys_).append(LS);
    sb.append("   numHashes    : ").append(NUM_HASHES).append(LS);
    sb.append("   bitsPerFP    : ").append(bitsPerFingerprint_).append(LS);
    sb.append("   capacity     : ").append(getCapacity()).append(LS);
    sb.append("   seed         : ").append(seed_).append(LS);
    sb.append("   bits/item    : ").append(getBitsPerItem()).append(LS);
    sb.append("### END SKETCH SUMMARY").append(LS);

    return sb.toString();
  }
}

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

package org.apache.datasketches.req;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.round;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.positional.PositionalSegment;

/**
 * This class handles serialization and deserialization.
 *
 * <p>ReqSketch SERIALIZATION FORMAT.</p>
 *
 * <p>Low significance bytes of this data structure are on the right just for visualization.
   * The multi-byte primitives are stored in native byte order.
   * The <i>byte</i> primitives are treated as unsigned. Multibyte primitives are indicated with "*" and
   * their size depends on the specific implementation.</p>
   *
   * <p>The ESTIMATION binary format for an estimating sketch with &gt; one item: </p>
   *
   * <pre>
   * Normal Binary Format:
   * PreInts=4
   * Empty=false
   * RawItems=false
   * # Constructors > 1, C0 to Cm, whatever is required
   *
   * Long Adr / Byte Offset
   *      ||    7   |    6   |    5   |    4   |    3   |    2   |    1   |     0              |
   *  0   || (empty)| #Ctors |        K        | Flags  |FamID=17| SerVer |     PreInts = 4    |
   *
   *      ||   15   |   14   |   13   |   12   |   11   |   10   |    9   |     8              |
   *  1   ||-----------------------------------N-----------------------------------------------|
   *
   *      ||        |        |        |        |        |        |        |    16              |
   *      ||--------------MaxItem*----------------------|--------------MinItem*----------------|
   *
   *      ||        |        |        |        |        |        |        |                    |
   *      ||----------------C1*-------------------------|----------------C0*-------------------|
   * </pre>
   *
   * <p>An EXACT-binary format sketch has only one serialized compactor: </p>
   *
   * <pre>
   * PreInts=2
   * Empty=false
   * RawItems=false
   * # Constructors=C0=1
   *
   * Long Adr / Byte Offset
   *      ||    7   |    6   |    5   |    4   |    3   |    2   |    1   |     0              |
   *  0   || (empty)|    1   |        K        | Flags  |FamID=17| SerVer |     PreInts = 2    |
   *
   *      ||        |        |        |        |        |        |        |     8              |
   *  1   ||                                   |-------------------------C0*-------------------|
   * </pre>
   *
   * <p>A RAW ITEMS binary format sketch has only a few items: </p>
   *
   * <pre>
   * PreInts=2
   * Empty=false
   * RawItems=true
   * # Constructors=C0=1
   *
   * Long Adr / Byte Offset
   *      ||    7   |    6   |    5   |    4   |    3   |    2   |    1   |     0              |
   *  0   || #Raw   |    1   |        K        | Flags  |FamID=17| SerVer |     PreInts = 2    |
   *
   *      ||        |        |        |        |        |        |        |     8              |
   *  1   ||                                   |------------------------ITEM*-----------------|
   * </pre>
   *
   * <p>An EMPTY binary format sketch has only 8 bytes including a reserved empty byte:
   *
   * <pre>
   * PreInts=2
   * Empty=true
   * RawItems=false
   * # Constructors==C0=1
   *
   * Long Adr / Byte Offset
   *      ||    7   |    6   |    5   |    4   |    3   |    2   |    1   |     0              |
   *  0   || (empty)|    0   |        K        | Flags  |FamID=17| SerVer |     PreInts = 2    |
   * </pre>
   * <pre>
   *
   * <b>Flags:</b>
   * Bit 0 : Endianness, reserved
   * Bit 1 : ReadOnly, reserved
   * Bit 2 : Empty
   * Bit 3 : HRA
   * Bit 4 : Raw Items
   * Bit 5 : L0 Sorted
   * Bit 6 : reserved
   * Bit 7 : reserved
   * </pre>
 *
 * @author Lee Rhodes
 */
class ReqSerDe {
  enum SerDeFormat { EMPTY, RAWITEMS, EXACT, ESTIMATION }

  private static final byte SER_VER = 1;
  private static final byte FAMILY_ID = (byte) Family.REQ.getID();

  static ReqSketch heapify(final MemorySegment seg) {
    final PositionalSegment posSeg = PositionalSegment.wrap(seg);
    //Extract first 8 bytes
    final byte preInts = posSeg.getByte();
    final byte serVer = posSeg.getByte();
    assert serVer == (byte)1;
    final byte familyId = posSeg.getByte();
    assert familyId == 17;
    //  Extract flags
    final int flags = posSeg.getByte() & 0xFF;
    final boolean empty = (flags & 4) > 0;
    final boolean hra = (flags & 8) > 0;
    final boolean rawItems = (flags & 16) > 0;
    final boolean lvl0Sorted = (flags & 32) > 0;
    //  remainder fields
    final int k = posSeg.getShort() & 0xFFFF;
    final int numCompactors = posSeg.getByte() & 0xFF;
    final int numRawItems = posSeg.getByte() & 0xFF;
    //  extract different serialization formats
    final SerDeFormat deserFormat = getDeserFormat(empty, rawItems, numCompactors);
    switch (deserFormat) {
      case EMPTY: {
        assert preInts == 2;
        return new ReqSketch(k, hra, null);
      }
      case RAWITEMS: {
        assert preInts == 2;
        final ReqSketch sk = new ReqSketch(k, hra, null);
        for (int i = 0; i < numRawItems; i++) { sk.update(posSeg.getFloat()); }
        return sk;
      }
      case EXACT: {
        assert preInts == 2;
        final Compactor compactor = extractCompactor(posSeg, lvl0Sorted, hra);
        //Construct sketch
        final long totalN = compactor.count;
        final float minItem = compactor.minItem;
        final float maxItem = compactor.maxItem;
        final List<ReqCompactor> compactors = new ArrayList<>();
        compactors.add(compactor.reqCompactor);
        final ReqSketch sk = new ReqSketch(k, hra, totalN, minItem, maxItem, compactors);
        sk.setMaxNomSize(sk.computeMaxNomSize());
        sk.setRetainedItems(sk.computeTotalRetainedItems());
        return sk;
      }
      default: { //ESTIMATION
        assert preInts == 4;
        final long totalN = posSeg.getLong();
        final float minItem = posSeg.getFloat();
        final float maxItem = posSeg.getFloat();

        final List<ReqCompactor> compactors = new ArrayList<>();
        for (int i = 0; i < numCompactors; i++) {
          final boolean level0sorted = i == 0 ? lvl0Sorted : true;
          final Compactor compactor = extractCompactor(posSeg, level0sorted, hra);
          compactors.add(compactor.reqCompactor);
        }
        final ReqSketch sk = new ReqSketch(k, hra, totalN, minItem, maxItem, compactors);
        sk.setMaxNomSize(sk.computeMaxNomSize());
        sk.setRetainedItems(sk.computeTotalRetainedItems());
        return sk;
      }
    }
  }

  static final Compactor extractCompactor(final PositionalSegment posSeg, final boolean lvl0Sorted,
      final boolean hra) {
    final long state = posSeg.getLong();
    final float sectionSizeFlt = posSeg.getFloat();
    final int sectionSize = round(sectionSizeFlt);
    final byte lgWt = posSeg.getByte();
    final byte numSections = posSeg.getByte();
    posSeg.incrementPosition(2);
    final int count = posSeg.getInt();
    final float[] arr = new float[count];
    posSeg.getFloatArray(arr, 0, count);
    float minItem = Float.MAX_VALUE;
    float maxItem = Float.MIN_VALUE;
    for (int i = 0; i < count; i++) {
      minItem = min(minItem, arr[i]);
      maxItem = max(maxItem, arr[i]);
    }
    final int delta = 2 * sectionSize * numSections;
    final int nomCap = 2 * delta;
    final int cap = max(count, nomCap);
    final FloatBuffer fltBuf = FloatBuffer.reconstruct(arr, count, cap, delta, lvl0Sorted, hra);
    final ReqCompactor reqCompactor =
        new ReqCompactor(lgWt, hra, state, sectionSizeFlt, numSections, fltBuf);
    return new Compactor(reqCompactor, minItem, maxItem, count);
  }

  static class Compactor {
    ReqCompactor reqCompactor;
    float minItem;
    float maxItem;
    int count;

    Compactor(final ReqCompactor reqCompactor, final float minItem, final float maxItem,
        final int count) {
      this.reqCompactor = reqCompactor;
      this.minItem = minItem;
      this.maxItem = maxItem;
      this.count = count;
    }
  }

  private static byte getFlags(final ReqSketch sk) {
    final boolean rawItems = sk.getN() <= ReqSketch.MIN_K;
    final boolean level0Sorted = sk.getCompactors().get(0).getBuffer().isSorted();
    final int flags = (sk.isEmpty() ? 4 : 0)
        | (sk.getHighRankAccuracyMode() ? 8 : 0)
        | (rawItems ? 16 : 0)
        | (level0Sorted ? 32 : 0);
    return (byte) flags;
  }

  static SerDeFormat getSerFormat(final ReqSketch sk) {
    if (sk.isEmpty()) { return SerDeFormat.EMPTY; }
    if (sk.getN() <= ReqSketch.MIN_K) { return SerDeFormat.RAWITEMS; }
    if (sk.getNumLevels() == 1) { return SerDeFormat.EXACT; }
    return SerDeFormat.ESTIMATION;
  }

  private static SerDeFormat getDeserFormat(final boolean empty, final boolean rawItems,
      final int numCompactors) {
    if (numCompactors <= 1) {
      if (empty) { return SerDeFormat.EMPTY; }
      if (rawItems) { return SerDeFormat.RAWITEMS; }
      return SerDeFormat.EXACT;
    }
    return SerDeFormat.ESTIMATION;
  }

  static byte[] toByteArray(final ReqSketch sk) {
    final SerDeFormat serDeFormat = getSerFormat(sk);
    final int bytes = getSerBytes(sk, serDeFormat);
    final byte[] arr = new byte[bytes];
    final PositionalSegment posSeg = PositionalSegment.wrap(MemorySegment.ofArray(arr));

    final byte preInts = (byte)(serDeFormat == SerDeFormat.ESTIMATION ? 4 : 2);
    final byte flags = getFlags(sk);
    final byte numCompactors = sk.isEmpty() ? 0 : (byte) sk.getNumLevels();
    final byte numRawItems = sk.getN() <= 4 ? (byte) sk.getN() : 0;
    posSeg.setByte(preInts);
    posSeg.setByte(SER_VER);
    posSeg.setByte(FAMILY_ID);
    posSeg.setByte(flags);
    posSeg.setShort((short)sk.getK());
    posSeg.setByte(numCompactors);
    posSeg.setByte(numRawItems);

    switch (serDeFormat) {
      case EMPTY: {
        assert posSeg.getPosition() == bytes;
        return arr;
      }
      case RAWITEMS: {
        final ReqCompactor c0 = sk.getCompactors().get(0);
        final FloatBuffer fbuf = c0.getBuffer();
        for (int i = 0; i < numRawItems; i++) { posSeg.setFloat(fbuf.getItem(i)); }
        assert posSeg.getPosition() == bytes;
        return arr;
      }
      case EXACT: {
        final ReqCompactor c0 = sk.getCompactors().get(0);
        posSeg.setByteArray(c0.toByteArray(), 0, c0.getSerializationBytes());
        assert posSeg.getPosition() == bytes;
        return arr;
      }
      default: { //Normal Estimation
        posSeg.setLong(sk.getN());
        posSeg.setFloat(sk.getMinItem());
        posSeg.setFloat(sk.getMaxItem());
        for (int i = 0; i < numCompactors; i++) {
          final ReqCompactor c = sk.getCompactors().get(i);
          posSeg.setByteArray(c.toByteArray(), 0, c.getSerializationBytes());
        }
        assert posSeg.getPosition() == bytes : posSeg.getPosition() + ", " + bytes;
        return arr;
      }
    }
  }

  static int getSerBytes(final ReqSketch sk, final SerDeFormat serDeFormat) {
    switch (serDeFormat) {
      case EMPTY: {
        return 8;
      }
      case RAWITEMS: {
        return (sk.getCompactors().get(0).getBuffer().getCount() * Float.BYTES) + 8;
      }
      case EXACT: {
        return sk.getCompactors().get(0).getSerializationBytes() + 8;
      }
      default: { //ESTIMATION
       int cBytes = 0;
       for (int i = 0; i < sk.getNumLevels(); i++) {
         cBytes += sk.getCompactors().get(i).getSerializationBytes();
        }
        return cBytes + 24;
      }
    }
  }

}

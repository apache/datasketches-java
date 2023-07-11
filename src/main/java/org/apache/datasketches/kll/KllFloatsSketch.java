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

package org.apache.datasketches.kll;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static org.apache.datasketches.common.ByteArrayUtil.putFloatLE;
import static org.apache.datasketches.common.Family.KLL;
import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR;
import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR_SINGLE_ITEM;
import static org.apache.datasketches.kll.KllPreambleUtil.EMPTY_BIT_MASK;
import static org.apache.datasketches.kll.KllPreambleUtil.LEVEL_ZERO_SORTED_BIT_MASK;
import static org.apache.datasketches.kll.KllPreambleUtil.PREAMBLE_INTS_EMPTY_SINGLE;
import static org.apache.datasketches.kll.KllPreambleUtil.PREAMBLE_INTS_FULL;
import static org.apache.datasketches.kll.KllPreambleUtil.SERIAL_VERSION_EMPTY_FULL;
import static org.apache.datasketches.kll.KllPreambleUtil.SERIAL_VERSION_SINGLE;
import static org.apache.datasketches.kll.KllPreambleUtil.SERIAL_VERSION_UPDATABLE;
import static org.apache.datasketches.kll.KllPreambleUtil.getMemorySerVer;
import static org.apache.datasketches.kll.KllSketch.Error.TGT_IS_READ_ONLY;
import static org.apache.datasketches.kll.KllSketch.Error.kllSketchThrow;
import static org.apache.datasketches.kll.KllSketch.SketchType.FLOATS_SKETCH;
import static org.apache.datasketches.quantilescommon.QuantilesUtil.THROWS_EMPTY;
import static org.apache.datasketches.quantilescommon.QuantilesUtil.equallyWeightedRanks;

import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Objects;

import org.apache.datasketches.common.SuppressFBWarnings;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.MemoryRequestServer;
import org.apache.datasketches.memory.WritableBuffer;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.quantilescommon.FloatsSortedView;
import org.apache.datasketches.quantilescommon.QuantileSearchCriteria;
import org.apache.datasketches.quantilescommon.QuantilesFloatsAPI;
import org.apache.datasketches.quantilescommon.QuantilesFloatsSketchIterator;

/**
 * This variation of the KllSketch implements primitive floats.
 *
 * @see org.apache.datasketches.kll.KllSketch
 */
public abstract class KllFloatsSketch extends KllSketch implements QuantilesFloatsAPI {
  private KllFloatsSketchSortedView kllFloatsSV = null;
  final static int ITEM_BYTES = Float.BYTES;

  KllFloatsSketch(final WritableMemory wmem, final MemoryRequestServer memReqSvr) {
    super(SketchType.FLOATS_SKETCH, wmem, memReqSvr);
  }

  /**
   * Factory heapify takes a compact sketch image in Memory and instantiates an on-heap sketch.
   * The resulting sketch will not retain any link to the source Memory.
   * @param srcMem a compact Memory image of a sketch serialized by this sketch.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @return a heap-based sketch based on the given Memory.
   */
  public static KllFloatsSketch heapify(final Memory srcMem) {
    Objects.requireNonNull(srcMem, "Parameter 'srcMem' must not be null");
    return KllHeapFloatsSketch.heapifyImpl(srcMem);
  }

  /**
   * Create a new direct instance of this sketch with the default <em>k</em>.
   * The default <em>k</em> = 200 results in a normalized rank error of about
   * 1.65%. Larger <em>k</em> will have smaller error but the sketch will be larger (and slower).
   * @param dstMem the given destination WritableMemory object for use by the sketch
   * @param memReqSvr the given MemoryRequestServer to request a larger WritableMemory
   * @return a new direct instance of this sketch
   */
  public static KllFloatsSketch newDirectInstance(
      final WritableMemory dstMem,
      final MemoryRequestServer memReqSvr) {
    return newDirectInstance(DEFAULT_K, dstMem, memReqSvr);
  }

  /**
   * Create a new direct instance of this sketch with a given <em>k</em>.
   * @param k parameter that controls size of the sketch and accuracy of estimates.
   * @param dstMem the given destination WritableMemory object for use by the sketch
   * @param memReqSvr the given MemoryRequestServer to request a larger WritableMemory
   * @return a new direct instance of this sketch
   */
  public static KllFloatsSketch newDirectInstance(
      final int k,
      final WritableMemory dstMem,
      final MemoryRequestServer memReqSvr) {
    Objects.requireNonNull(dstMem, "Parameter 'dstMem' must not be null");
    Objects.requireNonNull(memReqSvr, "Parameter 'memReqSvr' must not be null");
    return KllDirectFloatsSketch.newDirectInstance(k, DEFAULT_M, dstMem, memReqSvr);
  }

  /**
   * Create a new heap instance of this sketch with the default <em>k = 200</em>.
   * The default <em>k</em> = 200 results in a normalized rank error of about
   * 1.65%. Larger K will have smaller error but the sketch will be larger (and slower).
   * @return new KllFloatsSketch on the Java heap.
   */
  public static KllFloatsSketch newHeapInstance() {
    return newHeapInstance(DEFAULT_K);
  }

  /**
   * Create a new heap instance of this sketch with a given parameter <em>k</em>.
   * <em>k</em> can be between 8, inclusive, and 65535, inclusive.
   * The default <em>k</em> = 200 results in a normalized rank error of about
   * 1.65%. Larger K will have smaller error but the sketch will be larger (and slower).
   * @param k parameter that controls size of the sketch and accuracy of estimates.
   * @return new KllFloatsSketch on the Java heap.
   */
  public static KllFloatsSketch newHeapInstance(final int k) {
    return new KllHeapFloatsSketch(k, DEFAULT_M);
  }

  /**
   * Wrap a sketch around the given read only compact source Memory containing sketch data
   * that originated from this sketch.
   * @param srcMem the read only source Memory
   * @return instance of this sketch
   */
  public static KllFloatsSketch wrap(final Memory srcMem) {
    Objects.requireNonNull(srcMem, "Parameter 'srcMem' must not be null");
    final KllMemoryValidate memVal = new KllMemoryValidate(srcMem, FLOATS_SKETCH, null);
    if (getMemorySerVer(srcMem) == SERIAL_VERSION_UPDATABLE) {
      return new KllDirectFloatsSketch((WritableMemory) srcMem, null, memVal);
    } else {
      return new KllDirectCompactFloatsSketch(srcMem, memVal);
    }
  }

  /**
   * Wrap a sketch around the given source Writable Memory containing sketch data
   * that originated from this sketch.
   * @param srcMem a WritableMemory that contains data.
   * @param memReqSvr the given MemoryRequestServer to request a larger WritableMemory
   * @return instance of this sketch
   */
  public static KllFloatsSketch writableWrap(
      final WritableMemory srcMem,
      final MemoryRequestServer memReqSvr) {
    Objects.requireNonNull(srcMem, "Parameter 'srcMem' must not be null");
    final KllMemoryValidate memVal = new KllMemoryValidate(srcMem, FLOATS_SKETCH, null);
    if (getMemorySerVer(srcMem) == SERIAL_VERSION_UPDATABLE && !srcMem.isReadOnly()) {
        Objects.requireNonNull(memReqSvr, "Parameter 'memReqSvr' must not be null");
      return new KllDirectFloatsSketch(srcMem, memReqSvr, memVal);
    } else {
      return new KllDirectCompactFloatsSketch(srcMem, memVal);
    }
  }

  /**
   * Returns upper bound on the serialized size of a KllFloatsSketch given the following parameters.
   * @param k parameter that controls size of the sketch and accuracy of estimates
   * @param n stream length
   * @param updatableMemoryFormat true if updatable Memory format, otherwise the standard compact format.
   * @return upper bound on the serialized size of a KllSketch.
   */
  public static int getMaxSerializedSizeBytes(final int k, final long n, final boolean updatableMemoryFormat) {
    return getMaxSerializedSizeBytes(k, n, SketchType.FLOATS_SKETCH, updatableMemoryFormat);
  }

  @Override
  public double[] getCDF(final float[] splitPoints, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new IllegalArgumentException(THROWS_EMPTY); }
    refreshSortedView();
    return kllFloatsSV.getCDF(splitPoints, searchCrit);
  }

  @Override
  public FloatsPartitionBoundaries getPartitionBoundaries(final int numEquallyWeighted,
      final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new IllegalArgumentException(THROWS_EMPTY); }
    final double[] ranks = equallyWeightedRanks(numEquallyWeighted);
    final float[] boundaries = getQuantiles(ranks, searchCrit);
    boundaries[0] = getMinItem();
    boundaries[boundaries.length - 1] = getMaxItem();
    final FloatsPartitionBoundaries fpb = new FloatsPartitionBoundaries();
    fpb.N = this.getN();
    fpb.ranks = ranks;
    fpb.boundaries = boundaries;
    return fpb;
  }

  @Override
  public double[] getPMF(final float[] splitPoints, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new IllegalArgumentException(THROWS_EMPTY); }
    refreshSortedView();
    return kllFloatsSV.getPMF(splitPoints, searchCrit);
  }

  @Override
  public float getQuantile(final double rank, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new IllegalArgumentException(THROWS_EMPTY); }
    refreshSortedView();
    return kllFloatsSV.getQuantile(rank, searchCrit);
  }

  @Override
  public float[] getQuantiles(final double[] ranks, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new IllegalArgumentException(THROWS_EMPTY); }
    refreshSortedView();
    final int len = ranks.length;
    final float[] quantiles = new float[len];
    for (int i = 0; i < len; i++) {
      quantiles[i] = kllFloatsSV.getQuantile(ranks[i], searchCrit);
    }
    return quantiles;
  }

  /**
   * {@inheritDoc}
   * The approximate probability that the true quantile is within the confidence interval
   * specified by the upper and lower quantile bounds for this sketch is 0.99.
   */
  @Override
  public float getQuantileLowerBound(final double rank) {
    return getQuantile(max(0, rank - KllHelper.getNormalizedRankError(getMinK(), false)));
  }

  /**
   * {@inheritDoc}
   * The approximate probability that the true quantile is within the confidence interval
   * specified by the upper and lower quantile bounds for this sketch is 0.99.
   */
  @Override
  public float getQuantileUpperBound(final double rank) {
    return getQuantile(min(1.0, rank + KllHelper.getNormalizedRankError(getMinK(), false)));
  }

  @Override
  public double getRank(final float quantile, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new IllegalArgumentException(THROWS_EMPTY); }
    refreshSortedView();
    return kllFloatsSV.getRank(quantile, searchCrit);
  }

  /**
   * {@inheritDoc}
   * The approximate probability that the true rank is within the confidence interval
   * specified by the upper and lower rank bounds for this sketch is 0.99.
   */
  @Override
  public double getRankLowerBound(final double rank) {
    return max(0.0, rank - KllHelper.getNormalizedRankError(getMinK(), false));
  }

  /**
   * {@inheritDoc}
   * The approximate probability that the true rank is within the confidence interval
   * specified by the upper and lower rank bounds for this sketch is 0.99.
   */
  @Override
  public double getRankUpperBound(final double rank) {
    return min(1.0, rank + KllHelper.getNormalizedRankError(getMinK(), false));
  }

  @Override
  public double[] getRanks(final float[] quantiles, final QuantileSearchCriteria searchCrit) {
    if (isEmpty()) { throw new IllegalArgumentException(THROWS_EMPTY); }
    refreshSortedView();
    final int len = quantiles.length;
    final double[] ranks = new double[len];
    for (int i = 0; i < len; i++) {
      ranks[i] = kllFloatsSV.getRank(quantiles[i], searchCrit);
    }
    return ranks;
  }

  @Override
  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "OK in this case.")
  public FloatsSortedView getSortedView() {
    refreshSortedView();
    return kllFloatsSV;
  }

  @Override
  public QuantilesFloatsSketchIterator iterator() {
    return new KllFloatsSketchIterator(getFloatItemsArray(), getLevelsArray(), getNumLevels());
  }

  @Override
  public final void merge(final KllSketch other) {
    if (readOnly) { kllSketchThrow(TGT_IS_READ_ONLY); }
    final KllFloatsSketch othFltSk = (KllFloatsSketch)other;
    if (othFltSk.isEmpty()) { return; }
    KllFloatsHelper.mergeFloatImpl(this, othFltSk);
    kllFloatsSV = null;
  }

  /**
   * {@inheritDoc}
   * <p>The parameter <i>k</i> will not change.</p>
   */
  @Override
  public final void reset() {
    if (readOnly) { kllSketchThrow(TGT_IS_READ_ONLY); }
    final int k = getK();
    setN(0);
    setMinK(k);
    setNumLevels(1);
    setLevelZeroSorted(false);
    setLevelsArray(new int[] {k, k});
    setMinItem(Float.NaN);
    setMaxItem(Float.NaN);
    setFloatItemsArray(new float[k]);
  }

//  @Override
//  public byte[] toByteArray() {
//    return KllHelper.toCompactByteArrayImpl(this);
//  }

  @Override
  public void update(final float item) {
    if (readOnly) { kllSketchThrow(TGT_IS_READ_ONLY); }
    KllFloatsHelper.updateFloat(this, item);
    kllFloatsSV = null;
  }

  //restricted

  /**
   * @return full size of internal items array including empty space at bottom.
   */
  abstract float[] getFloatItemsArray();

  /**
   * @return items array of retained items.
   */
  abstract float[] getFloatRetainedItemsArray();

  abstract float getFloatSingleItem();

  @Override
  abstract byte[] getMinMaxByteArr();

  @Override
  int getMinMaxSizeBytes() {
    return Float.BYTES * 2;
  }

  @Override
  int getRetainedDataSizeBytes() {
    return getNumRetained() * Float.BYTES;
  }

  @Override
  final byte[] getSingleItemByteArr() {
    final byte[] bytes = new byte[4];
    putFloatLE(bytes, 0, getFloatSingleItem());
    return bytes;
  }

  @Override
  int getSingleItemSizeBytes() {
    return Float.BYTES;
  }

  private final void refreshSortedView() {
    kllFloatsSV = (kllFloatsSV == null) ? new KllFloatsSketchSortedView(this) : kllFloatsSV;
  }

  abstract void setFloatItemsArray(float[] floatItems);

  abstract void setFloatItemsArrayAt(int index, float item);

  abstract void setMaxItem(float item);

  abstract void setMinItem(float item);

  @Override
  public byte[] toByteArray() {
    return toByteArray(false);
  }

  @SuppressWarnings("unused") //debug
  byte[] toByteArray(final boolean updatable) {
    //ints 0,1
    final byte preInts = (getN() <= 1 && !updatable) ? PREAMBLE_INTS_EMPTY_SINGLE  : PREAMBLE_INTS_FULL;
    final byte serVer = updatable
        ? SERIAL_VERSION_UPDATABLE
        : isSingleItem() ? SERIAL_VERSION_SINGLE : SERIAL_VERSION_EMPTY_FULL;
    final byte famId = (byte)(KLL.getID());
    final byte flags = (byte) (isEmpty()
        ? EMPTY_BIT_MASK
        : isLevelZeroSorted() ? LEVEL_ZERO_SORTED_BIT_MASK : 0);
    final short k = (short) getK();
    final byte m = (byte) getM();
    //ints 2,3
    final long n = getN();
    //ints 4
    final short minK = (short) getMinK();
    final byte numLevels = (byte) getNumLevels();
    //end of full preamble
    final int[] myLevelsArr = updatable ? getLevelsArray() : Arrays.copyOf(levelsArr, levelsArr.length - 1);
    final float minItem = isEmpty() ? Float.NaN : getMinItem();
    final float maxItem = isEmpty() ? Float.NaN : getMaxItem();
    printFloatArr(getFloatItemsArray());
    printFloatArr(getFloatRetainedItemsArray());
    final float[] itemsArr = updatable ? getFloatItemsArray() : getFloatRetainedItemsArray();
    final Memory mem = Memory.wrap(this.getFloatItemsArray()); //debug
    //compute total bytes out
    final int totalBytes;
    if (!updatable && isEmpty()) { totalBytes = DATA_START_ADR_SINGLE_ITEM; }
    else if (!updatable && isSingleItem()) { totalBytes = DATA_START_ADR_SINGLE_ITEM + getSingleItemSizeBytes(); }
    else { totalBytes =
        DATA_START_ADR + myLevelsArr.length * Integer.BYTES + 2 * Float.BYTES + itemsArr.length * Float.BYTES; }
    final byte[] bytesOut = new byte[totalBytes];
    final WritableBuffer wbuf = WritableMemory.writableWrap(bytesOut).asWritableBuffer(ByteOrder.LITTLE_ENDIAN);
    //load first 8 bytes
    wbuf.putByte(preInts);
    wbuf.putByte(serVer);
    wbuf.putByte(famId);
    wbuf.putByte(flags);
    wbuf.putShort(k);
    wbuf.putByte(m);
    wbuf.incrementPosition(1);
    if (!updatable && isEmpty()) {
      return bytesOut;
    }
    if (!updatable && isSingleItem()) {
      wbuf.putFloat(wbuf.getPosition(), getFloatSingleItem());
      return bytesOut;
    }
    wbuf.putLong(n);
    wbuf.putShort(minK);
    wbuf.putByte(numLevels);
    wbuf.incrementPosition(1);
    wbuf.putIntArray(myLevelsArr, 0, myLevelsArr.length);
    wbuf.putFloat(minItem);
    wbuf.putFloat(maxItem);
    wbuf.putFloatArray(itemsArr, 0, itemsArr.length);
    return bytesOut;
  }

  void printIntArr(final int[] intArr) {
    println("Int Array");
    for (int i = 0; i < intArr.length; i++) { System.out.println(i + ", " + intArr[i]); }
    println("");

  }

  void printFloatArr(final float[] fltArr) {
    println("Float Array");
    for (int i = 0; i < fltArr.length; i++) { System.out.println(i + ", " + fltArr[i]); }
    println("");
  }

  private final static boolean enablePrinting = true;

  /**
   * @param format the format
   * @param args the args
   */
  static final void printf(final String format, final Object ...args) {
    if (enablePrinting) { System.out.printf(format, args); }
  }

  /**
   * @param o the Object to println
   */
  static final void println(final Object o) {
    if (enablePrinting) { System.out.println(o.toString()); }
  }

}

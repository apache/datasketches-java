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

package org.apache.datasketches.hll;

import static org.apache.datasketches.hll.HllUtil.EMPTY;
import static org.apache.datasketches.hll.TgtHllType.HLL_8;

import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

/**
 * This performs union operations for HLL sketches. This union operator is configured with a
 * <i>lgMaxK</i> instead of the normal <i>lgConfigK</i>.
 *
 * <p>This union operator does permit the unioning of sketches with different values of
 * <i>lgConfigK</i>.  The user should be aware that the resulting accuracy of a sketch returned
 * at the end of the unioning process will be a function of the smallest of <i>lgMaxK</i> and
 * <i>lgConfigK</i> that the union operator has seen.
 *
 * <p>This union operator also permits unioning of any of the three different target HllSketch
 * types.
 *
 * <p>Although the API for this union operator parallels many of the methods of the
 * <i>HllSketch</i>, the behavior of the union operator has some fundamental differences.
 *
 * <p>First, the user cannot specify the {@link TgtHllType} as an input parameter.
 * Instead, it is specified for the sketch returned with {@link #getResult(TgtHllType)}.
 *
 * <p>Second, the internal effective value of log-base-2 of <i>K</i> for the union operation can
 * change dynamically based on the smallest <i>lgConfigK</i> that the union operation has seen.
 *
 * @author Lee Rhodes
 * @author Kevin Lang
 */
public class Union extends BaseHllSketch {
  final int lgMaxK;
  private final HllSketch gadget;

  /**
   * Construct this Union operator with the default maximum log-base-2 of <i>K</i>.
   */
  public Union() {
    lgMaxK = HllSketch.DEFAULT_LG_K;
    gadget = new HllSketch(lgMaxK, HLL_8);
  }

  /**
   * Construct this Union operator with a given maximum log-base-2 of <i>K</i>.
   * @param lgMaxK the desired maximum log-base-2 of <i>K</i>.  This value must be
   * between 4 and 21 inclusively.
   */
  public Union(final int lgMaxK) {
    this.lgMaxK = HllUtil.checkLgK(lgMaxK);
    gadget = new HllSketch(lgMaxK, HLL_8);
  }

  /**
   * Construct this Union operator with a given maximum log-base-2 of <i>K</i> and the given
   * WritableMemory as the destination for this Union. This WritableMemory is usually configured
   * for off-heap memory. What remains on the java heap is a thin wrapper object that reads and
   * writes to the given WritableMemory.
   *
   * <p>The given <i>dstMem</i> is checked for the required capacity as determined by
   * {@link HllSketch#getMaxUpdatableSerializationBytes(int, TgtHllType)}.
   * @param lgMaxK the desired maximum log-base-2 of <i>K</i>.  This value must be
   * between 4 and 21 inclusively.
   * @param dstMem the destination memory for the sketch.
   */
  public Union(final int lgMaxK, final WritableMemory dstMem) {
    this.lgMaxK = HllUtil.checkLgK(lgMaxK);
    gadget = new HllSketch(lgMaxK, HLL_8, dstMem);
  }

  Union(final HllSketch sketch) {
    lgMaxK = sketch.getLgConfigK();
    final TgtHllType tgtHllType = sketch.getTgtHllType();
    if (tgtHllType != TgtHllType.HLL_8) {
      throw new SketchesArgumentException("Union can only wrap HLL_8 sketches.");
    }
    gadget = sketch;
  }

  /**
   * Construct a union operator populated with the given byte array image of an HllSketch.
   * @param byteArray the given byte array
   * @return a union operator populated with the given byte array image of an HllSketch.
   */
  public static final Union heapify(final byte[] byteArray) {
    return heapify(Memory.wrap(byteArray));
  }

  /**
   * Construct a union operator populated with the given Memory image of an HllSketch.
   * @param mem the given Memory
   * @return a union operator populated with the given Memory image of an HllSketch.
   */
  public static final Union heapify(final Memory mem) {
    final int lgK = HllUtil.checkLgK(mem.getByte(PreambleUtil.LG_K_BYTE));
    final HllSketch sk = HllSketch.heapify(mem);
    final Union union = new Union(lgK);
    union.update(sk);
    return union;
  }

  /**
   * Wraps the given WritableMemory, which must be a image of a valid updatable HLL_8 sketch,
   * and may have data. What remains on the java heap is a
   * thin wrapper object that reads and writes to the given WritableMemory, which, depending on
   * how the user configures the WritableMemory, may actually reside on the Java heap or off-heap.
   *
   * <p>The given <i>dstMem</i> is checked for the required capacity as determined by
   * {@link HllSketch#getMaxUpdatableSerializationBytes(int, TgtHllType)}, and for the correct type.
   * @param wmem an writable image of a valid sketch with data.
   * @return a Union operator where the sketch data is in the given dstMem.
   */
  public static final Union writableWrap(final WritableMemory wmem) {
    return new Union(HllSketch.writableWrap(wmem));
  }

  @Override
  public double getCompositeEstimate() {
    return gadget.hllSketchImpl.getCompositeEstimate();
  }

  @Override
  CurMode getCurMode() {
    return gadget.getCurMode();
  }

  @Override
  public int getCompactSerializationBytes() {
    return gadget.getCompactSerializationBytes();
  }

  @Override
  public double getEstimate() {
    return gadget.getEstimate();
  }

  /**
   * Gets the effective <i>lgConfigK</i> for the union operator, which may be less than
   * <i>lgMaxK</i>.
   * @return the <i>lgConfigK</i>.
   */
  @Override
  public int getLgConfigK() {
    return gadget.getLgConfigK();
  }

  /**
   * Returns the maximum size in bytes that this union operator can grow to given a lgK.
   *
   * @param lgK The maximum Log2 of K for this union operator. This value must be
   * between 4 and 21 inclusively.
   * @return the maximum size in bytes that this union operator can grow to.
   */
  public static int getMaxSerializationBytes(final int lgK) {
    return HllSketch.getMaxUpdatableSerializationBytes(lgK, TgtHllType.HLL_8);
  }

  @Override
  public double getLowerBound(final int numStdDev) {
    return gadget.getLowerBound(numStdDev);
  }

  /**
   * Return the result of this union operator as an HLL_4 sketch.
   * @return the result of this union operator as an HLL_4 sketch.
   */
  public HllSketch getResult() {
    return gadget.copyAs(HllSketch.DEFAULT_HLL_TYPE);
  }

  /**
   * Return the result of this union operator with the specified {@link TgtHllType}
   * @param tgtHllType the TgtHllType enum
   * @return the result of this union operator with the specified TgtHllType
   */
  public HllSketch getResult(final TgtHllType tgtHllType) {
    return gadget.copyAs(tgtHllType);
  }

  @Override
  public TgtHllType getTgtHllType() {
    return TgtHllType.HLL_8;
  }

  @Override
  public int getUpdatableSerializationBytes() {
    return gadget.getUpdatableSerializationBytes();
  }

  @Override
  public double getUpperBound(final int numStdDev) {
    return gadget.getUpperBound(numStdDev);
  }

  @Override
  public boolean isCompact() {
    return gadget.isCompact();
  }

  @Override
  public boolean isEmpty() {
    return gadget.isEmpty();
  }

  @Override
  public boolean isMemory() {
    return gadget.isMemory();
  }

  @Override
  public boolean isOffHeap() {
    return gadget.isOffHeap();
  }

  @Override
  public boolean isOutOfOrderFlag() { //TODO return to pkg private
    return gadget.isOutOfOrderFlag();
  }

  @Override
  public boolean isSameResource(final Memory mem) {
    return gadget.isSameResource(mem);
  }

  /**
   * Resets to empty and retains the current lgK, but does not change the configured value of
   * lgMaxK.
   */
  @Override
  public void reset() {
    gadget.reset();
  }

  /**
   * Gets the serialization of this union operator as a byte array in compact form, which is
   * designed to be heapified only. It is not directly updatable.
   * For the Union operator, this is the serialization of the internal state of
   * the union operator as a sketch.
   * @return the serialization of this union operator as a byte array.
   */
  @Override
  public byte[] toCompactByteArray() {
    return gadget.toCompactByteArray();
  }

  @Override
  public byte[] toUpdatableByteArray() {
    return gadget.toUpdatableByteArray();
  }

  @Override
  public String toString(final boolean summary, final boolean hllDetail,
      final boolean auxDetail, final boolean all) {
    return gadget.toString(summary, hllDetail, auxDetail, all);
  }

  /**
   * Update this union operator with the given sketch.
   * @param sketch the given sketch.
   */
  public void update(final HllSketch sketch) {
    gadget.hllSketchImpl = unionImpl(sketch, gadget, lgMaxK);
  }

  @Override
  void couponUpdate(final int coupon) {
    if (coupon == EMPTY) { return; }
    gadget.hllSketchImpl = gadget.hllSketchImpl.couponUpdate(coupon);
  }

  // Union operator logic

  /**
   * Union the given source and destination sketches. This static method examines the state of
   * the current internal gadget and the incoming sketch and determines the optimum way to
   * perform the union. This may involve swapping, down-sampling, transforming, and / or
   * copying one of the arguments and may completely replace the internals of the union.
   *
   * <p>A swap in update direction is required:</p>
   * <ul><li>If the Gadget is in LIST, SET or EMPTY mode, AND
   * The source sketch is in HLL mode.
   * The source sketch will need to be copied or downsampled to the heap in HLL_8 mode if it is
   * larger than maxLgK, and then replace the Gadget. If the Gadget was Memory, the resulting
   * heap object will need to be converted to Memory form using the Gadget Memory.</li>
   * </ul>
   *
   * <p>A downsample of gadget is required:</p>
   * <ul><li>If both source and Gadget are in HLL mode AND source LgK <b>less than</b> Gadget LgK.
   * Downsample Gadget, convert back to memory form if required, update gadget from source.</li>
   * </ul>
   *
   * @param source the given incoming sketch, which must not be modified.
   * @param gadget the given gadget sketch, which must have a target of HLL_8 and may be
   * modified.
   * @param lgMaxK the maximum value of log2 K for this union.
   * @return the union of the two sketches in the form of the internal HllSketchImpl, which for
   * the union is always in HLL_8 form.
   */
  private static HllSketchImpl unionImpl(final HllSketch source, final HllSketch gadget,
      final int lgMaxK) {
    assert gadget.getTgtHllType() == HLL_8;
    if ((source == null) || source.isEmpty()) { return gadget.hllSketchImpl; }
    if (source.getCurMode() == CurMode.LIST ) {
      source.mergeTo(gadget);
      gadget.putOutOfOrderFlag(gadget.isOutOfOrderFlag() | source.isOutOfOrderFlag());
      return gadget.hllSketchImpl;
    }
    if (source.getCurMode() == CurMode.SET ) {
      source.mergeTo(gadget);
      gadget.putOutOfOrderFlag(true);
      return gadget.hllSketchImpl;
    }
    //Hereafter, the srcImpl is in HLL mode.
    final int srcLgK = source.getLgConfigK();
    final int gadgetLgK = gadget.getLgConfigK();
    final int bit0 = gadget.isMemory() ? 1 : 0;
    final int bits1_2 = (gadget.isEmpty() ? 3 : gadget.getCurMode().ordinal()) << 1;
    final int bit3 = (srcLgK < gadgetLgK) ? 8 : 0;
    final int bit4 = (srcLgK > lgMaxK) ? 16 : 0;
    final int sw = bit4 | bit3 | bits1_2 | bit0;
    switch (sw) {
      case 0: //src <= max, src >= gdt, gdtLIST, gdtHeap, reverse merge, no downsample, ooof=OR
      case 8: //src <= max, src <  gdt, gdtLIST, gdtHeap, reverse merge, no downsample, ooof=OR
      { final HllSketch srcHll8Heap = source.copyAs(HLL_8);       //copy src to Heap
        gadget.mergeTo(srcHll8Heap);                              //merge gdt -> srcCopy
        final HllSketch gdtTmp = srcHll8Heap;                     //replace gdt
        gdtTmp.putOutOfOrderFlag(gdtTmp.isOutOfOrderFlag() | source.isOutOfOrderFlag());
        return gdtTmp.hllSketchImpl;
      }
      case 1: //src <= max, src >= gdt, gdtLIST, gdtDirect, reverse merge, no downsample, ooof=OR
      case 9: //src <= max, src <  gdt, gdtLIST, gdtDirect, reverse merge, no downsample, ooof=OR
      { final HllSketch srcHll8Heap = source.copyAs(HLL_8);       //copy src to Heap
        gadget.mergeTo(srcHll8Heap);                              //merge gdt -> srcCopy
        final WritableMemory wmem = gadget.getWritableMemory();   //use the gdt wmem
        final byte[] byteArr = srcHll8Heap.toUpdatableByteArray();//serialize srcCopy
        wmem.putByteArray(0, byteArr, 0, byteArr.length);         //replace old data with new
        final HllSketch gdtTmp = HllSketch.writableWrap(wmem);    //wrap & replace gdt
        gdtTmp.putOutOfOrderFlag(gdtTmp.isOutOfOrderFlag() | source.isOutOfOrderFlag());
        return gdtTmp.hllSketchImpl;
      }
      case 2:  //src <= max, src >= gdt, gdtSET, gdtHeap, reverse merge, no downsample, ooof=True
      case 10: //src <= max, src <  gdt, gdtSET, gdtHeap, reverse merge, no downsample, ooof=True
      { final HllSketch srcHll8Heap = source.copyAs(HLL_8);       //copy src to Heap
        gadget.mergeTo(srcHll8Heap);                              //merge gdt -> srcCopy
        final HllSketch gdtTmp = srcHll8Heap;                     //replace gdt
        gdtTmp.putOutOfOrderFlag(true);
        return gdtTmp.hllSketchImpl;
      }
      case 3:  //src <= max, src >= gdt, gdtSET, gdtDirect, reverse merge, no downsample, ooof=True
      case 11: //src <= max, src <  gdt, gdtSET, gdtDirect, reverse merge, no downsample, ooof=True
      { final HllSketch srcHll8Heap = source.copyAs(HLL_8);       //copy src to Heap
        gadget.mergeTo(srcHll8Heap);                              //merge gdt -> srcCopy
        final WritableMemory wmem = gadget.getWritableMemory();   //use the gdt wmem
        final byte[] byteArr = srcHll8Heap.toUpdatableByteArray();//serialize srcCopy
        wmem.putByteArray(0, byteArr, 0, byteArr.length);         //replace old data with new
        final HllSketch gdtTmp = HllSketch.writableWrap(wmem);    //wrap & replace gdt
        gdtTmp.putOutOfOrderFlag(true);
        return gdtTmp.hllSketchImpl;
      }
      case 4:  //src <= max, src >= gdt, gdtHLL, gdtHeap,   forward merge, no downsample, ooof=True
      case 5:  //src <= max, src >= gdt, gdtHLL, gdtDirect, forward merge, no downsample, ooof=True
      case 20: //src >  max, src >= gdt, gdtHLL, gdtHeap,   forward merge, no downsample, ooof=True
      case 21: //src >  max, src >= gdt, gdtHLL, gdtDirect, forward merge, no downsample, ooof=True
      { //Possible Optimization if srcLgK = gdtLgK && src is HLL_8
        source.mergeTo(gadget);                                   //merge src -> gdt
        gadget.putOutOfOrderFlag(true);
        return gadget.hllSketchImpl;
      }
      case 6:  //src <= max, src >= gdt, gdtEmpty, gdtHeap, replace, no downsample, ooof=Src
      case 14: //src <= max, src <  gdt, gdtEmpty, gdtHeap, replace, no downsample, ooof=Src
      { final HllSketch srcHll8Heap = source.copyAs(HLL_8);       //copy src to Heap
        //ooof is already what source is.
        return srcHll8Heap.hllSketchImpl;
      }
      case 7:  //src <= max, src >= gdt, gdtEmpty, gdtDirect, replace mem, no downsample, ooof=Src
      case 15: //src <= max, src <  gdt, gdtEmpty, gdtDirect, replace mem, no downsample, ooof=Src
      { final HllSketch srcHll8Heap = source.copyAs(HLL_8);       //copy src to Heap
        final WritableMemory wmem = gadget.getWritableMemory();   //use the gdt wmem
        final byte[] byteArr = srcHll8Heap.toUpdatableByteArray();//serialize srcCopy
        wmem.putByteArray(0, byteArr, 0, byteArr.length);         //replace old data with new
        final HllSketch gdtTmp = HllSketch.writableWrap(wmem);    //wrap & replace gdt
        //ooof is already what source is.
        return gdtTmp.hllSketchImpl;
      }
      case 12: //src <= max, src <  gdt, gdtHLL, gdtHeap, fwd Merge/replace, downsample Gdt, ooof=True
      {
        final HllSketch gdtHll8Heap = downsample(gadget, srcLgK); //downsample gdt to srcLgK
        source.mergeTo(gdtHll8Heap);                              //merge src -> gdtCopy
        gdtHll8Heap.putOutOfOrderFlag(true);
        return gdtHll8Heap.hllSketchImpl;
      }
      case 13: //src <= max, src < gdt, gdtHLL, gdtDirect, fwd merge/repl mem, downsample Gdt, ooof=True
      {
        final HllSketch gdtHll8Heap = downsample(gadget, srcLgK); //downsample gdt to srcLgK
        source.mergeTo(gdtHll8Heap);                              //merge src -> gdtCopy
        final WritableMemory wmem = gadget.getWritableMemory();   //use the gdt wmem
        final byte[] byteArr = gdtHll8Heap.toUpdatableByteArray();//serialize gdtCopy
        wmem.putByteArray(0, byteArr, 0, byteArr.length);         //replace old data with new
        final HllSketch gdtTmp = HllSketch.writableWrap(wmem);    //wrap & replace gdt
        gdtTmp.putOutOfOrderFlag(true);
        return gdtTmp.hllSketchImpl;
      }
      case 16: //src >  max, src >= gdt, gdtList,  gdtHeap, reverse merge, downsample Src, ooof=OR
      {
        final HllSketch srcHll8Heap = downsample(source, lgMaxK); //downsample src to Max
        gadget.mergeTo(srcHll8Heap);                              //merge gdt -> srcCopy
        final HllSketch gdtTmp = srcHll8Heap;                                     //replace gdt
        gdtTmp.putOutOfOrderFlag(gdtTmp.isOutOfOrderFlag() | source.isOutOfOrderFlag());
        return gdtTmp.hllSketchImpl;
      }
      case 17: //src > max, src >= gdt, gdtList, gdtDirect, rev merge/repl mem, downsample Src, ooof=OR
      {
        final HllSketch srcHll8Heap = downsample(source, lgMaxK); //downsample src to Max
        gadget.mergeTo(srcHll8Heap);                              //merge gdt -> srcCopy
        final WritableMemory wmem = gadget.getWritableMemory();   //use the gdt wmem
        final byte[] byteArr = srcHll8Heap.toUpdatableByteArray();//serialize srcCopy
        wmem.putByteArray(0, byteArr, 0, byteArr.length);         //replace old data with new
        final HllSketch gdtTmp = HllSketch.writableWrap(wmem);    //wrap & replace gdt
        gdtTmp.putOutOfOrderFlag(gdtTmp.isOutOfOrderFlag() | source.isOutOfOrderFlag());
        return gdtTmp.hllSketchImpl;
      }
      case 18: //src > max, src >= gdt, gdtSet, gdtHeap, rev merge, downsample Src, ooof=True
      {
        final HllSketch srcHll8Heap = downsample(source, lgMaxK); //downsample src to Max
        gadget.mergeTo(srcHll8Heap);                              //merge gdt -> srcCopy
        final HllSketch gdtTmp = srcHll8Heap;                     //replace gdt
        gdtTmp.putOutOfOrderFlag(true);
        return gdtTmp.hllSketchImpl;
      }
      case 19: //src > max, src >= gdt, gdtSet, gdtDirect, rev merge/rep mem, downsample Src, ooof=True
      {
        final HllSketch srcHll8Heap = downsample(source, lgMaxK); //downsample src to Max
        gadget.mergeTo(srcHll8Heap);                              //merge gdt -> srcCopy
        final WritableMemory wmem = gadget.getWritableMemory();   //use the gdt wmem
        final byte[] byteArr = srcHll8Heap.toUpdatableByteArray();//serialize srcCopy
        wmem.putByteArray(0, byteArr, 0, byteArr.length);         //replace old data with new
        final HllSketch gdtTmp = HllSketch.writableWrap(wmem);    //wrap & replace gdt
        gdtTmp.putOutOfOrderFlag(true);
        return gdtTmp.hllSketchImpl;
      }
      case 22: //src >  max, src >= gdt, gdtEmpty, gdtHeap, replace, downsample Src, ooof=Src
      {
        final HllSketch srcHll8Heap = downsample(source, lgMaxK); //downsample src to Max
        return srcHll8Heap.hllSketchImpl;                                     //replace gdt
        //ooof is already what source is.
      }
      case 23: //src >  max, src >= gdt, gdtEmpty, gdtDirect, replace mem, downsample Src, ooof=Src
      {
        final HllSketch srcHll8Heap = downsample(source, lgMaxK); //downsample src to Max
        final WritableMemory wmem = gadget.getWritableMemory();   //use the gdt wmem
        final byte[] byteArr = srcHll8Heap.toUpdatableByteArray();//serialize srcCopy
        wmem.putByteArray(0, byteArr, 0, byteArr.length);         //replace old data with new
        return HllSketch.writableWrap(wmem).hllSketchImpl;        //wrap & replace gdt
        //ooof is already what source is.
      }
      default: return gadget.hllSketchImpl;
    }
  }

  //Used by union operator.  Always copies or downsamples to Heap HLL_8.
  //Caller must ultimately manage oooFlag, as caller has more context.
  /**
   * Copies or downsamples the given candidate HLLmode sketch to tgtLgK, HLL_8, on the heap.
   *
   * @param candidate the HllSketch to downsample, must be in HLL mode.
   * @param tgtLgK the LgK to downsample to.
   * @return the downsampled HllSketch.
   */
  private static final HllSketch downsample(final HllSketch candidate, final int tgtLgK) {
    final AbstractHllArray candArr = (AbstractHllArray) candidate.hllSketchImpl;
    final HllArray tgtHllArr = HllArray.newHeapHll(tgtLgK, TgtHllType.HLL_8);
    final PairIterator candItr = candArr.iterator();
    while (candItr.nextValid()) {
      tgtHllArr.couponUpdate(candItr.getPair());
    }
    //both of these are required for isomorphism
    tgtHllArr.putHipAccum(candArr.getHipAccum());
    tgtHllArr.putOutOfOrderFlag(candidate.isOutOfOrderFlag());
    return new HllSketch(tgtHllArr);
  }

}

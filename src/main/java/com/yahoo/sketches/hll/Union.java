/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.hll.CurMode.HLL;
import static com.yahoo.sketches.hll.HllUtil.EMPTY;
import static com.yahoo.sketches.hll.TgtHllType.HLL_8;
import static java.lang.Math.min;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.SketchesArgumentException;

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
    this.lgMaxK = HllSketch.DEFAULT_LG_K;
    gadget = new HllSketch(lgMaxK, HLL_8);
  }

  /**
   * Construct this Union operator with a given maximum log-base-2 of <i>K</i>.
   * @param lgMaxK the desired maximum log-base-2 of <i>K</i>.  This value must be
   * between 7 and 21 inclusively.
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
   * between 7 and 21 inclusively.
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
  boolean isOutOfOrderFlag() {
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
    gadget.hllSketchImpl = unionImpl(sketch.hllSketchImpl, gadget.hllSketchImpl, lgMaxK);
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
   * @param incomingImpl the given incoming sketch, which may not be modified.
   * @param gadgetImpl the given gadget sketch, which must have a target of HLL_8 and may be
   * modified.
   * @param lgMaxK the maximum value of log2 K for this union.
   * @return the union of the two sketches in the form of the internal HllSketchImpl, which for
   * the union is always in HLL_8 form.
   */
  private static final HllSketchImpl unionImpl(final HllSketchImpl incomingImpl,
      final HllSketchImpl gadgetImpl, final int lgMaxK) {
    assert gadgetImpl.getTgtHllType() == HLL_8;
    HllSketchImpl srcImpl = incomingImpl; //default
    HllSketchImpl dstImpl = gadgetImpl; //default
    if ((incomingImpl == null) || incomingImpl.isEmpty()) { return gadgetImpl; }

    final int hi2bits = (gadgetImpl.isEmpty()) ? 3 : gadgetImpl.getCurMode().ordinal();
    final int lo2bits = incomingImpl.getCurMode().ordinal();

    final int sw = (hi2bits << 2) | lo2bits;

    switch (sw) {
      case 0: { //src: LIST, gadget: LIST
        final PairIterator srcItr = srcImpl.iterator(); //LIST
        while (srcItr.nextValid()) {
          dstImpl = dstImpl.couponUpdate(srcItr.getPair()); //assignment required
        }
        //whichever is True wins:
        dstImpl.putOutOfOrderFlag(dstImpl.isOutOfOrderFlag() | srcImpl.isOutOfOrderFlag());
        break;
      }
      case 1: { //src: SET, gadget: LIST
        //consider a swap here
        final PairIterator srcItr = srcImpl.iterator(); //SET
        while (srcItr.nextValid()) {
          dstImpl = dstImpl.couponUpdate(srcItr.getPair()); //assignment required
        }
        dstImpl.putOutOfOrderFlag(true); //SET oooFlag is always true
        break;
      }
      case 2: { //src: HLL, gadget: LIST
        //swap so that src is gadget-LIST, tgt is HLL
        //use lgMaxK because LIST has effective K of 2^26
        srcImpl = gadgetImpl;
        dstImpl = copyOrDownsampleHll(incomingImpl, lgMaxK);
        final PairIterator srcItr = srcImpl.iterator();
        while (srcItr.nextValid()) {
          dstImpl = dstImpl.couponUpdate(srcItr.getPair()); //assignment required
        }
        //whichever is True wins:
        dstImpl.putOutOfOrderFlag(srcImpl.isOutOfOrderFlag() | dstImpl.isOutOfOrderFlag());
        break;
      }
      case 4: { //src: LIST, gadget: SET
        final PairIterator srcItr = srcImpl.iterator(); //LIST
        while (srcItr.nextValid()) {
          dstImpl = dstImpl.couponUpdate(srcItr.getPair()); //assignment required
        }
        dstImpl.putOutOfOrderFlag(true); //SET oooFlag is always true
        break;
      }
      case 5: { //src: SET, gadget: SET
        final PairIterator srcItr = srcImpl.iterator(); //SET
        while (srcItr.nextValid()) {
          dstImpl = dstImpl.couponUpdate(srcItr.getPair()); //assignment required
        }
        dstImpl.putOutOfOrderFlag(true); //SET oooFlag is always true
        break;
      }
      case 6: { //src: HLL, gadget: SET
        //swap so that src is gadget-SET, tgt is HLL
        //use lgMaxK because LIST has effective K of 2^26
        srcImpl = gadgetImpl;
        dstImpl = copyOrDownsampleHll(incomingImpl, lgMaxK);
        final PairIterator srcItr = srcImpl.iterator(); //LIST
        assert dstImpl.getCurMode() == HLL;
        while (srcItr.nextValid()) {
          dstImpl = dstImpl.couponUpdate(srcItr.getPair()); //assignment required
        }
        dstImpl.putOutOfOrderFlag(true); //merging SET into non-empty HLL -> true
        break;
      }
      case 8: { //src: LIST, gadget: HLL
        assert dstImpl.getCurMode() == HLL;
        final PairIterator srcItr = srcImpl.iterator(); //LIST
        while (srcItr.nextValid()) {
          dstImpl = dstImpl.couponUpdate(srcItr.getPair()); //assignment required
        }
        //whichever is True wins:
        dstImpl.putOutOfOrderFlag(dstImpl.isOutOfOrderFlag() | srcImpl.isOutOfOrderFlag());
        break;
      }
      case 9: { //src: SET, gadget: HLL
        assert dstImpl.getCurMode() == HLL;
        final PairIterator srcItr = srcImpl.iterator(); //SET
        while (srcItr.nextValid()) {
          dstImpl = dstImpl.couponUpdate(srcItr.getPair()); //assignment required
        }
        dstImpl.putOutOfOrderFlag(true); //merging SET into existing HLL -> true
        break;
      }
      case 10: { //src: HLL, gadget: HLL
        final int srcLgK = srcImpl.getLgConfigK();
        final int dstLgK = dstImpl.getLgConfigK();
        if ((srcLgK < dstLgK) || (dstImpl.getTgtHllType() != HLL_8)) {
          dstImpl = copyOrDownsampleHll(dstImpl, min(dstLgK, srcLgK)); //TODO Fix for off-heap
        }
        final PairIterator srcItr = srcImpl.iterator(); //HLL
        while (srcItr.nextValid()) {
          dstImpl = dstImpl.couponUpdate(srcItr.getPair()); //assignment required
        }
        dstImpl.putOutOfOrderFlag(true); //union of two HLL modes is always true
        break;
      }
      case 12: { //src: LIST, gadget: empty
        final PairIterator srcItr = srcImpl.iterator(); //LIST
        while (srcItr.nextValid()) {
          dstImpl = dstImpl.couponUpdate(srcItr.getPair()); //assignment required
        }
        dstImpl.putOutOfOrderFlag(srcImpl.isOutOfOrderFlag()); //whatever source is
        break;
      }
      case 13: { //src: SET, gadget: empty
        final PairIterator srcItr = srcImpl.iterator(); //SET
        while (srcItr.nextValid()) {
          dstImpl = dstImpl.couponUpdate(srcItr.getPair()); //assignment required
        }
        dstImpl.putOutOfOrderFlag(true); //SET oooFlag is always true
        break;
      }
      case 14: { //src: HLL, gadget: empty
        dstImpl = copyOrDownsampleHll(srcImpl, lgMaxK);
        dstImpl.putOutOfOrderFlag(srcImpl.isOutOfOrderFlag()); //whatever source is.
        break;
      }
    }
    if (gadgetImpl.isMemory() && !dstImpl.isMemory()) {
      //dstImpl is on heap, gadget is Memory; we have to put dstImpl back into the gadget
      final WritableMemory gadgetWmem = gadgetImpl.getWritableMemory();
      assert gadgetWmem != null;
      final int bytes =
          HllSketch.getMaxUpdatableSerializationBytes(dstImpl.getLgConfigK(), HLL_8);
      gadgetWmem.clear(0, bytes);
      final byte[] dstByteArr = dstImpl.toUpdatableByteArray();
      gadgetWmem.putByteArray(0, dstByteArr, 0, dstByteArr.length);
      dstImpl = HllSketch.writableWrap(gadgetWmem).hllSketchImpl;
    }
    return dstImpl;
  }

  //Used by union operator.  Always copies or downsamples to Heap HLL_8.
  //Caller must ultimately manage oooFlag, as caller has more info
  private static final HllSketchImpl copyOrDownsampleHll(
      final HllSketchImpl srcImpl, final int tgtLgK) {
    assert srcImpl.getCurMode() == HLL;
    final AbstractHllArray src = (AbstractHllArray) srcImpl;
    final int srcLgK = src.getLgConfigK();
    if ((srcLgK <= tgtLgK) && (src.getTgtHllType() == TgtHllType.HLL_8)) {
      return src.copy();
    }
    final int minLgK = Math.min(srcLgK, tgtLgK);
    final HllArray tgtHllArr = HllArray.newHeapHll(minLgK, TgtHllType.HLL_8);
    final PairIterator srcItr = src.iterator();
    while (srcItr.nextValid()) {
      tgtHllArr.couponUpdate(srcItr.getPair());
    }
    //both of these are required for isomorphism
    tgtHllArr.putHipAccum(src.getHipAccum());
    tgtHllArr.putOutOfOrderFlag(src.isOutOfOrderFlag());
    return tgtHllArr;
  }

}

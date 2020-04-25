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

import static org.apache.datasketches.Util.invPow2;
import static org.apache.datasketches.hll.HllUtil.EMPTY;
import static org.apache.datasketches.hll.PreambleUtil.HLL_BYTE_ARR_START;
import static org.apache.datasketches.hll.PreambleUtil.extractTgtHllType;
import static org.apache.datasketches.hll.TgtHllType.HLL_8;

import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

/**
 * This performs union operations for all HllSketches. This union operator can be configured to be
 * on or off heap.  The source sketch given to this union using the {@link #update(HllSketch)} can
 * be configured with any precision value <i>lgConfigK</i> (from 4 to 21), any <i>TgtHllType</i>
 * (HLL_4, HLL_6, HLL_8), and either on or off-heap; and it can be in either of the sparse modes
 * (<i>LIST</i> or <i>SET</i>), or the dense mode (<i>HLL</i>).
 *
 * <p>Although the API for this union operator parallels many of the methods of the
 * <i>HllSketch</i>, the behavior of the union operator has some fundamental differences.</p>
 *
 * <p>First, this union operator is configured with a <i>lgMaxK</i> instead of the normal
 * <i>lgConfigK</i>.  Generally, this union operator will inherit the lowest <i>lgConfigK</i>
 * less than <i>lgMaxK</i> that it has seen. However, the <i>lgConfigK</i> of incoming sketches that
 * are still in sparse are ignored. The <i>lgMaxK</i> provides the user the ability to specify the
 * largest maximum size for the union operation.
 *
 * <p>Second, the user cannot specify the {@link TgtHllType} as an input parameter to the union.
 * Instead, it is specified for the sketch returned with {@link #getResult(TgtHllType)}.
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
   * @param dstWmem the destination writable memory for the sketch.
   */
  public Union(final int lgMaxK, final WritableMemory dstWmem) {
    this.lgMaxK = HllUtil.checkLgK(lgMaxK);
    gadget = new HllSketch(lgMaxK, HLL_8, dstWmem);
  }

  //used only by writableWrap
  private Union(final HllSketch sketch) {
    lgMaxK = sketch.getLgConfigK();
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
    final HllSketch sk = HllSketch.heapify(mem, false); //allows non-finalized image
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
   * @param srcWmem an writable image of a valid sketch with data.
   * @return a Union operator where the sketch data is in the given dstMem.
   */
  public static final Union writableWrap(final WritableMemory srcWmem) {
    final TgtHllType tgtHllType = extractTgtHllType(srcWmem);
    if (tgtHllType != TgtHllType.HLL_8) {
      throw new SketchesArgumentException(
          "Union can only wrap writable HLL_8 sketches that were the Gadget of a Union.");
    }
    //allows writableWrap of non-finalized image
    return new Union(HllSketch.writableWrap(srcWmem, false));
  }

  @Override
  public double getCompositeEstimate() {
    checkRebuildCurMinNumKxQ(gadget);
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
    checkRebuildCurMinNumKxQ(gadget);
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

  @Override
  public double getLowerBound(final int numStdDev) {
    checkRebuildCurMinNumKxQ(gadget);
    return gadget.getLowerBound(numStdDev);
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

  /**
   * Return the result of this union operator as an HLL_4 sketch.
   * @return the result of this union operator as an HLL_4 sketch.
   */
  public HllSketch getResult() {
    return getResult(HllSketch.DEFAULT_HLL_TYPE);
  }

  /**
   * Return the result of this union operator with the specified {@link TgtHllType}
   * @param tgtHllType the TgtHllType enum
   * @return the result of this union operator with the specified TgtHllType
   */
  public HllSketch getResult(final TgtHllType tgtHllType) {
    checkRebuildCurMinNumKxQ(gadget);
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
    checkRebuildCurMinNumKxQ(gadget);
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
  boolean isOutOfOrder() {
    return gadget.isOutOfOrder();
  }

  @Override
  public boolean isSameResource(final Memory mem) {
    return gadget.isSameResource(mem);
  }

  boolean isRebuildCurMinNumKxQFlag() {
    return gadget.hllSketchImpl.isRebuildCurMinNumKxQFlag();
  }

  void putRebuildCurMinNumKxQFlag(final boolean rebuild) {
    gadget.hllSketchImpl.putRebuildCurMinNumKxQFlag(rebuild);
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
    checkRebuildCurMinNumKxQ(gadget);
    return gadget.toCompactByteArray();
  }

  @Override
  public byte[] toUpdatableByteArray() {
    checkRebuildCurMinNumKxQ(gadget);
    return gadget.toUpdatableByteArray();
  }

  @Override
  public String toString(final boolean summary, final boolean hllDetail,
      final boolean auxDetail, final boolean all) {
    checkRebuildCurMinNumKxQ(gadget);
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
   * perform the union. This may involve swapping the merge order, downsampling, transforming,
   * and / or copying one of the arguments and may completely replace the internals of the union.
   *
   * <p>If the union gadget is empty, the source sketch is effectively copied to the union gadget
   * after any required transformations.
   *
   * <p>The direction of the merge is reversed if the union gadget is in LIST or SET mode, and the
   * source sketch is in HLL mode. This is done to maintain maximum accuracy of the union process.
   *
   * <p>The source sketch is downsampled if the source LgK is larger than maxLgK and in HLL mode.
   *
   * <p>The union gadget is downsampled if both source and union gadget are in HLL mode
   * and the source LgK <b>less than</b> the union gadget LgK.
   *
   * @param source the given incoming sketch, which cannot be modified.
   * @param gadget the given gadget sketch, which has a target of HLL_8 and holds the result.
   * @param lgMaxK the maximum value of log2 K for this union.
   * @return the union of the two sketches in the form of the internal HllSketchImpl, which is
   * always in HLL_8 form.
   */
  private static HllSketchImpl unionImpl(final HllSketch source, final HllSketch gadget,
      final int lgMaxK) {
    assert gadget.getTgtHllType() == HLL_8;
    if ((source == null) || source.isEmpty()) {
      return gadget.hllSketchImpl;
    }

    final CurMode srcMode = source.getCurMode();
    if (srcMode == CurMode.LIST ) {
      source.mergeTo(gadget);
      return gadget.hllSketchImpl;
    }

    final int srcLgK = source.getLgConfigK();
    final int gadgetLgK = gadget.getLgConfigK();
    final boolean srcIsMem = source.isMemory();
    final boolean gdtIsMem = gadget.isMemory();
    final boolean gdtEmpty = gadget.isEmpty();

    if (srcMode == CurMode.SET ) {
      if (gdtEmpty && (srcLgK == gadgetLgK) && (!srcIsMem) && (!gdtIsMem)) {
        gadget.hllSketchImpl = source.copyAs(HLL_8).hllSketchImpl;
        return gadget.hllSketchImpl;
      }
      source.mergeTo(gadget);
      return gadget.hllSketchImpl;
    }

    //Hereafter, the source is in HLL mode.
    final int bit0 = gdtIsMem ? 1 : 0;
    final int bits1_2 = (gdtEmpty ? 3 : gadget.getCurMode().ordinal()) << 1;
    final int bit3 = (srcLgK < gadgetLgK) ? 8 : 0;
    final int bit4 = (srcLgK > lgMaxK) ? 16 : 0;
    final int sw = bit4 | bit3 | bits1_2 | bit0;
    HllSketchImpl hllSketchImpl = null; //never returned as null
    switch (sw) {
      case 0: //src <= max, src >= gdt, gdtLIST, gdtHeap
      case 8: //src <= max, src <  gdt, gdtLIST, gdtHeap
      case 2: //src <= max, src >= gdt, gdtSET,  gdtHeap
      case 10://src <= max, src <  gdt, gdtSET,  gdtHeap
      { //Action: copy src, reverse merge w/autofold, ooof=src
        final HllSketch srcHll8Heap = source.copyAs(HLL_8);
        gadget.mergeTo(srcHll8Heap); //merge gdt(Hll8,heap,list/set) -> src(Hll8,heap,hll)
        hllSketchImpl = srcHll8Heap.hllSketchImpl;
        break;
      }
      case 16://src >  max, src >= gdt, gdtList, gdtHeap
      case 18://src >  max, src >= gdt, gdtSet,  gdtHeap
      { //Action: downsample src to MaxLgK, reverse merge w/autofold, ooof=src
        final HllSketch srcHll8Heap = downsample(source, lgMaxK);
        gadget.mergeTo(srcHll8Heap); //merge gdt(Hll8,heap,list/set) -> src(Hll8,heap,hll)
        hllSketchImpl = srcHll8Heap.hllSketchImpl;
        break;
      }

      case 1: //src <= max, src >= gdt, gdtLIST, gdtMemory
      case 9: //src <= max, src <  gdt, gdtLIST, gdtMemory
      case 3: //src <= max, src >= gdt, gdtSET,  gdtMemory
      case 11://src <= max, src <  gdt, gdtSET,  gdtMemory
      { //Action: copy src, reverse merge w/autofold, use gdt memory, ooof=src
        final HllSketch srcHll8Heap = source.copyAs(HLL_8);
        gadget.mergeTo(srcHll8Heap);  //merge gdt(Hll8,mem,list/set) -> src(Hll8,heap,hll)
        hllSketchImpl = useGadgetMemory(gadget, srcHll8Heap, false).hllSketchImpl;
        break;
      }
      case 17://src >  max, src >= gdt, gdtList, gdtMemory
      case 19://src >  max, src >= gdt, gdtSet,  gdtMemory
      { //Action: downsample src to MaxLgK, reverse merge w/autofold, use gdt memory, ooof=src
        final HllSketch srcHll8Heap = downsample(source, lgMaxK);
        gadget.mergeTo(srcHll8Heap); //merge gdt(Hll8,mem,list/set) -> src(Hll8,heap,hll), autofold
        hllSketchImpl = useGadgetMemory(gadget, srcHll8Heap, false).hllSketchImpl;
        break;
      }

      case 4: //src <= max, src >= gdt, gdtHLL, gdtHeap
      case 20://src >  max, src >= gdt, gdtHLL, gdtHeap
      case 5: //src <= max, src >= gdt, gdtHLL, gdtMemory
      case 21://src >  max, src >= gdt, gdtHLL, gdtMemory
      { //Action: forward HLL merge w/autofold, ooof=True
        //merge src(Hll4,6,8,heap/mem,Mode=HLL) -> gdt(Hll8,heap,Mode=HLL)
        mergeHlltoHLLmode(source, gadget, srcLgK, gadgetLgK, srcIsMem, gdtIsMem);
        hllSketchImpl = gadget.putOutOfOrderFlag(true).hllSketchImpl;
        break;
      }
      case 12://src <= max, src <  gdt, gdtHLL, gdtHeap
      { //Action: downsample gdt to srcLgK, forward HLL merge w/autofold, ooof=True
        final HllSketch gdtHll8Heap = downsample(gadget, srcLgK);
        //merge src(Hll4,6,8;heap/mem,Mode=HLL) -> gdt(Hll8,heap,hll)
        mergeHlltoHLLmode(source, gdtHll8Heap, srcLgK, gadgetLgK, srcIsMem, false);
        hllSketchImpl = gdtHll8Heap.putOutOfOrderFlag(true).hllSketchImpl;
        break;
      }
      case 13://src <= max, src < gdt, gdtHLL, gdtMemory
      { //Action: downsample gdt to srcLgK, forward HLL merge w/autofold, use gdt memory, ooof=True
        final HllSketch gdtHll8Heap = downsample(gadget, srcLgK);
        //merge src(Hll4,6,8;heap/mem;Mode=HLL) -> gdt(Hll8,heap,Mode=HLL)
        mergeHlltoHLLmode(source, gdtHll8Heap, srcLgK, gadgetLgK, srcIsMem, false);
        hllSketchImpl = useGadgetMemory(gadget, gdtHll8Heap, true).hllSketchImpl;
        break;
      }

      case 6: //src <= max, src >= gdt, gdtEmpty, gdtHeap
      case 14://src <= max, src <  gdt, gdtEmpty, gdtHeap
      { //Action: copy src, replace gdt, ooof=src
        final HllSketch srcHll8Heap = source.copyAs(HLL_8);
        hllSketchImpl = srcHll8Heap.hllSketchImpl;
        break;
      }
      case 22://src >  max, src >= gdt, gdtEmpty, gdtHeap
      { //Action: downsample src to lgMaxK, replace gdt, ooof=src
        final HllSketch srcHll8Heap = downsample(source, lgMaxK);
        hllSketchImpl = srcHll8Heap.hllSketchImpl;
        break;
      }

      case 7: //src <= max, src >= gdt, gdtEmpty, gdtMemory
      case 15://src <= max, src <  gdt, gdtEmpty, gdtMemory
      { //Action: copy src, use gdt memory, ooof=src
        final HllSketch srcHll8Heap = source.copyAs(HLL_8);
        hllSketchImpl = useGadgetMemory(gadget, srcHll8Heap, false).hllSketchImpl;
        break;
      }
      case 23://src >  max, src >= gdt, gdtEmpty, gdtMemory, replace mem, downsample src, ooof=src
      { //Action: downsample src to lgMaxK, use gdt memory, ooof=src
        final HllSketch srcHll8Heap = downsample(source, lgMaxK);
        hllSketchImpl = useGadgetMemory(gadget, srcHll8Heap, false).hllSketchImpl;
        break;
      }
      //default: return gadget.hllSketchImpl; //not possible
    }
    return hllSketchImpl;
  }

  private static final HllSketch useGadgetMemory(
      final HllSketch gadget, final HllSketch hll8Heap, final boolean setOooFlag) {
    final WritableMemory wmem = gadget.getWritableMemory();    //use the gdt wmem
    final byte[] byteArr = hll8Heap.toUpdatableByteArray();    //serialize srcCopy
    wmem.putByteArray(0, byteArr, 0, byteArr.length);          //replace old data with new
    return (setOooFlag)
        ? HllSketch.writableWrap(wmem, false).putOutOfOrderFlag(true) //wrap, set oooflag, return
        : HllSketch.writableWrap(wmem, false);                        //wrap & return
  }

  private static final void mergeHlltoHLLmode(final HllSketch src, final HllSketch tgt,
      final int srcLgK, final int tgtLgK, final boolean srcIsMem, final boolean tgtIsMem) {
      final int sw = (tgtIsMem ? 1 : 0) | (srcIsMem ? 2 : 0)
          | ((srcLgK > tgtLgK) ? 4 : 0) | ((src.getTgtHllType() != HLL_8) ? 8 : 0);
      switch (sw) {
        case 0: { //HLL_8, srcLgK=tgtLgK, src=heap, tgt=heap
          final int srcK = 1 << srcLgK;
          final byte[] srcArr = ((Hll8Array) src.hllSketchImpl).hllByteArr;
          final byte[] tgtArr = ((Hll8Array) tgt.hllSketchImpl).hllByteArr;
          for (int i = 0; i < srcK; i++) {
            final byte srcV = srcArr[i];
            final byte tgtV = tgtArr[i];
            tgtArr[i] = (srcV > tgtV) ? srcV : tgtV;
          }
          break;
        }
        case 1: { //HLL_8, srcLgK=tgtLgK, src=heap, tgt=mem
          final int srcK = 1 << srcLgK;
          final byte[] srcArr = ((Hll8Array) src.hllSketchImpl).hllByteArr;
          final WritableMemory tgtMem = tgt.getWritableMemory();
          for (int i = 0; i < srcK; i++) {
            final byte srcV = srcArr[i];
            final byte tgtV = tgtMem.getByte(HLL_BYTE_ARR_START + i);
            tgtMem.putByte(HLL_BYTE_ARR_START + i, (srcV > tgtV) ? srcV : tgtV);
          }
          break;
        }
        case 2: { //HLL_8, srcLgK=tgtLgK, src=mem,  tgt=heap
          final int srcK = 1 << srcLgK;
          final Memory srcMem = src.getMemory();
          final byte[] tgtArr = ((Hll8Array) tgt.hllSketchImpl).hllByteArr;
          for (int i = 0; i < srcK; i++) {
            final byte srcV = srcMem.getByte(HLL_BYTE_ARR_START + i);
            final byte tgtV = tgtArr[i];
            tgtArr[i] = (srcV > tgtV) ? srcV : tgtV;
          }
          break;
        }
        case 3: { //HLL_8, srcLgK=tgtLgK, src=mem,  tgt=mem
          final int srcK = 1 << srcLgK;
          final Memory srcMem = src.getMemory();
          final WritableMemory tgtMem = tgt.getWritableMemory();
          for (int i = 0; i < srcK; i++) {
            final byte srcV = srcMem.getByte(HLL_BYTE_ARR_START + i);
            final byte tgtV = tgtMem.getByte(HLL_BYTE_ARR_START + i);
            tgtMem.putByte(HLL_BYTE_ARR_START + i, (srcV > tgtV) ? srcV : tgtV);
          }
          break;
        }
        case 4: { //HLL_8, srcLgK>tgtLgK, src=heap, tgt=heap
          final int srcK = 1 << srcLgK;
          final int tgtKmask = (1 << tgtLgK) - 1;
          final byte[] srcArr = ((Hll8Array) src.hllSketchImpl).hllByteArr;
          final byte[] tgtArr = ((Hll8Array) tgt.hllSketchImpl).hllByteArr;
          for (int i = 0; i < srcK; i++) {
            final byte srcV = srcArr[i];
            final int j = i & tgtKmask;
            final byte tgtV = tgtArr[j];
            tgtArr[j] = (srcV > tgtV) ? srcV : tgtV;
          }
          break;
        }
        case 5: { //HLL_8, srcLgK>tgtLgK, src=heap, tgt=mem
          final int srcK = 1 << srcLgK;
          final int tgtKmask = (1 << tgtLgK) - 1;
          final byte[] srcArr = ((Hll8Array) src.hllSketchImpl).hllByteArr;
          final WritableMemory tgtMem = tgt.getWritableMemory();
          for (int i = 0; i < srcK; i++) {
            final byte srcV = srcArr[i];
            final int j = i & tgtKmask;
            final byte tgtV = tgtMem.getByte(HLL_BYTE_ARR_START + j);
            tgtMem.putByte(HLL_BYTE_ARR_START + j, (srcV > tgtV) ? srcV : tgtV);
          }
          break;
        }
        case 6: { //HLL_8, srcLgK>tgtLgK, src=mem,  tgt=heap
          final int srcK = 1 << srcLgK;
          final int tgtKmask = (1 << tgtLgK) - 1;
          final Memory srcMem = src.getMemory();
          final byte[] tgtArr = ((Hll8Array) tgt.hllSketchImpl).hllByteArr;
          for (int i = 0; i < srcK; i++) {
            final byte srcV = srcMem.getByte(HLL_BYTE_ARR_START + i);
            final int j = i & tgtKmask;
            final byte tgtV = tgtArr[j];
            tgtArr[j] = (srcV > tgtV) ? srcV : tgtV;
          }
          break;
        }
        case 7: { //HLL_8, srcLgK>tgtLgK, src=mem,  tgt=mem
          final int srcK = 1 << srcLgK;
          final int tgtKmask = (1 << tgtLgK) - 1;
          final Memory srcMem = src.getMemory();
          final WritableMemory tgtMem = tgt.getWritableMemory();
          for (int i = 0; i < srcK; i++) {
            final byte srcV = srcMem.getByte(HLL_BYTE_ARR_START + i);
            final int j = i & tgtKmask;
            final byte tgtV = tgtMem.getByte(HLL_BYTE_ARR_START + j);
            tgtMem.putByte(HLL_BYTE_ARR_START + j, (srcV > tgtV) ? srcV : tgtV);
          }
          break;
        }
        case 8: case 9: case 10: case 11:
        {
          //!HLL_8, srcLgK=tgtLgK, src=heap/mem, tgt=heap/mem
          final int srcK = 1 << srcLgK;
          final AbstractHllArray srcAbsHllArr = (AbstractHllArray)(src.hllSketchImpl);
          final AbstractHllArray tgtAbsHllArr = (AbstractHllArray)(tgt.hllSketchImpl);
          for (int i = 0; i < srcK; i++) {
            final int srcV = srcAbsHllArr.getSlotValue(i);
            tgtAbsHllArr.updateSlotNoKxQ(i, srcV);
          }
          break;
        }
        case 12: case 13: case 14: case 15:
        {
          //!HLL_8, srcLgK>tgtLgK, src=heap/mem, tgt=heap/mem
          final int srcK = 1 << srcLgK;
          final int tgtKmask = (1 << tgtLgK) - 1;
          final AbstractHllArray srcAbsHllArr = (AbstractHllArray)(src.hllSketchImpl);
          final AbstractHllArray tgtAbsHllArr = (AbstractHllArray)(tgt.hllSketchImpl);
          for (int i = 0; i < srcK; i++) {
            final int srcV = srcAbsHllArr.getSlotValue(i);
            final int j = i & tgtKmask;
            tgtAbsHllArr.updateSlotNoKxQ(j, srcV);
          }
          break;
        }
      }
      tgt.hllSketchImpl.putRebuildCurMinNumKxQFlag(true);
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
      tgtHllArr.couponUpdate(candItr.getPair()); //rebuilds KxQ, etc.
    }
    //both of these are required for isomorphism
    tgtHllArr.putHipAccum(candArr.getHipAccum());
    tgtHllArr.putOutOfOrder(candidate.isOutOfOrder());
    tgtHllArr.putRebuildCurMinNumKxQFlag(false);
    return new HllSketch(tgtHllArr);
  }

  //Used to rebuild curMin, numAtCurMin and KxQ registers, due to high performance merge operation
  static final void checkRebuildCurMinNumKxQ(final HllSketch sketch) {
    final HllSketchImpl hllSketchImpl = sketch.hllSketchImpl;
    final CurMode curMode = sketch.getCurMode();
    final TgtHllType tgtHllType = sketch.getTgtHllType();
    final boolean rebuild = hllSketchImpl.isRebuildCurMinNumKxQFlag();
    if ( !rebuild || (curMode != CurMode.HLL) || (tgtHllType != HLL_8) ) { return; }
    final AbstractHllArray absHllArr = (AbstractHllArray)(hllSketchImpl);
    int curMin = 64;
    int numAtCurMin = 0;
    double kxq0 = 1 << absHllArr.getLgConfigK();
    double kxq1 = 0;
    final PairIterator itr = absHllArr.iterator();
    while (itr.nextAll()) {
      final int v = itr.getValue();
      if (v > 0) {
        if (v < 32) { kxq0 += invPow2(v) - 1.0; }
        else        { kxq1 += invPow2(v) - 1.0; }
      }
      if (v > curMin) { continue; }
      if (v < curMin) {
        curMin = v;
        numAtCurMin = 1;
      } else {
        numAtCurMin++;
      }
    }
    absHllArr.putKxQ0(kxq0);
    absHllArr.putKxQ1(kxq1);
    absHllArr.putCurMin(curMin);
    absHllArr.putNumAtCurMin(numAtCurMin);
    absHllArr.putRebuildCurMinNumKxQFlag(false);
    //HipAccum is not affected
  }

}

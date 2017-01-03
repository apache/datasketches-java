/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.DoublesUtil.copyToHeap;

import com.yahoo.memory.Memory;

/**
 * Union operation for on-heap.
 *
 * @author Lee Rhodes
 * @author Kevin Lang
 */
public final class DoublesUnionImpl extends DoublesUnion {
  private int k_;
  private DoublesSketch gadget_ = null;

  private DoublesUnionImpl(final int k) {
    k_ = k;
  }

  /**
   * Returns a empty Heap DoublesUnion object.
   * @param k the specified <i>k</i> for the DoublesUnion object
   */
  static DoublesUnionImpl heapInstance(final int k) {
    final DoublesUnionImpl union = new DoublesUnionImpl(k);
    return union;
  }

  /**
   * Returns a empty DoublesUnion object that refers to the given direct, off-heap Memory,
   * which will be initialized to the empty state.
   *
   * @param dstMem the Memory to be used by the sketch
   * @return a DoublesUnion object
   */
  static DoublesUnionImpl directInstance(final int k, final Memory dstMem) {
    final DirectDoublesSketch sketch = DirectDoublesSketch.newInstance(k, dstMem);
    final DoublesUnionImpl union = new DoublesUnionImpl(k);
    union.k_ = k;
    union.gadget_ = sketch;
    return union;
  }

  /**
   * Returns a Heap DoublesUnion object that has been initialized with the data from the given
   * sketch.
   *
   * @param sketch A DoublesSketch to be used as a source of data only and will not be modified.
   * @return a DoublesUnion object
   */
  static DoublesUnionImpl heapifyInstance(final DoublesSketch sketch) {
    final int k = sketch.getK();
    final DoublesUnionImpl union = new DoublesUnionImpl(k);
    union.k_ = k;
    union.gadget_ = copyToHeap(sketch);
    return union;
  }

  /**
   * Returns a Heap DoublesUnion object that has been initialized with the data from the given
   * Memory image of a DoublesSketch. The srcMem object will not be modified and a reference to
   * it is not retained.
   *
   * @param srcMem a Memory image of a quantiles DoublesSketch
   * @return a DoublesUnion object
   */
  static DoublesUnionImpl heapifyInstance(final Memory srcMem) {
    final HeapDoublesSketch sketch = HeapDoublesSketch.heapifyInstance(srcMem);
    final int k = sketch.getK();
    final DoublesUnionImpl union = new DoublesUnionImpl(k);
    union.k_ = k;
    union.gadget_ = sketch;
    return union;
  }

  /**
   * Returns a Union object that wraps off-heap data structure of the given memory image of
   * a non-compact DoublesSketch. The data structures of the Union remain off-heap.
   *
   * @param mem A memory image of a non-compact DoublesSketch to be used as the data
   * structure for the union and will be modified.
   * @return a Union object
   */
  static DoublesUnionImpl wrapInstance(final Memory mem) {
    final DirectDoublesSketch sketch = DirectDoublesSketch.wrapInstance(mem);
    final int k = sketch.getK();
    final DoublesUnionImpl union = new DoublesUnionImpl(k);
    union.k_ = k;
    union.gadget_ = sketch;
    return union;
  }

  @Override
  public boolean isEmpty() {
    return (gadget_ == null) ? true : gadget_.isEmpty();
  }

  @Override
  public boolean isDirect() {
    return (gadget_ == null) ? false : gadget_.isDirect();
  }

  @Override
  public void update(final DoublesSketch sketchIn) {
    gadget_ = updateLogic(k_, gadget_, sketchIn);
  }

  @Override
  public void update(final Memory mem) {
    gadget_ = updateLogic(k_, gadget_, HeapDoublesSketch.heapifyInstance(mem));
  }

  @Override
  public void update(final double dataItem) {
    if (gadget_ == null) {
      gadget_ = HeapDoublesSketch.newInstance(k_);
    }
    gadget_.update(dataItem);
  }

  @Override
  public DoublesSketch getResult() {
    if (gadget_ == null) {
      return HeapDoublesSketch.newInstance(k_);
    }
    return DoublesUtil.copyToHeap(gadget_); //can't have any externally owned handles.
  }

  @Override
  public DoublesSketch getResultAndReset() {
    if (gadget_ == null) { return null; } //Intentionally return null here for speed.
    final DoublesSketch ds = gadget_;
    gadget_ = null;
    return ds;
  }

  @Override
  public void reset() {
    gadget_ = null;
  }

  @Override
  public byte[] toByteArray() {
    if (gadget_ == null) {
      final HeapDoublesSketch sketch = HeapDoublesSketch.newInstance(k_);
      return DoublesByteArrayImpl.toByteArray(sketch, true, false);
    }
    return DoublesByteArrayImpl.toByteArray(gadget_, true, false);
  }

  @Override
  public String toString() {
    return toString(true, false);
  }

  @Override
  public String toString(final boolean sketchSummary, final boolean dataDetail) {
    if (gadget_ == null) {
      return HeapDoublesSketch.newInstance(k_).toString();
    }
    return gadget_.toString(sketchSummary, dataDetail);
  }

  //@formatter:off
  @SuppressWarnings("null")
  static DoublesSketch updateLogic(final int myK, final DoublesSketch myQS,
      final DoublesSketch other) {
    int sw1 = ((myQS  == null) ? 0 :  myQS.isEmpty() ? 4 : 8);
    sw1 |=    ((other == null) ? 0 : other.isEmpty() ? 1 : 2);
    int outCase = 0; //0=null, 1=NOOP, 2=copy, 3=merge
    switch (sw1) {
      case 0:  outCase = 0; break; //myQS = null,  other = null ; return null
      case 1:  outCase = 4; break; //myQS = null,  other = empty; copy or downsample(myK)
      case 2:  outCase = 2; break; //myQS = null,  other = valid; copy or downsample(myK)
      case 4:  outCase = 1; break; //myQS = empty, other = null ; no-op
      case 5:  outCase = 1; break; //myQS = empty, other = empty; no-op
      case 6:  outCase = 3; break; //myQS = empty, other = valid; merge
      case 8:  outCase = 1; break; //myQS = valid, other = null ; no-op
      case 9:  outCase = 1; break; //myQS = valid, other = empty: no-op
      case 10: outCase = 3; break; //myQS = valid, other = valid; merge
      //default: //This cannot happen and cannot be tested
    }
    DoublesSketch ret = null;
    switch (outCase) {
      case 0: ret = null; break; //retun null
      case 1: ret = myQS; break; //no-op
      case 2: { //myQS = null, other = valid
        if (myK < other.getK()) {
          ret = other.downSample(other, myK, null);
        } else {
          ret = DoublesUtil.copyToHeap(other); //required because caller has handle
        }
        break;
      }
      case 3: { //must merge
        if (myQS.getK() <= other.getK()) { //I am smaller or equal, thus the target
          DoublesUnionImpl.mergeInto(other, myQS);
          ret = myQS;
        } else {
          //myQS_K > other_K, must reverse roles
          //must copy other as it will become mine and can't have any externally owned handles.
          final DoublesSketch myNewQS = DoublesUtil.copyToHeap(other);
          DoublesUnionImpl.mergeInto(myQS, myNewQS);
          ret = myNewQS;
        }
        break;
      }
      case 4: {
        ret = HeapDoublesSketch.newInstance(Math.min(myK, other.getK()));
        break;
      }
      //default: //This cannot happen and cannot be tested
    }
    return ret;
  }
  //@formatter:on

  /**
   * Merges the source sketch into the target sketch that can have a smaller value of K.
   * However, it is required that the ratio of the two K values be a power of 2.
   * I.e., source.getK() = target.getK() * 2^(nonnegative integer).
   * The source is not modified.
   *
   * <p>Note: It is easy to prove that the following simplified code which launches multiple waves of
   * carry propagation does exactly the same amount of merging work (including the work of
   * allocating fresh buffers) as the more complicated and seemingly more efficient approach that
   * tracks a single carry propagation wave through both sketches.
   *
   * <p>This simplified code probably does do slightly more "outer loop" work, but I am pretty
   * sure that even that is within a constant factor of the more complicated code, plus the
   * total amount of "outer loop" work is at least a factor of K smaller than the total amount of
   * merging work, which is identical in the two approaches.
   *
   * <p>Note: a two-way merge that doesn't modify either of its two inputs could be implemented
   * by making a deep copy of the larger sketch and then merging the smaller one into it.
   * However, it was decided not to do this.
   *
   * @param src The source sketch
   * @param tgt The target sketch
   */

  static void mergeInto(final DoublesSketch src, final DoublesSketch tgt) {
    final int srcK = src.getK();
    final int tgtK = tgt.getK();
    final long srcN = src.getN();
    final long tgtN = tgt.getN();

    if (srcK != tgtK) {
      DoublesMergeImpl.downSamplingMergeInto(src, tgt);
      return;
    }

    final double[] srcCombBuf = src.getCombinedBuffer();
    final long nFinal = tgtN + srcN;

    for (int i = 0; i < src.getBaseBufferCount(); i++) {
      tgt.update(srcCombBuf[i]);
    }

    final int spaceNeeded = DoublesUpdateImpl.maybeGrowLevels(tgtK, nFinal);
    final int curCombBufCap = tgt.getCombinedBufferItemCapacity();
    if (spaceNeeded > curCombBufCap) {
      tgt.growCombinedBuffer(curCombBufCap, spaceNeeded); // copies base buffer plus current levels
    }

    final double[] scratch2KBuf = new double[2 * tgtK];

    long srcBitPattern = src.getBitPattern();
    assert srcBitPattern == (srcN / (2L * srcK));

    for (int srcLvl = 0; srcBitPattern != 0L; srcLvl++, srcBitPattern >>>= 1) {
      if ((srcBitPattern & 1L) > 0L) {
        final long newTgtBitPattern = DoublesUpdateImpl.inPlacePropagateCarry(
            srcLvl,
            srcCombBuf, ((2 + srcLvl) * tgtK),
            scratch2KBuf, 0,
            false,
            tgtK,
            tgt.getCombinedBuffer(),
            tgt.getBitPattern()
        );
        tgt.putBitPattern(newTgtBitPattern);
        // won't update qsTarget.n_ until the very end
      }
    }

    tgt.putN(nFinal);

    assert tgt.getN() / (2 * tgtK) == tgt.getBitPattern(); // internal consistency check

    final double srcMax = src.getMaxValue();
    final double srcMin = src.getMinValue();
    final double tgtMax = tgt.getMaxValue();
    final double tgtMin = tgt.getMinValue();
    if (srcMax > tgtMax) { tgt.putMaxValue(srcMax); }
    if (srcMin < tgtMin) { tgt.putMinValue(srcMin); }
  }

}

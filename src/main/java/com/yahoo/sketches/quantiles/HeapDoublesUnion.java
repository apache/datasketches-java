/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import com.yahoo.sketches.memory.Memory;

/**
 * Union operation for on-heap.
 * 
 * @author Lee Rhodes
 */
final class HeapDoublesUnion extends DoublesUnion {
  private final int k_;
  private HeapDoublesSketch gadget_;

  HeapDoublesUnion(final int k) {
    k_ = k;
  }
  
  HeapDoublesUnion(final DoublesSketch sketch) {
    k_ = sketch.getK();
    gadget_ = (HeapDoublesSketch) sketch;
  }
  
  /**
   * Heapify the given srcMem into a HeapUnion object.
   * @param srcMem the given srcMem. 
   * A reference to srcMem will not be maintained internally.
   */
  HeapDoublesUnion(final Memory srcMem) {
    gadget_ = HeapDoublesSketch.getInstance(srcMem);
    k_ = gadget_.getK();
  }
  
  @Override
  public void update(DoublesSketch sketchIn) {
    gadget_ = updateLogic(k_, gadget_, (HeapDoublesSketch)sketchIn);
  }

  @Override
  public void update(Memory srcMem) {
    HeapDoublesSketch that = HeapDoublesSketch.getInstance(srcMem);
    gadget_ = updateLogic(k_, gadget_, that);
  }

  @Override
  public void update(double dataItem) {
    if (gadget_ == null) gadget_ = HeapDoublesSketch.getInstance(k_);
    gadget_.update(dataItem);
  }

  @Override
  public DoublesSketch getResult() {
    if (gadget_ == null) return HeapDoublesSketch.getInstance(k_);
    return HeapDoublesSketch.copy(gadget_); //can't have any externally owned handles.
  }
  
  @Override
  public DoublesSketch getResultAndReset() {
    if (gadget_ == null) return null; //Intentionally return null here for speed.
    DoublesSketch hqs = gadget_;
    gadget_ = null;
    return hqs;
  }
  
  @Override
  public void reset() {
    gadget_ = null;
  }
  
  @Override
  public String toString() {
    return toString(true, false);
  }
  
  @Override
  public String toString(boolean sketchSummary, boolean dataDetail) {
    if (gadget_ == null) return HeapDoublesSketch.getInstance(k_).toString();
    return gadget_.toString(sketchSummary, dataDetail);
  }
  

//@formatter:off
  @SuppressWarnings("null")
  static HeapDoublesSketch updateLogic(final int myK, final HeapDoublesSketch myQS, 
      final HeapDoublesSketch other) {
    int sw1 = ((myQS   == null)? 0 :   myQS.isEmpty()? 4: 8);
    sw1 |=    ((other  == null)? 0 :  other.isEmpty()? 1: 2);
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
    HeapDoublesSketch ret = null;
    switch (outCase) {
      case 0: ret = null; break;
      case 1: ret = myQS; break;
      case 2: {
        if (myK < other.getK()) {
          ret = (HeapDoublesSketch) other.downSample(myK);
        } else {
          ret = HeapDoublesSketch.copy(other); //required because caller has handle
        }
        break;
      }
      case 3: { //must merge
        if (myQS.getK() <= other.getK()) { //I am smaller or equal, thus the target
          HeapDoublesUnion.mergeInto(other, myQS);
          ret = myQS;
        } else {
          //myQS_K > other_K, must reverse roles
          //must copy other as it will become mine and can't have any externally owned handles.
          HeapDoublesSketch myNewQS = HeapDoublesSketch.copy(other);
          HeapDoublesUnion.mergeInto(myQS, myNewQS);
          ret = myNewQS;
        }
        break;
      }
      case 4: {
        ret = HeapDoublesSketch.getInstance(Math.min(myK, other.getK()));
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
   * <p> This simplified code probably does do slightly more "outer loop" work, but I am pretty 
   * sure that even that is within a constant factor of the more complicated code, plus the 
   * total amount of "outer loop" work is at least a factor of K smaller than the total amount of 
   * merging work, which is identical in the two approaches.
   *
   * <p>Note: a two-way merge that doesn't modify either of its two inputs could be implemented 
   * by making a deep copy of the larger sketch and then merging the smaller one into it.
   * However, it was decided not to do this.
   * 
   * @param source The source sketch
   * @param target The target sketch
   */
  
  static void mergeInto(DoublesSketch source, DoublesSketch target) {
    
    HeapDoublesSketch src = (HeapDoublesSketch)source;
    HeapDoublesSketch tgt = (HeapDoublesSketch)target;
    int srcK = src.getK();
    int tgtK = tgt.getK();
    long srcN = src.getN();
    long tgtN = tgt.getN();
    
    if (srcK != tgtK) {
      DoublesUtil.downSamplingMergeInto(src, tgt);
      return;
    }
    
    double[] srcLevels     = src.getCombinedBuffer(); // aliasing is a bit dangerous
    double[] srcBaseBuffer = srcLevels;               // aliasing is a bit dangerous
  
    long nFinal = tgtN + srcN;
  
    for (int i = 0; i < src.getBaseBufferCount(); i++) {
      tgt.update(srcBaseBuffer[i]);
    }
  
    DoublesUtil.maybeGrowLevels(nFinal, tgt);
  
    double[] scratchBuf = new double[2*tgtK];
  
    long srcBitPattern = src.getBitPattern();
    assert srcBitPattern == (srcN / (2L * srcK));
    for (int srcLvl = 0; srcBitPattern != 0L; srcLvl++, srcBitPattern >>>= 1) {
      if ((srcBitPattern & 1L) > 0L) {
        DoublesUtil.inPlacePropagateCarry(
            srcLvl,
            srcLevels, ((2+srcLvl) * tgtK),
            scratchBuf, 0,
            false, tgt);
        // won't update qsTarget.n_ until the very end
      }
    }
  
    tgt.n_ = nFinal;
    
    assert tgt.getN() / (2*tgtK) == tgt.getBitPattern(); // internal consistency check
    
    double srcMax = src.getMaxValue();
    double srcMin = src.getMinValue();
    double tgtMax = tgt.getMaxValue();
    double tgtMin = tgt.getMinValue();
    if (srcMax > tgtMax) { tgt.maxValue_ = srcMax; }
    if (srcMin < tgtMin) { tgt.minValue_ = srcMin; }
  }

}

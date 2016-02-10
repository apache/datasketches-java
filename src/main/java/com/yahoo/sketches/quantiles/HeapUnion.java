/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.quantiles;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.MemoryRequest;

/**
 * Union operation for on-heap.
 * 
 * @author Lee Rhodes
 */
@SuppressWarnings("unused")
class HeapUnion extends Union {
  private HeapQuantilesSketch gadget_ = null;
  
  HeapUnion() {} //creates a virgin Union object
  
  HeapUnion(QuantilesSketch sketch) {
    gadget_ = (HeapQuantilesSketch) sketch;
  }
  
  /**
   * Heapify the given srcMem into a HeapUnion object.
   * @param srcMem the given srcMem. 
   * A reference to srcMem will not be maintained internally.
   */
  HeapUnion(Memory srcMem) {
    gadget_ = HeapQuantilesSketch.getInstance(srcMem);
  }
  
  @Override
  public void update(QuantilesSketch sketchIn) {
    gadget_ = unionUpdateLogic(gadget_, (HeapQuantilesSketch)sketchIn);
  }

  @Override
  public void update(Memory srcMem) {
    HeapQuantilesSketch that = HeapQuantilesSketch.getInstance(srcMem);
    gadget_ = unionUpdateLogic(gadget_, that);
  }

  @Override
  public void update(double dataItem) {
    gadget_.update(dataItem);
  }

  @Override
  public QuantilesSketch getResult() {
    if (gadget_ == null) return null;
    return HeapQuantilesSketch.copy(gadget_);
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
    return gadget_.toString(sketchSummary, dataDetail);
  }
  

//@formatter:off
  @SuppressWarnings("null")
  static HeapQuantilesSketch unionUpdateLogic(HeapQuantilesSketch qs1, HeapQuantilesSketch qs2) {
    int sw1 = ((qs1   == null)? 0 :   qs1.isEmpty()? 4: 8);
    sw1 |=    ((qs2 == null)? 0 : qs2.isEmpty()? 1: 2);
    int outCase = 0; //0=null, 1=NOOP, 2=copy, 3=merge 
    switch (sw1) {
      case 0:  outCase = 0; break; //null   qs1 = null,  qs2 = null
      case 1:  outCase = 2; break; //copy   qs1 = null,  qs2 = empty
      case 2:  outCase = 2; break; //copy   qs1 = null,  qs2 = valid
      case 4:  outCase = 1; break; //noop   qs1 = empty, qs2 = null 
      case 5:  outCase = 1; break; //noop   qs1 = empty, qs2 = empty
      case 6:  outCase = 3; break; //merge  qs1 = empty, qs2 = valid
      case 8:  outCase = 1; break; //noop   qs1 = valid, qs2 = null
      case 9:  outCase = 1; break; //noop   qs1 = valid, qs2 = empty
      case 10: outCase = 3; break; //merge  qs1 = valid, qs2 = valid
    }
    switch (outCase) {
      case 0: return null;
      case 1: return qs1;
      case 2: {
        return HeapQuantilesSketch.copy(qs2);
      }
    }
    //must merge
    if (qs1.getK() <= qs2.getK()) {
      HeapUnion.mergeInto(qs2, qs1);
      return qs1;
    }
    
    //qs1K > qs2K
    HeapQuantilesSketch copyQS2 = HeapQuantilesSketch.copy(qs2);
    HeapUnion.mergeInto(qs1, copyQS2);
    return copyQS2;
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
  
  static void mergeInto(QuantilesSketch source, QuantilesSketch target) {
    
    HeapQuantilesSketch src = (HeapQuantilesSketch)source;
    HeapQuantilesSketch tgt = (HeapQuantilesSketch)target;
    int srcK = src.getK();
    int tgtK = tgt.getK();
    long srcN = src.getN();
    long tgtN = tgt.getN();
    
    if (srcK != tgtK) {
      Util.downSamplingMergeInto(src, tgt);
      return;
    }
    
    double[] srcLevels     = src.getCombinedBuffer(); // aliasing is a bit dangerous
    double[] srcBaseBuffer = srcLevels;               // aliasing is a bit dangerous
  
    long nFinal = tgtN + srcN;
  
    for (int i = 0; i < src.getBaseBufferCount(); i++) {
      tgt.update(srcBaseBuffer[i]);
    }
  
    Util.maybeGrowLevels(nFinal, tgt);
  
    double[] scratchBuf = new double[2*tgtK];
  
    long srcBitPattern = src.getBitPattern();
    assert srcBitPattern == (srcN / (2L * srcK));
    for (int srcLvl = 0; srcBitPattern != 0L; srcLvl++, srcBitPattern >>>= 1) {
      if ((srcBitPattern & 1L) > 0L) {
        Util.inPlacePropagateCarry(
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

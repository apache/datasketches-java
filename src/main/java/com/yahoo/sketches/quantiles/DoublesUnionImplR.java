/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.Util.LS;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.SketchesReadOnlyException;

/**
 * Union operation for on-heap.
 *
 * @author Lee Rhodes
 * @author Kevin Lang
 */
class DoublesUnionImplR extends DoublesUnion {
  int maxK_;
  UpdateDoublesSketch gadget_ = null;

  DoublesUnionImplR(final int maxK) {
    maxK_ = maxK;
  }

  /**
   * Returns a read-only Union object that wraps off-heap data structure of the given memory
   * image of a non-compact DoublesSketch. The data structures of the Union remain off-heap.
   *
   * @param mem A memory image of a non-compact DoublesSketch to be used as the data
   * structure for the union and will be modified.
   * @return a Union object
   */
  static DoublesUnionImplR wrapInstance(final Memory mem) {
    final DirectUpdateDoublesSketchR sketch = DirectUpdateDoublesSketchR.wrapInstance(mem);
    final int k = sketch.getK();
    final DoublesUnionImplR union = new DoublesUnionImplR(k);
    union.maxK_ = k;
    union.gadget_ = sketch;
    return union;
  }

  @Override
  public void update(final DoublesSketch sketchIn) {
    throw new SketchesReadOnlyException("Call to update() on read-only Union");
  }

  @Override
  public void update(final Memory mem) {
    throw new SketchesReadOnlyException("Call to update() on read-only Union");
  }

  @Override
  public void update(final double dataItem) {
    throw new SketchesReadOnlyException("Call to update() on read-only Union");
  }

  @Override
  public byte[] toByteArray() {
    if (gadget_ == null) {
      return DoublesSketch.builder().setK(maxK_).build().toByteArray();
    }
    return gadget_.toByteArray();
  }

  @Override
  public UpdateDoublesSketch getResult() {
    if (gadget_ == null) {
      return HeapUpdateDoublesSketch.newInstance(maxK_);
    }
    return DoublesUtil.copyToHeap(gadget_); //can't have any externally owned handles.
  }

  @Override
  public UpdateDoublesSketch getResult(final WritableMemory dstMem) {
    final long memCapBytes = dstMem.getCapacity();
    if (gadget_ == null) {
      if (memCapBytes < DoublesSketch.getUpdatableStorageBytes(0, 0)) {
        throw new SketchesArgumentException("Insufficient capacity for result: " + memCapBytes);
      }
      return DirectUpdateDoublesSketch.newInstance(maxK_, dstMem);
    }

    gadget_.putMemory(dstMem, false);
    return DirectUpdateDoublesSketch.wrapInstance(dstMem);
  }

  @Override
  public UpdateDoublesSketch getResultAndReset() {
    throw new SketchesReadOnlyException("Call to getResultAndReset() on read-only Union");
  }

  @Override
  public void reset() {
    throw new SketchesReadOnlyException("Call to reset() on read-only Union");
  }

  @Override
  public boolean isEmpty() {
    return (gadget_ == null) || gadget_.isEmpty();
  }

  @Override
  public boolean isDirect() {
    return (gadget_ != null) && gadget_.isDirect();
  }

  @Override
  public int getMaxK() {
    return maxK_;
  }

  @Override
  public int getEffectiveK() {
    return (gadget_ != null) ? gadget_.getK() : maxK_;
  }

  @Override
  public String toString() {
    return toString(true, false);
  }

  @Override
  public String toString(final boolean sketchSummary, final boolean dataDetail) {
    final StringBuilder sb = new StringBuilder();
    final String thisSimpleName = this.getClass().getSimpleName();
    final int maxK = getMaxK();
    final String kStr = String.format("%,d", maxK);
    sb.append(Util.LS).append("### Quantiles ").append(thisSimpleName).append(LS);
    sb.append("   maxK                         : ").append(kStr);
    if (gadget_ == null) {
      sb.append(HeapUpdateDoublesSketch.newInstance(maxK_).toString());
      return sb.toString();
    }
    sb.append(gadget_.toString(sketchSummary, dataDetail));
    return sb.toString();
  }

  @Override
  public boolean isSameResource(final Memory that) {
    return (gadget_ == null) ? false : gadget_.isSameResource(that);
  }

}

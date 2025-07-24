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

package org.apache.datasketches.quantiles2;

import static org.apache.datasketches.common.Util.LS;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.SketchesReadOnlyException;

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
   * Returns a read-only Union object that wraps off-heap data structure of the given MemorySegment
   * image of a non-compact DoublesSketch. The data structures of the Union remain off-heap.
   *
   * @param seg A MemorySegment image of a non-compact DoublesSketch to be used as the data
   * structure for the union and will be modified.
   * @return a Union object
   */
  static DoublesUnionImplR wrapInstance(final MemorySegment seg) {
    final DirectUpdateDoublesSketchR sketch = DirectUpdateDoublesSketchR.wrapInstance(seg);
    final int k = sketch.getK();
    final DoublesUnionImplR union = new DoublesUnionImplR(k);
    union.maxK_ = k;
    union.gadget_ = sketch;
    return union;
  }

  @Override
  public void union(final DoublesSketch sketchIn) {
    throw new SketchesReadOnlyException("Call to update() on read-only Union");
  }

  @Override
  public void union(final MemorySegment seg) {
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
  public UpdateDoublesSketch getResult(final MemorySegment dstSeg) {
    final long segCapBytes = dstSeg.byteSize();
    if (gadget_ == null) {
      if (segCapBytes < DoublesSketch.getUpdatableStorageBytes(0, 0)) {
        throw new SketchesArgumentException("Insufficient capacity for result: " + segCapBytes);
      }
      return DirectUpdateDoublesSketch.newInstance(maxK_, dstSeg);
    }

    gadget_.putMemory(dstSeg, false);
    return DirectUpdateDoublesSketch.wrapInstance(dstSeg);
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
  public boolean hasMemorySegment() {
    return (gadget_ != null) && gadget_.hasMemorySegment();
  }

  @Override
  public boolean isOffHeap() {
    return (gadget_ != null) && gadget_.isOffHeap();
  }

  @Override
  public boolean isEmpty() {
    return (gadget_ == null) || gadget_.isEmpty();
  }

  @Override
  public boolean isSameResource(final MemorySegment that) {
    return (gadget_ == null) ? false : gadget_.isSameResource(that);
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
    sb.append(LS).append("### Quantiles ").append(thisSimpleName).append(LS);
    sb.append("   maxK                         : ").append(kStr);
    if (gadget_ == null) {
      sb.append(HeapUpdateDoublesSketch.newInstance(maxK_).toString());
      return sb.toString();
    }
    sb.append(gadget_.toString(sketchSummary, dataDetail));
    return sb.toString();
  }

}

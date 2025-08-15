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

package org.apache.datasketches.quantiles;

import static org.apache.datasketches.common.Util.LS;
import static org.apache.datasketches.quantiles.DoublesUtil.copyToHeap;

import java.lang.foreign.MemorySegment;
import java.util.Objects;

import org.apache.datasketches.common.MemorySegmentRequest;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.SketchesReadOnlyException;

/**
 * Union operation.
 *
 * @author Lee Rhodes
 * @author Kevin Lang
 */
final class DoublesUnionImpl extends DoublesUnion {
  int maxK_;
  UpdateDoublesSketch gadget_ = null;

  private DoublesUnionImpl(final int maxK) {
   maxK_ = maxK;
  }

  /**
   * Returns a empty DoublesUnion object on the heap.
   * @param maxK determines the accuracy and size of the union and is a maximum.
   * The effective <i>k</i> can be smaller due to unions with smaller <i>k</i> sketches.
   * It is recommended that <i>maxK</i> be a power of 2 to enable unioning of sketches with
   * different <i>k</i>.
   * @return a new DoublesUnionImpl on the Java heap
   */
  static DoublesUnionImpl heapInstance(final int maxK) {
    return new DoublesUnionImpl(maxK);
  }

  /**
   * Returns a empty DoublesUnion object that uses the given MemorySegment for its internal sketch gadget
   * and will be initialized to the empty state.
   *
   * @param maxK determines the accuracy and size of the union and is a maximum.
   * The effective <i>k</i> can be smaller due to unions with smaller <i>k</i> sketches.
   * It is recommended that <i>maxK</i> be a power of 2 to enable unioning of sketches with
   * different <i>k</i>.
   * @param dstSeg the MemorySegment to be used by the internal sketch and must not be null.
   * @param mSegReq the MemorySegmentRequest used if the given MemorySegment needs to expand.
   * Otherwise, it can be null and the default MemorySegmentRequest will be used.
   * @return a DoublesUnion object
   */
  static DoublesUnionImpl directInstance(final int maxK, final MemorySegment dstSeg, final MemorySegmentRequest mSegReq) {
    Objects.requireNonNull(dstSeg);
    final DirectUpdateDoublesSketch sketch = DirectUpdateDoublesSketch.newInstance(maxK, dstSeg, mSegReq);
    final DoublesUnionImpl union = new DoublesUnionImpl(maxK);
    union.maxK_ = maxK;
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
    Objects.requireNonNull(sketch);
    final int k = sketch.getK();
    final DoublesUnionImpl union = new DoublesUnionImpl(k);
    union.maxK_ = k;
    union.gadget_ = copyToHeap(sketch);
    return union;
  }

  /**
   * Returns a Heap DoublesUnion object that has been initialized with the data from the given
   * MemorySegment image of a DoublesSketch. The srcSeg object will not be modified and a reference to
   * it is not retained. The <i>maxK</i> of the resulting union will be that obtained from
   * the sketch MemorySegment image.
   *
   * @param srcSeg an optionally read-only MemorySegment image of a DoublesSketch
   * @return a DoublesUnion object
   */
  static DoublesUnionImpl heapifyInstance(final MemorySegment srcSeg) {
    Objects.requireNonNull(srcSeg);
    final HeapUpdateDoublesSketch sketch = HeapUpdateDoublesSketch.heapifyInstance(srcSeg);
    final DoublesUnionImpl union = new DoublesUnionImpl(sketch.getK());
    union.gadget_ = sketch;
    return union;
  }

  /**
   * Returns an updatable Union object that wraps the data of the given MemorySegment
   * image of a updatable DoublesSketch. The data of the Union will remain in the MemorySegment.
   *
   * @param srcSeg A MemorySegment image of an updatable DoublesSketch to be used as the data structure for the union and will be modified.
   * @param mSegReq the MemorySegmentRequest used if the given MemorySegment needs to expand.
   * Otherwise, it can be null and the default MemorySegmentRequest will be used.
   * @return a Union object
   */
  static DoublesUnionImpl wrapInstance(final MemorySegment srcSeg, final MemorySegmentRequest mSegReq) {
    Objects.requireNonNull(srcSeg);
    if (srcSeg.isReadOnly()) { throw new SketchesReadOnlyException("Cannot create a Union with a Read Only MemorySegment."); }
    final DirectUpdateDoublesSketch sketch = DirectUpdateDoublesSketch.wrapInstance(srcSeg, mSegReq);
    final DoublesUnionImpl union = new DoublesUnionImpl(sketch.getK());
    union.gadget_ = sketch;
    return union;
  }

  @Override
  public void union(final DoublesSketch sketchIn) {
    Objects.requireNonNull(sketchIn);
    gadget_ = updateLogic(maxK_, gadget_, sketchIn);
    gadget_.doublesSV = null;
  }

  @Override
  public void union(final MemorySegment seg) {
    Objects.requireNonNull(seg);
    gadget_ = updateLogic(maxK_, gadget_, DoublesSketch.wrap(seg, null));
    gadget_.doublesSV = null;
  }

  @Override
  public void update(final double dataItem) {
    if (gadget_ == null) {
      gadget_ = HeapUpdateDoublesSketch.newInstance(maxK_);
    }
    gadget_.update(dataItem);
    gadget_.doublesSV = null;
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
    return DoublesUtil.copyToHeap(gadget_);
  }

  @Override
  public UpdateDoublesSketch getResult(final MemorySegment dstSeg, final MemorySegmentRequest mSegReq) {
    final long segCapBytes = dstSeg.byteSize();
    if (gadget_ == null) {
      if (segCapBytes < DoublesSketch.getUpdatableStorageBytes(0, 0)) {
        throw new SketchesArgumentException("Insufficient capacity for result: " + segCapBytes);
      }
      return DirectUpdateDoublesSketch.newInstance(maxK_, dstSeg, mSegReq);
    }

    gadget_.putIntoMemorySegment(dstSeg, false);
    return DirectUpdateDoublesSketch.wrapInstance(dstSeg, mSegReq);
  }

  @Override
  public UpdateDoublesSketch getResultAndReset() {
    if (gadget_ == null) { return null; } //Intentionally return null here for speed.
    final UpdateDoublesSketch ds = gadget_.getSketchAndReset();
    gadget_ = null;
    return ds;
  }

  @Override
  public void reset() {
    gadget_.reset();
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

  //@formatter:off
  static UpdateDoublesSketch updateLogic(final int myMaxK, final UpdateDoublesSketch myQS, final DoublesSketch other) {
    int sw1 = ((myQS  == null) ? 0 :  myQS.isEmpty() ? 4 : 8);
    sw1 |=    ((other == null) ? 0 : other.isEmpty() ? 1 : 2);
    int outCase = 0; //0=null, 1=NOOP, 2=copy, 3=merge

    switch (sw1) {
      case 0:  outCase = 0; break; //myQS = null,  other = null ; return null
      case 1:  outCase = 4; break; //myQS = null,  other = empty; create empty-heap(myMaxK)
      case 2:  outCase = 2; break; //myQS = null,  other = valid; stream or downsample to myMaxK
      case 4:  outCase = 1; break; //myQS = empty, other = null ; no-op
      case 5:  outCase = 1; break; //myQS = empty, other = empty; no-op
      case 6:  outCase = 3; break; //myQS = empty, other = valid; merge
      case 8:  outCase = 1; break; //myQS = valid, other = null ; no-op
      case 9:  outCase = 1; break; //myQS = valid, other = empty: no-op
      case 10: outCase = 3; break; //myQS = valid, other = valid; merge
      default: break; //This cannot happen
    }
    UpdateDoublesSketch ret = null;

    switch (outCase) {
      case 0: break; //return null
      case 1: ret = myQS; break; //no-op
      case 2: { //myQS = null,  other = valid; stream or downsample to myMaxK
        assert other != null;
        if (!other.isEstimationMode()) { //other is exact, stream items in
          ret = HeapUpdateDoublesSketch.newInstance(myMaxK);
          // exact mode, only need copy base buffer
          final DoublesSketchAccessor otherAccessor = DoublesSketchAccessor.wrap(other, false);
          for (int i = 0; i < otherAccessor.numItems(); ++i) {
            ret.update(otherAccessor.get(i));
          }
        }
        else { //myQS = null, other is est mode
          ret = (myMaxK < other.getK())
              ? other.downSampleInternal(other, myMaxK, null, null) //null seg, null mSegReq
              : DoublesUtil.copyToHeap(other); //copy required because caller has handle
        }
        break;
      }
      case 3: { //myQS = empty/valid, other = valid; merge
        assert other != null;
        assert myQS != null;
        if (!other.isEstimationMode()) { //other is exact, stream items in
          ret = myQS;
          // exact mode, only need copy base buffer
          final DoublesSketchAccessor otherAccessor = DoublesSketchAccessor.wrap(other, false);
          for (int i = 0; i < otherAccessor.numItems(); ++i) {
            ret.update(otherAccessor.get(i));
          }
        } else if (myQS.getK() <= other.getK()) { //I am smaller or equal, thus the target
          DoublesMergeImpl.mergeInto(other, myQS);
          ret = myQS;
        } else if (myQS.isEmpty()) {
          if (myQS.hasMemorySegment()) {
            final MemorySegment seg = myQS.getMemorySegment(); //myQS is empty, ok to reconfigure
            other.putIntoMemorySegment(seg, false); // not compact, but BaseBuf ordered
            ret = DirectUpdateDoublesSketch.wrapInstance(seg, null);
          } else { //myQS is empty and on heap
            ret = DoublesUtil.copyToHeap(other);
          }
        } else { //Not Empty: myQS has data, downsample to tmp
          final UpdateDoublesSketch tmp = DoublesSketch.builder().setK(other.getK()).build();

          DoublesMergeImpl.downSamplingMergeInto(myQS, tmp); //myData -> tmp
          ret = (myQS.hasMemorySegment())
              ? DoublesSketch.builder().setK(other.getK()).build(myQS.getMemorySegment())
              : DoublesSketch.builder().setK(other.getK()).build();

          DoublesMergeImpl.mergeInto(tmp, ret);
          DoublesMergeImpl.mergeInto(other, ret);
        }
        break;
      }
      case 4: { //myQS = null,  other = empty; create empty-heap(myMaxK)
        ret = HeapUpdateDoublesSketch.newInstance(myMaxK);
        break;
      }
      default: break; //This cannot happen
    }
    return ret;
  }
  //@formatter:on

}

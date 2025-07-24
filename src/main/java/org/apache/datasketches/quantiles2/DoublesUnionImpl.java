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

import static org.apache.datasketches.quantiles2.DoublesUtil.copyToHeap;

import java.lang.foreign.MemorySegment;
import java.util.Objects;

/**
 * Union operation for on-heap.
 *
 * @author Lee Rhodes
 * @author Kevin Lang
 */
final class DoublesUnionImpl extends DoublesUnionImplR {

  private DoublesUnionImpl(final int maxK) {
    super(maxK);
  }

  /**
   * Returns a empty Heap DoublesUnion object.
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
   * Returns a empty DoublesUnion object that refers to the given direct, off-heap MemorySegment,
   * which will be initialized to the empty state.
   *
   * @param maxK determines the accuracy and size of the union and is a maximum.
   * The effective <i>k</i> can be smaller due to unions with smaller <i>k</i> sketches.
   * It is recommended that <i>maxK</i> be a power of 2 to enable unioning of sketches with
   * different <i>k</i>.
   * @param dstMem the MemorySegment to be used by the sketch
   * @return a DoublesUnion object
   */
  static DoublesUnionImpl directInstance(final int maxK, final MemorySegment dstMem) {
    Objects.requireNonNull(dstMem);
    final DirectUpdateDoublesSketch sketch = DirectUpdateDoublesSketch.newInstance(maxK, dstMem);
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
   * MemorySegment image of a DoublesSketch. The srcMem object will not be modified and a reference to
   * it is not retained. The <i>maxK</i> of the resulting union will be that obtained from
   * the sketch MemorySegment image.
   *
   * @param srcMem a MemorySegment image of a quantiles DoublesSketch
   * @return a DoublesUnion object
   */
  static DoublesUnionImpl heapifyInstance(final MemorySegment srcMem) {
    Objects.requireNonNull(srcMem);
    final HeapUpdateDoublesSketch sketch = HeapUpdateDoublesSketch.heapifyInstance(srcMem);
    final DoublesUnionImpl union = new DoublesUnionImpl(sketch.getK());
    union.gadget_ = sketch;
    return union;
  }

  /**
   * Returns an updatable Union object that wraps off-heap data structure of the given MemorySegment
   * image of a non-compact DoublesSketch. The data structures of the Union remain off-heap.
   *
   * @param mem A MemorySegment image of a non-compact DoublesSketch to be used as the data
   * structure for the union and will be modified.
   * @return a Union object
   */
  static DoublesUnionImpl wrapInstance(final MemorySegment mem) {
    Objects.requireNonNull(mem);
    final DirectUpdateDoublesSketch sketch = DirectUpdateDoublesSketch.wrapInstance(mem);
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
  public void union(final MemorySegment mem) {
    Objects.requireNonNull(mem);
    gadget_ = updateLogic(maxK_, gadget_, DoublesSketch.wrap(mem));
    gadget_.doublesSV = null;
  }

  @Override
  public void update(final double quantile) {
    if (gadget_ == null) {
      gadget_ = HeapUpdateDoublesSketch.newInstance(maxK_);
    }
    gadget_.update(quantile);
    gadget_.doublesSV = null;
  }

  @Override
  public UpdateDoublesSketch getResultAndReset() {
    if (gadget_ == null) { return null; } //Intentionally return null here for speed.
    final UpdateDoublesSketch ds = gadget_;
    gadget_ = null;
    return ds;
  }

  @Override
  public void reset() {
    gadget_ = null;
  }

  //@formatter:off
  static UpdateDoublesSketch updateLogic(final int myMaxK, final UpdateDoublesSketch myQS,
                                         final DoublesSketch other) {
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
          final DoublesSketchAccessor otherAccessor = DoublesSketchAccessor.wrap(other);
          for (int i = 0; i < otherAccessor.numItems(); ++i) {
            ret.update(otherAccessor.get(i));
          }
        }
        else { //myQS = null, other is est mode
          ret = (myMaxK < other.getK())
              ? other.downSampleInternal(other, myMaxK, null) //null mem
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
          final DoublesSketchAccessor otherAccessor = DoublesSketchAccessor.wrap(other);
          for (int i = 0; i < otherAccessor.numItems(); ++i) {
            ret.update(otherAccessor.get(i));
          }
        } else if (myQS.getK() <= other.getK()) { //I am smaller or equal, thus the target
          DoublesMergeImpl.mergeInto(other, myQS);
          ret = myQS;
        } else if (myQS.isEmpty()) {
          if (myQS.hasMemorySegment()) {
            final MemorySegment mem = myQS.getMemorySegment(); //myQS is empty, ok to reconfigure
            other.putMemory(mem, false); // not compact, but BB ordered
            ret = DirectUpdateDoublesSketch.wrapInstance(mem);
          } else { //myQS is empty and on heap
            ret = DoublesUtil.copyToHeap(other);
          }
        }
        else { //Not Empty: myQS has data, downsample to tmp
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

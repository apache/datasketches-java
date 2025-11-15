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

import java.lang.foreign.MemorySegment;
import java.util.Comparator;
import java.util.Objects;

import org.apache.datasketches.common.ArrayOfItemsSerDe;

/**
 * The API for Union operations for generic QuantilesItemsSketches
 *
 * @param <T> The sketch item data type.
 *
 * @author Lee Rhodes
 * @author Alexander Saydakov
 */
public final class QuantilesItemsUnion<T> {

  final int maxK_;
  final Comparator<? super T> comparator_;
  QuantilesItemsSketch<T> gadget_;
  Class<T> clazz_;

  private QuantilesItemsUnion(
      final int maxK,
      final Comparator<? super T> comparator,
      final QuantilesItemsSketch<T> gadget) {
    Objects.requireNonNull(gadget, "Gadget sketch must not be null.");
    Objects.requireNonNull(comparator, "Comparator must not be null.");
    maxK_ = maxK;
    comparator_ = comparator;
    gadget_ = gadget;
    clazz_ = gadget.clazz;
    gadget_.classicQisSV = null;
  }

  /**
   * Create an instance of QuantilesItemsUnion with the default k.
   * @param <T> The sketch item data type.
   * @param clazz The sketch class type.
   * @param comparator to compare items.
   * @return a new instance of QuantilesItemsUnion
   */
  public static <T> QuantilesItemsUnion<T> getInstance(
      final Class<T> clazz,
      final Comparator<? super T> comparator) {
    final QuantilesItemsSketch<T> emptySk = QuantilesItemsSketch.getInstance(clazz, comparator);
    return new QuantilesItemsUnion<>(PreambleUtil.DEFAULT_K, comparator, emptySk);
  }

  /**
   * Create an instance of QuantilesItemsUnion
   * @param <T> The sketch item data type.
   * @param clazz The sketch class type.
   * @param maxK determines the accuracy and size of the union and is a maximum.
   * The effective <i>k</i> can be smaller due to unions with smaller <i>k</i> sketches.
   * <i>maxK</i> must be a power of 2 to enable unioning of sketches with a different <i>k</i>.
   * @param comparator to compare items.
   * @return an new instance of QuantilesItemsUnion
   */
  public static <T> QuantilesItemsUnion<T> getInstance(
      final Class<T> clazz,
      final int maxK,
      final Comparator<? super T> comparator) {
    final QuantilesItemsSketch<T> emptySk = QuantilesItemsSketch.getInstance(clazz, maxK, comparator);
    return new QuantilesItemsUnion<>(maxK, comparator, emptySk);
  }

  /**
   * Initialize a new QuantilesItemsUnion with a heapified instance of an QuantilesItemsSketch from a MemorySegment.
   * @param <T> The sketch data type.
   * @param clazz The sketch class type.
   * @param srcSeg the given srcSeg, an image of an QuantilesItemsSketch. A reference to srcSeg will not be maintained internally.
   * @param comparator to compare items.
   * @param serDe an instance of ArrayOfItemsSerDe.
   * @return an QuantilesItemsUnion initialized with a heapified QuantilesItemsSketch from a MemorySegment.
   */
  public static <T> QuantilesItemsUnion<T> initializeWithMemorySegment(
      final Class<T> clazz,
      final MemorySegment srcSeg,
      final Comparator<? super T> comparator,
      final ArrayOfItemsSerDe<T> serDe) {
    final QuantilesItemsSketch<T> gadget = QuantilesItemsSketch.heapify(clazz, srcSeg, comparator, serDe);
    return new QuantilesItemsUnion<>(gadget.getK(), gadget.getComparator(), gadget);
  }

  /**
   * Initialize a new QuantilesItemsUnion with an instance of QuantilesItemsSketch
   * @param <T> The sketch data type
   * @param sketch an instance of QuantilesItemsSketch to initialize this union
   * @return an initialized instance of QuantilesItemsUnion
   */
  public static <T> QuantilesItemsUnion<T> initialize(final QuantilesItemsSketch<T> sketch) {
    return new QuantilesItemsUnion<>(sketch.getK(), sketch.getComparator(), QuantilesItemsSketch.copy(sketch));
  }

  /**
   * Iterative union operation, which means this method can be repeatedly called.
   * Merges the given sketch into this union object.
   * The given sketch is not modified.
   * It is required that the ratio of the two K's be a power of 2.
   * This is easily satisfied if each of the K's are already a power of 2.
   * If the given sketch is null or empty it is ignored.
   *
   * <p>It is required that the results of the union operation, which can be obtained at any time,
   * is obtained from {@link #getResult() }.</p>
   *
   * @param sketchIn the sketch to be merged into this one.
   */
  public void union(final QuantilesItemsSketch<T> sketchIn) {
    gadget_ = updateLogic(maxK_, comparator_, gadget_, sketchIn);
  }

  /**
   * Iterative union operation, which means this method can be repeatedly called.
   * Merges the given MemorySegment image of a QuantilesItemsSketch into this union object.
   * The given MemorySegment object is not modified and a link to it is not retained.
   * It is required that the ratio of the two K's be a power of 2.
   * This is easily satisfied if each of the K's are already a power of 2.
   * If the given sketch is null or empty it is ignored.
   *
   * <p>It is required that the results of the union operation, which can be obtained at any time,
   * is obtained from {@link #getResult() }.</p>
   * @param srcSeg MemorySegment image of sketch to be merged
   * @param serDe an instance of ArrayOfItemsSerDe
   */
  public void union(
      final MemorySegment srcSeg,
      final ArrayOfItemsSerDe<T> serDe) {
    final QuantilesItemsSketch<T> that = QuantilesItemsSketch.heapify(clazz_, srcSeg, comparator_, serDe);
    gadget_ = updateLogic(maxK_, comparator_, gadget_, that);
  }

  /**
   * Update this union with the given dataItem.
   *
   * @param dataItem The given datum.
   */
  public void update(final T dataItem) {
    if (dataItem == null) { return; }
    if (gadget_ == null) {
      gadget_ = QuantilesItemsSketch.getInstance(clazz_, maxK_, comparator_);
    }
    gadget_.update(dataItem);
  }

  /**
   * Gets the result of this Union operation as a copy of the internal state.
   * This enables further union update operations on this state.
   * @return the result of this Union operation
   */
  public QuantilesItemsSketch<T> getResult() {
    if (gadget_ == null) {
      return QuantilesItemsSketch.getInstance(clazz_, maxK_, comparator_);
    }
    return QuantilesItemsSketch.copy(gadget_); //can't have any externally owned handles.
  }

  /**
   * Gets the sketch result of this Union operation and resets this Union to the virgin state.
   *
   * @return the result of this Union operation and reset.
   */
  public QuantilesItemsSketch<T> getResultAndReset() {
    if (gadget_ == null) { return null; } //Intentionally return null here for speed.
    return gadget_.getSketchAndReset();
  }

  /**
   * Resets this Union to a virgin state.
   * Keeps maxK, comparator and clazz
   */
  public void reset() {
    gadget_ = null;
  }

  /**
   * Returns true if this union is empty
   * @return true if this union is empty
   */
  public boolean isEmpty() {
    return (gadget_ == null) || gadget_.isEmpty();
  }

  /**
   * Returns the configured <i>maxK</i> of this Union.
   * @return the configured <i>maxK</i> of this Union.
   */
  public int getMaxK() {
    return maxK_;
  }

  /**
   * Returns the effective <i>k</i> of this Union.
   * @return the effective <i>k</i> of this Union.
   */
  public int getEffectiveK() {
    return (gadget_ != null) ? gadget_.getK() : maxK_;
  }

  /**
   * Returns summary information about the backing sketch.
   */
  @Override
  public String toString() {
    return toString(true, false);
  }

  /**
   * Returns summary information about the backing sketch. Used for debugging.
   * @param sketchSummary if true includes sketch summary
   * @param dataDetail if true includes data detail
   * @return summary information about the sketch.
   */
  public String toString(final boolean sketchSummary, final boolean dataDetail) {
    final StringBuilder sb = new StringBuilder();
    final String thisSimpleName = this.getClass().getSimpleName();
    final int maxK = this.getMaxK();
    final String kStr = String.format("%,d", maxK);
    sb.append(LS).append("### Quantiles ").append(thisSimpleName).append(LS);
    sb.append("   maxK                         : ").append(kStr);
    if (gadget_ == null) {
      sb.append(QuantilesItemsSketch.getInstance(clazz_, maxK_, comparator_).toString());
      return sb.toString();
    }
    sb.append(gadget_.toString(sketchSummary, dataDetail));
    return sb.toString();
  }

  /**
   * Serialize this union to a byte array. Result is an QuantilesItemsSketch, serialized in an
   * unordered, non-compact form. The resulting byte[] can be passed to getInstance for either a
   * sketch or union.
   *
   * @param serDe an instance of ArrayOfItemsSerDe
   * @return byte array of this union
   */
  public byte[] toByteArray(final ArrayOfItemsSerDe<T> serDe) {
    if (gadget_ == null) {
      final QuantilesItemsSketch<T> sketch = QuantilesItemsSketch.getInstance(clazz_, maxK_, comparator_);
      return sketch.toByteArray(serDe);
    }
    return gadget_.toByteArray(serDe);
  }

  //@formatter:off
  @SuppressWarnings("unchecked")
  static <T> QuantilesItemsSketch<T> updateLogic(final int myMaxK, final Comparator<? super T> comparator,
      final QuantilesItemsSketch<T> myQS, final QuantilesItemsSketch<T> other) {
    int sw1 = ((myQS   == null) ? 0 :   myQS.isEmpty() ? 4 : 8);
    sw1 |=    ((other  == null) ? 0 :  other.isEmpty() ? 1 : 2);
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
    QuantilesItemsSketch<T> ret = null;

    switch (outCase) {
      case 0: break;
      case 1: ret = myQS; break;
      case 2: { //myQS = null,  other = valid; stream or downsample to myMaxK
        assert other != null;
        if (!other.isEstimationMode()) { //other is exact, stream items in
          ret = QuantilesItemsSketch.getInstance(other.getClassOfT(), myMaxK, comparator);
          final int otherCnt = other.getBaseBufferCount();
          final Object[] combBuf = other.getCombinedBuffer();
          for (int i = 0; i < otherCnt; i++) {
            ret.update((T) combBuf[i]);
          }
        }
        else { //myQS = null, other is est mode
          ret = (myMaxK < other.getK())
              ? other.downSample(myMaxK)
              : QuantilesItemsSketch.copy(other); //required because caller has handle
        }
        break;
      }
      case 3: { //myQS = empty/valid, other = valid; merge
        assert other != null;
        assert myQS != null;
        if (!other.isEstimationMode()) { //other is exact, stream items in
          ret = myQS;
          final int otherCnt = other.getBaseBufferCount();
          final Object[] combBuf = other.getCombinedBuffer();
          for (int i = 0; i < otherCnt; i++) {
            ret.update((T) combBuf[i]);
          }
        } else if (myQS.getK() <= other.getK()) { //I am smaller or equal, thus the target
          ItemsMergeImpl.mergeInto(other, myQS);
          ret = myQS;
        }
        else { //Bigger: myQS.getK() > other.getK(), must reverse roles
          //must copy other as it will become mine and can't have any externally owned handles.
          ret = QuantilesItemsSketch.copy(other);
          ItemsMergeImpl.mergeInto(myQS, ret);
        }
        break;
      }
      case 4: {
        assert other != null;
        ret = QuantilesItemsSketch.getInstance(other.getClassOfT(), Math.min(myMaxK, other.getK()), comparator);
        break;
      }
      default: break; //This cannot happen
    }
    return ret;
  }
  //@formatter:on

}

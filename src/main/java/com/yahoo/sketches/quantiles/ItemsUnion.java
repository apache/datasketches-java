/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.Util.LS;

import java.util.Comparator;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.ArrayOfItemsSerDe;

/**
 * The API for Union operations for generic ItemsSketches
 *
 * @param <T> type of item
 *
 * @author Lee Rhodes
 * @author Alexander Saydakov
 */
public final class ItemsUnion<T> {

  protected final int maxK_;
  protected final Comparator<? super T> comparator_;
  protected ItemsSketch<T> gadget_;

  private ItemsUnion(final int maxK, final Comparator<? super T> comparator,
      final ItemsSketch<T> gadget) {
    maxK_ = maxK;
    comparator_ = comparator;
    gadget_ = gadget;
  }

  /**
   * Create an instance of ItemsUnion with the default k
   * @param <T> type of item
   * @param comparator to compare items
   * @return an instance of ItemsUnion
   */
  public static <T> ItemsUnion<T> getInstance(final Comparator<? super T> comparator) {
    return new ItemsUnion<T>(PreambleUtil.DEFAULT_K, comparator, null);
  }

  /**
   * Create an instance of ItemsUnion
   * @param <T> type of item
   * @param maxK determines the accuracy and size of the union and is a maximum value.
   * The effective <i>k</i> can be smaller due to unions with smaller <i>k</i> sketches.
   * It is recommended that <i>maxK</i> be a power of 2 to enable unioning of sketches with
   * different values of <i>k</i>.
   * @param comparator to compare items
   * @return an instance of ItemsUnion
   */
  public static <T> ItemsUnion<T> getInstance(final int maxK, final Comparator<? super T> comparator) {
    return new ItemsUnion<>(maxK, comparator, null);
  }

  /**
   * Heapify the given srcMem into a Union object.
   * @param <T> type of item
   * @param srcMem the given srcMem.
   * A reference to srcMem will not be maintained internally.
   * @param comparator to compare items
   * @param serDe an instance of ArrayOfItemsSerDe
   * @return an instance of ItemsUnion
   */
  public static <T> ItemsUnion<T> getInstance(final Memory srcMem,
      final Comparator<? super T> comparator, final ArrayOfItemsSerDe<T> serDe) {
    final ItemsSketch<T> gadget = ItemsSketch.getInstance(srcMem, comparator, serDe);
    return new ItemsUnion<>(gadget.getK(), gadget.getComparator(), gadget);
  }

  /**
   * Create an instance of ItemsUnion based on ItemsSketch
   * @param <T> type of item
   * @param sketch the basis of the union
   * @return an instance of ItemsUnion
   */
  public static <T> ItemsUnion<T> getInstance(final ItemsSketch<T> sketch) {
    return new ItemsUnion<>(sketch.getK(), sketch.getComparator(), ItemsSketch.copy(sketch));
  }

  /**
   * Iterative union operation, which means this method can be repeatedly called.
   * Merges the given sketch into this union object.
   * The given sketch is not modified.
   * It is required that the ratio of the two K values be a power of 2.
   * This is easily satisfied if each of the K values is already a power of 2.
   * If the given sketch is null or empty it is ignored.
   *
   * <p>It is required that the results of the union operation, which can be obtained at any time,
   * is obtained from {@link #getResult() }.</p>
   *
   * @param sketchIn the sketch to be merged into this one.
   */
  public void update(final ItemsSketch<T> sketchIn) {
    gadget_ = updateLogic(maxK_, comparator_, gadget_, sketchIn);
  }

  /**
   * Iterative union operation, which means this method can be repeatedly called.
   * Merges the given Memory image of a ItemsSketch into this union object.
   * The given Memory object is not modified and a link to it is not retained.
   * It is required that the ratio of the two K values be a power of 2.
   * This is easily satisfied if each of the K values is already a power of 2.
   * If the given sketch is null or empty it is ignored.
   *
   * <p>It is required that the results of the union operation, which can be obtained at any time,
   * is obtained from {@link #getResult() }.</p>
   * @param srcMem Memory image of sketch to be merged
   * @param serDe an instance of ArrayOfItemsSerDe
   */
  public void update(final Memory srcMem, final ArrayOfItemsSerDe<T> serDe) {
    final ItemsSketch<T> that = ItemsSketch.getInstance(srcMem, comparator_, serDe);
    gadget_ = updateLogic(maxK_, comparator_, gadget_, that);
  }

  /**
   * Update this union with the given double (or float) data Item.
   *
   * @param dataItem The given datum.
   */
  public void update(final T dataItem) {
    if (dataItem == null) { return; }
    if (gadget_ == null) {
      gadget_ = ItemsSketch.getInstance(maxK_, comparator_);
    }
    gadget_.update(dataItem);
  }

  /**
   * Gets the result of this Union operation as a copy of the internal state.
   * This enables further union update operations on this state.
   * @return the result of this Union operation
   */
  public ItemsSketch<T> getResult() {
    if (gadget_ == null) {
      return ItemsSketch.getInstance(maxK_, comparator_);
    }
    return ItemsSketch.copy(gadget_); //can't have any externally owned handles.
  }

  /**
   * Gets the result of this Union operation (without a copy) and resets this Union to the
   * virgin state.
   *
   * @return the result of this Union operation and reset.
   */
  public ItemsSketch<T> getResultAndReset() {
    if (gadget_ == null) { return null; } //Intentionally return null here for speed.
    final ItemsSketch<T> hqs = gadget_;
    gadget_ = null;
    return hqs;
  }

  /**
   * Resets this Union to a virgin state.
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
   * Returns true if this union is direct
   * @return true if this union is direct
   */
  public boolean isDirect() {
    return (gadget_ != null) && gadget_.isDirect();
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
    sb.append(Util.LS).append("### Quantiles ").append(thisSimpleName).append(LS);
    sb.append("   maxK                         : ").append(kStr);
    if (gadget_ == null) {
      sb.append(ItemsSketch.getInstance(maxK_, comparator_).toString());
      return sb.toString();
    }
    sb.append(gadget_.toString(sketchSummary, dataDetail));
    return sb.toString();
  }

  /**
   * Serialize this union to a byte array. Result is an ItemsSketch, serialized in an
   * unordered, non-compact form. The resulting byte[] can be passed to getInstance for either a
   * sketch or union.
   *
   * @param serDe an instance of ArrayOfItemsSerDe
   * @return byte array of this union
   */
  public byte[] toByteArray(final ArrayOfItemsSerDe<T> serDe) {
    if (gadget_ == null) {
      final ItemsSketch<T> sketch = ItemsSketch.getInstance(maxK_, comparator_);
      return sketch.toByteArray(serDe);
    }
    return gadget_.toByteArray(serDe);
  }

  //@formatter:off
  @SuppressWarnings({"null", "unchecked"})
  static <T> ItemsSketch<T> updateLogic(final int myMaxK, final Comparator<? super T> comparator,
      final ItemsSketch<T> myQS, final ItemsSketch<T> other) {
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
      //default: //This cannot happen and cannot be tested
    }
    ItemsSketch<T> ret = null;
    switch (outCase) {
      case 0: ret = null; break;
      case 1: ret = myQS; break;
      case 2: { //myQS = null,  other = valid; stream or downsample to myMaxK
        assert other != null;
        if (!other.isEstimationMode()) { //other is exact, stream items in
          ret = ItemsSketch.getInstance(myMaxK, comparator);
          final int otherCnt = other.getBaseBufferCount();
          final Object[] combBuf = other.getCombinedBuffer();
          for (int i = 0; i < otherCnt; i++) {
            ret.update((T) combBuf[i]);
          }
        }
        else { //myQS = null, other is est mode
          ret = (myMaxK < other.getK())
              ? other.downSample(myMaxK)
              : ItemsSketch.copy(other); //required because caller has handle
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
        }
        else { //myQS = empty/valid, other = valid and in est mode
          if (myQS.getK() <= other.getK()) { //I am smaller or equal, thus the target
            ItemsMergeImpl.mergeInto(other, myQS);
            ret = myQS;
          }
          else { //Bigger: myQS.getK() > other.getK(), must reverse roles
            //must copy other as it will become mine and can't have any externally owned handles.
            ret = ItemsSketch.copy(other);
            ItemsMergeImpl.mergeInto(myQS, ret);
          }
        }
        break;
      }
      case 4: {
        assert other != null;
        ret = ItemsSketch.getInstance(Math.min(myMaxK, other.getK()), comparator);
        break;
      }
      //default: //This cannot happen and cannot be tested
    }
    return ret;
  }
  //@formatter:on

}

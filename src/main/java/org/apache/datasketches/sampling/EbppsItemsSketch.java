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

package org.apache.datasketches.sampling;

import static org.apache.datasketches.sampling.PreambleUtil.EBPPS_SER_VER;
import static org.apache.datasketches.sampling.PreambleUtil.EMPTY_FLAG_MASK;
import static org.apache.datasketches.sampling.PreambleUtil.HAS_PARTIAL_ITEM_MASK;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.datasketches.common.ArrayOfItemsSerDe;
import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

/**
 * An implementation of an Exact and Bounded Sampling Proportional to Size sketch.
 * 
 * <p>From: "Exact PPS Sampling with Bounded Sample Size",
 * B. Hentschel, P. J. Haas, Y. Tian. Information Processing Letters, 2023.
 * 
 * <p>This sketch samples data from a stream of items proportional to the weight of each item.
 * The sample guarantees the presence of an item in the result is proportional to that item's
 * portion of the total weight seen by the sketch, and returns a sample no larger than size k.
 * 
 * <p>The sample may be smaller than k and the resulting size of the sample potentially includes
 * a probabilistic component, meaning the resulting sample size is not always constant.
 *
 * @author Jon Malkin
 */
public class EbppsItemsSketch<T> {
  private static final int MAX_K = Integer.MAX_VALUE - 2;
  private static final int EBPPS_C_DOUBLE        = 40; // part of sample state, not preamble
  private static final int EBPPS_ITEMS_START     = 48;

  private int k_;                      // max size of sketch, in items
  private long n_;                     // total number of items processed by the sketch

  private double cumulativeWt_;        // total weight of items processed by the sketch
  private double wtMax_;               // maximum weight seen so far
  private double rho_;                 // latest scaling parameter for downsampling

  private EbppsItemsSample<T> sample_; // Object holding the current state of the sample

  final private EbppsItemsSample<T> tmp_;    // temporary storage

  /**
   * Constructor
   * @param k The maximum number of samples to retain
   */
  public EbppsItemsSketch(final int k) {
    checkK(k);
    k_ = k;
    rho_ = 1.0;
    sample_ = new EbppsItemsSample<>(k);
    tmp_ = new EbppsItemsSample<>(1);
  }

  // private copy constructor
  private EbppsItemsSketch(final EbppsItemsSketch<T> other) {
    k_ = other.k_;
    n_ = other.n_;
    rho_ = other.rho_;
    cumulativeWt_ = other.cumulativeWt_;
    wtMax_ = other.wtMax_;
    sample_ = new EbppsItemsSample<>(other.sample_);
    tmp_ = new EbppsItemsSample<>(1);
  }

  // private constructor for heapify
  private EbppsItemsSketch(final EbppsItemsSample<T> sample,
                           final int k,
                           final long n,
                           final double cumWt,
                           final double maxWt,
                           final double rho) {
    k_ = k;
    n_ = n;
    cumulativeWt_ = cumWt;
    wtMax_ = maxWt;
    rho_ = rho;
    sample_ = sample;
    tmp_ = new EbppsItemsSample<>(1);
  }

  /**
   * Returns a sketch instance of this class from the given srcMem,
   * which must be a Memory representation of this sketch class.
   *
   * @param <T>    The type of item this sketch contains
   * @param srcMem a Memory representation of a sketch of this class.
   *               <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param serDe  An instance of ArrayOfItemsSerDe
   * @return a sketch instance of this class
   */
  public static <T> EbppsItemsSketch<T> heapify(final Memory srcMem,
                                                final ArrayOfItemsSerDe<T> serDe)
  {
    final int numPreLongs = PreambleUtil.getAndCheckPreLongs(srcMem);
    final int serVer = PreambleUtil.extractSerVer(srcMem);
    final int familyId = PreambleUtil.extractFamilyID(srcMem);
    final int flags = PreambleUtil.extractFlags(srcMem);
    final boolean isEmpty = (flags & EMPTY_FLAG_MASK) != 0;
    final boolean hasPartialItem = (flags & HAS_PARTIAL_ITEM_MASK) != 0;

    // Check values
    if (isEmpty) {
      if (numPreLongs != Family.EBPPS.getMinPreLongs()) {
        throw new SketchesArgumentException("Possible corruption: Must be " + Family.EBPPS.getMinPreLongs()
                + " for an empty sketch. Found: " + numPreLongs);
      }
    } else {
      if (numPreLongs != Family.EBPPS.getMaxPreLongs()) {
        throw new SketchesArgumentException("Possible corruption: Must be "
                + Family.EBPPS.getMaxPreLongs() + " for a non-empty sketch. Found: " + numPreLongs);
      }
    }
    if (serVer != EBPPS_SER_VER) {
        throw new SketchesArgumentException(
                "Possible Corruption: Ser Ver must be " + EBPPS_SER_VER + ": " + serVer);
    }
    final int reqFamilyId = Family.EBPPS.getID();
    if (familyId != reqFamilyId) {
      throw new SketchesArgumentException(
              "Possible Corruption: FamilyID must be " + reqFamilyId + ": " + familyId);
    }

    final int k = PreambleUtil.extractK(srcMem);
    if (k < 1 || k > MAX_K) {
      throw new SketchesArgumentException("Possible Corruption: k must be at least 1 "
              + "and less than " + MAX_K + ". Found: " + k);
    }

    if (isEmpty) {
      return new EbppsItemsSketch<>(k);
    }

    final long n = PreambleUtil.extractN(srcMem);
    if (n < 0) {
      throw new SketchesArgumentException("Possible Corruption: n cannot be negative: " + n);
    }

    final double cumWt = PreambleUtil.extractEbppsCumulativeWeight(srcMem);
    if (cumWt < 0.0 || Double.isNaN(cumWt) || Double.isInfinite(cumWt)) {
      throw new SketchesArgumentException("Possible Corruption: cumWt must be nonnegative and finite: " + cumWt);
    }

    final double maxWt = PreambleUtil.extractEbppsMaxWeight(srcMem);
    if (maxWt < 0.0 || Double.isNaN(maxWt) || Double.isInfinite(maxWt)) {
      throw new SketchesArgumentException("Possible Corruption: maxWt must be nonnegative and finite: " + maxWt);
    }

    final double rho = PreambleUtil.extractEbppsRho(srcMem);
    if (rho < 0.0 || rho > 1.0 ||  Double.isNaN(rho) || Double.isInfinite(rho)) {
      throw new SketchesArgumentException("Possible Corruption: rho must be in [0.0, 1.0]: " + rho);
    }

    // extract C (part of sample_, not the preamble)
    // due to numeric precision issues, c may occasionally be very slightly larger than k
    final double c = srcMem.getDouble(EBPPS_C_DOUBLE);
    if (c < 0 || c >= (k + 1) || Double.isNaN(c) || Double.isInfinite(c)) {
      throw new SketchesArgumentException("Possible Corruption: c must be between 0 and k: " + c);
    }

    // extract items
    final int numTotalItems = (int) Math.ceil(c);
    final int numFullItems = (int) Math.floor(c); // floor() not strictly necessary
    final int offsetBytes = EBPPS_ITEMS_START;
    final T[] rawItems = serDe.deserializeFromMemory(
            srcMem.region(offsetBytes, srcMem.getCapacity() - offsetBytes), 0, numTotalItems);
    final List<T> itemsList = Arrays.asList(rawItems);
    final ArrayList<T> data;
    final T partialItem;
    if (hasPartialItem) {
      if (numFullItems >= numTotalItems) {
        throw new SketchesArgumentException("Possible Corruption: Expected partial item but none found");
      }

      data = new ArrayList<>(itemsList.subList(0, numFullItems));
      partialItem = itemsList.get(numFullItems); // 0-based, so last item
    } else {
      data = new ArrayList<>(itemsList);
      partialItem = null; // just to be explicit
    }

    final EbppsItemsSample<T> sample = new EbppsItemsSample<>(data, partialItem, c);

    return new EbppsItemsSketch<>(sample, k, n, cumWt, maxWt, rho);
  }

  /**
   * Updates this sketch with the given data item with weight 1.0.
   * @param item an item from a stream of items
   */
  public void update(final T item) {
    update(item, 1.0);
  }

  /**
   * Updates this sketch with the given data item with the given weight.
   * @param item an item from a stream of items
   * @param weight the weight of the item
   */
  public void update(final T item, final double weight) {
    if (weight < 0.0 || Double.isNaN(weight) || Double.isInfinite(weight)) {
      throw new SketchesArgumentException("Item weights must be nonnegative and finite. "
        + "Found: " + weight);
    }
    if (weight == 0.0) {
      return;
    }

    final double newCumWt = cumulativeWt_ + weight;
    final double newWtMax = Math.max(wtMax_, weight);
    final double newRho = Math.min(1.0 / newWtMax, k_ / newCumWt);

    if (cumulativeWt_ > 0.0) {
      sample_.downsample((newRho / rho_));
    }

    tmp_.replaceContent(item, newRho * weight);
    sample_.merge(tmp_);

    cumulativeWt_ = newCumWt;
    wtMax_ = newWtMax;
    rho_ = newRho;
    ++n_;
  }

  /* Merging
   * There is a trivial merge algorithm that involves downsampling each sketch A and B
   * as A.cum_wt / (A.cum_wt + B.cum_wt) and B.cum_wt / (A.cum_wt + B.cum_wt),
   * respectively. That merge does preserve first-order probabilities, specifically
   * the probability proportional to size property, and like all other known merge
   * algorithms distorts second-order probabilities (co-occurrences). There are
   * pathological cases, most obvious with k=2 and A.cum_wt == B.cum_wt where that
   * approach will always take exactly 1 item from A and 1 from B, meaning the
   * co-occurrence rate for two items from either sketch is guaranteed to be 0.0.
   * 
   * With EBPPS, once an item is accepted into the sketch we no longer need to
   * track the item's weight: All accepted items are treated equally. As a result, we
   * can take inspiration from the reservoir sampling merge in the datasketches-java
   * library. We need to merge the smaller sketch into the larger one, swapping as
   * needed to ensure that, at which point we simply call update() with the items
   * in the smaller sketch as long as we adjust the weight appropriately.
   * Merging smaller into larger is essential to ensure that no item has a
   * contribution to C > 1.0.
   */

  /**
   * Merges the provided sketch into the current one.
   * @param other the sketch to merge into the current object
   */
  public void merge(final EbppsItemsSketch<T> other) {
    if (other.getCumulativeWeight() == 0.0) {
      return;
    } else if (other.getCumulativeWeight() > cumulativeWt_) {
      // need to swap this with other
      // make a copy of other, merge into it, and take the result
      final EbppsItemsSketch<T> copy = new EbppsItemsSketch<>(other);
      copy.internalMerge(this);
      k_ = copy.k_;
      n_ = copy.n_;
      cumulativeWt_ = copy.cumulativeWt_;
      wtMax_ = copy.wtMax_;
      rho_ = copy.rho_;
      sample_ = copy.sample_;
    } else {
      internalMerge(other);
    }
  }

  // merge implementation called exclusively from public merge()
  private void internalMerge(final EbppsItemsSketch<T> other) {
    // assumes that other.cumulativeWeight_ <= cumulativeWt_m
    // which must be checked before calling this

    final double finalCumWt = cumulativeWt_ + other.cumulativeWt_;
    final double newWtMax = Math.max(wtMax_, other.wtMax_);
    k_ = Math.min(k_, other.k_);
    final long newN = n_ + other.n_;

    // Insert other's items with the cumulative weight
    // split between the input items. We repeat the same process
    // for full items and the partial item, scaling the input
    // weight appropriately.
    // We handle all C input items, meaning we always process
    // the partial item using a scaled down weight.
    // Handling the partial item by probabilistically including
    // it as a full item would be correct on average but would
    // introduce bias for any specific merge operation.
    final double avgWt = other.cumulativeWt_ / other.getC();
    final ArrayList<T> items = other.sample_.getFullItems();
    if (items != null) {
      for (T item : items) {
        // newWtMax is pre-computed
        final double newCumWt = cumulativeWt_ + avgWt;
        final double newRho = Math.min(1.0 / newWtMax, k_ / newCumWt);

        if (cumulativeWt_ > 0.0) {
          sample_.downsample(newRho / rho_);
        }

        tmp_.replaceContent(item, newRho * avgWt);
        sample_.merge(tmp_);

        cumulativeWt_ = newCumWt;
        rho_ = newRho;
      }
    }

    // insert partial item with weight scaled by the fractional part of C
    if (other.sample_.hasPartialItem()) {
      final double otherCFrac = other.getC() % 1;
      final double newCumWt = cumulativeWt_ + (otherCFrac * avgWt);
      final double newRho = Math.min(1.0 / newWtMax, k_ / newCumWt);

      if (cumulativeWt_ > 0.0) {
        sample_.downsample(newRho / rho_);
      }
  
      tmp_.replaceContent(other.sample_.getPartialItem(), newRho * otherCFrac * avgWt);
      sample_.merge(tmp_);

      // cumulativeWt_ will be assigned momentarily
      rho_ = newRho;
    }

    // avoid numeric issues by setting cumulative weight to the
    // pre-computed value
    cumulativeWt_ = finalCumWt;
    n_ = newN;
  }

  /**
   * Returns a copy of the current sample. The exact size may be
   * probabilsitic, differing by at most 1 item.
   * @return the current sketch sample
   */
  public ArrayList<T> getResult() { return sample_.getSample(); }

  /**
   * Provides a human-readable summary of the sketch
   * @return a summary of information in the sketch
   */
  @Override
   public String toString() {
    return null;
  }

  /**
   * Returns the configured maximum sample size.
   * @return configured maximum sample size
   */
  public int getK() { return k_; }

  /**
   * Returns the number of items processed by the sketch, regardless
   * of item weight.
   * @return count of items processed by the sketch
   */
  public long getN() { return n_; }

  /**
   * Returns the cumulative weight of items processed by the sketch.
   * @return cumulative weight of items seen
   */
  public double getCumulativeWeight() { return cumulativeWt_; }

  /**
   * Returns the expected number of samples returned upon a call to
   * getResult(). The number is a floating point value, where the 
   * fractional portion represents the probability of including a
   * "partial item" from the sample.
   *
   * <p>The value C should be no larger than the sketch's configured
   * value of k, although numerical precision limitations mean it
   * may exceed k by double precision floating point error margins
   * in certain cases.
   * @return The expected number of samples returned when querying the sketch
   */
  public double getC() { return sample_.getC(); }

  /**
   * Returns true if the sketch is empty.
   * @return empty flag
   */
  public boolean isEmpty() { return n_ == 0; }

  /**
   * Resets the sketch to its default, empty state.
   */
  public void reset() {
    n_ = 0;
    cumulativeWt_ = 0.0;
    wtMax_ = 0.0;
    rho_ = 1.0;
    sample_ = new EbppsItemsSample<>(k_);
  }

  /**
   * Returns the size of a byte array representation of this sketch. May fail for polymorphic item types.
   *
   * @param serDe An instance of ArrayOfItemsSerDe
   * @return the length of a byte array representation of this sketch
   */
  public int getSerializedSizeBytes(final ArrayOfItemsSerDe<? super T> serDe) {
    if (isEmpty()) {
      return Family.EBPPS.getMinPreLongs() << 3;
    } else if (sample_.getC() < 1.0) {
      return getSerializedSizeBytes(serDe, sample_.getPartialItem().getClass());
    } else {
      return getSerializedSizeBytes(serDe, sample_.getSample().get(0).getClass());
    }
  }

  /**
   * Returns the length of a byte array representation of this sketch. Copies contents into an array of the
   * specified class for serialization to allow for polymorphic types.
   *
   * @param serDe An instance of ArrayOfItemsSerDe
   * @param clazz The class represented by &lt;T&gt;
   * @return the length of a byte array representation of this sketch
   */
  public int getSerializedSizeBytes(final ArrayOfItemsSerDe<? super T> serDe, final Class<?> clazz) {
    if (n_ == 0) {
      return Family.EBPPS.getMinPreLongs() << 3;
    }

    final int preLongs = Family.EBPPS.getMaxPreLongs();
    final byte[] itemBytes = serDe.serializeToByteArray(sample_.getAllSamples(clazz));
    // in C++, c_ is serialized as part of the sample_ and not included in the header size
    return (preLongs << 3) + Double.BYTES + itemBytes.length;
  }

  /**
   * Returns a byte array representation of this sketch. May fail for polymorphic item types.
   *
   * @param serDe An instance of ArrayOfItemsSerDe
   * @return a byte array representation of this sketch
   */
  public byte[] toByteArray(final ArrayOfItemsSerDe<? super T> serDe) {
    if (n_ == 0) {
      // null class is ok since empty -- no need to call serDe
      return toByteArray(serDe, null);
    } else if (sample_.getC() < 1.0) {
      return toByteArray(serDe, sample_.getPartialItem().getClass());
    } else {
      return toByteArray(serDe, sample_.getSample().get(0).getClass());
    }
  }

   /**
   * Returns a byte array representation of this sketch. Copies contents into an array of the
   * specified class for serialization to allow for polymorphic types.
   *
   * @param serDe An instance of ArrayOfItemsSerDe
   * @param clazz The class represented by &lt;T&gt;
   * @return a byte array representation of this sketch
   */
  public byte[] toByteArray(final ArrayOfItemsSerDe<? super T> serDe, final Class<?> clazz) {
    final int preLongs, outBytes;
    final boolean empty = n_ == 0;
    byte[] itemBytes = null; // for serialized items from sample_

    if (empty) {
      preLongs = 1;
      outBytes = 8;
    } else {
      preLongs = Family.EBPPS.getMaxPreLongs();
      itemBytes = serDe.serializeToByteArray(sample_.getAllSamples(clazz));
      // in C++, c_ is serialized as part of the sample_ and not included in the header size
      outBytes = (preLongs << 3) + Double.BYTES + itemBytes.length;
    }
    final byte[] outArr = new byte[outBytes];
    final WritableMemory mem = WritableMemory.writableWrap(outArr);

    // Common header elements
    PreambleUtil.insertPreLongs(mem, preLongs);              // Byte 0
    PreambleUtil.insertSerVer(mem, EBPPS_SER_VER);           // Byte 1
    PreambleUtil.insertFamilyID(mem, Family.EBPPS.getID());  // Byte 2
    if (empty) {
      PreambleUtil.insertFlags(mem, EMPTY_FLAG_MASK);        // Byte 3
    } else {
      PreambleUtil.insertFlags(mem, sample_.hasPartialItem() ? HAS_PARTIAL_ITEM_MASK : 0);
    }
    PreambleUtil.insertK(mem, k_);                           // Bytes 4-7
    
    // conditional elements
    if (!empty) {
      PreambleUtil.insertN(mem, n_);
      PreambleUtil.insertEbppsCumulativeWeight(mem, cumulativeWt_);
      PreambleUtil.insertEbppsMaxWeight(mem, wtMax_);
      PreambleUtil.insertEbppsRho(mem, rho_);
      
      // data from sample_ -- itemBytes includes the partial item
      mem.putDouble(EBPPS_C_DOUBLE, sample_.getC());
      mem.putByteArray(EBPPS_ITEMS_START, itemBytes, 0, itemBytes.length);
    }

    return outArr;
  }

  private static void checkK(final int k) {
    if (k <= 0 || k > MAX_K) {
      throw new SketchesArgumentException("k must be strictly positive and less than " + MAX_K);
    }
  }
}

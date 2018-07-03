/*
 * Copyright 2016-17, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.sampling;

import static com.yahoo.sketches.Util.LS;
import static com.yahoo.sketches.sampling.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.sampling.PreambleUtil.FAMILY_BYTE;
import static com.yahoo.sketches.sampling.PreambleUtil.SER_VER;
import static com.yahoo.sketches.sampling.PreambleUtil.extractEncodedReservoirSize;
import static com.yahoo.sketches.sampling.PreambleUtil.extractFlags;
import static com.yahoo.sketches.sampling.PreambleUtil.extractMaxK;
import static com.yahoo.sketches.sampling.PreambleUtil.extractPreLongs;
import static com.yahoo.sketches.sampling.PreambleUtil.extractSerVer;

import java.util.ArrayList;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.ArrayOfItemsSerDe;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.SketchesArgumentException;

/**
 * Class to union reservoir samples of generic items.
 *
 * <p>For efficiency reasons, the unioning process picks one of the two sketches to use as the
 * base. As a result, we provide only a stateful union. Using the same approach for a merge would
 * result in unpredictable side effects on the underlying sketches.</p>
 *
 * <p>A union object is created with a maximum value of <tt>k</tt>, represented using the
 * ReservoirSize class. The unioning process may cause the actual number of samples to fall below
 * that maximum value, but never to exceed it. The result of a union will be a reservoir where
 * each item from the global input has a uniform probability of selection, but there are no
 * claims about higher order statistics. For instance, in general all possible permutations of
 * the global input are not equally likely.</p>
 *
 * <p>If taking the union of two reservoirs of different sizes, the output sample will contain no more
 * than MIN(k_1, k_2) samples.</p>
 *
 * @param <T> The specific Java type for this sketch
 *
 * @author Jon Malkin
 * @author Kevin Lang
 */
public final class ReservoirItemsUnion<T> {
  private ReservoirItemsSketch<T> gadget_;
  private final int maxK_;

  /**
   * Empty constructor using ReservoirSize-encoded maxK value
   *
   * @param maxK Maximum allowed reservoir capacity for this union
   */
  private ReservoirItemsUnion(final int maxK) {
    maxK_ = maxK;
  }

  /**
   * Creates an empty Union with a maximum reservoir capacity of size k.
   *
   * @param <T> The type of item this sketch contains
   * @param maxK The maximum allowed reservoir capacity for any sketches in the union
   * @return A new ReservoirItemsUnion
   */
  public static <T> ReservoirItemsUnion<T> newInstance(final int maxK) {
    return new ReservoirItemsUnion<>(maxK);
  }

  /**
   * Instantiates a Union from Memory
   *
   * @param <T> The type of item this sketch contains
   * @param srcMem Memory object containing a serialized union
   * @param serDe An instance of ArrayOfItemsSerDe
   * @return A ReservoirItemsUnion created from the provided Memory
   */
  public static <T> ReservoirItemsUnion<T> heapify(final Memory srcMem,
                                                   final ArrayOfItemsSerDe<T> serDe) {
    Family.RESERVOIR_UNION.checkFamilyID(srcMem.getByte(FAMILY_BYTE));

    final int numPreLongs = extractPreLongs(srcMem);
    final int serVer = extractSerVer(srcMem);
    final boolean isEmpty = (extractFlags(srcMem) & EMPTY_FLAG_MASK) != 0;
    int maxK = extractMaxK(srcMem);

    final boolean preLongsEqMin = (numPreLongs == Family.RESERVOIR_UNION.getMinPreLongs());
    final boolean preLongsEqMax = (numPreLongs == Family.RESERVOIR_UNION.getMaxPreLongs());

    if (!preLongsEqMin & !preLongsEqMax) {
      throw new SketchesArgumentException("Possible corruption: Non-empty union with only "
          + Family.RESERVOIR_UNION.getMinPreLongs() + "preLongs");
    }

    if (serVer != SER_VER) {
      if (serVer == 1) {
        final short encMaxK = extractEncodedReservoirSize(srcMem);
        maxK = ReservoirSize.decodeValue(encMaxK);
      } else {
        throw new SketchesArgumentException(
                "Possible Corruption: Ser Ver must be " + SER_VER + ": " + serVer);
      }
    }

    final ReservoirItemsUnion<T> riu = new ReservoirItemsUnion<>(maxK);

    if (!isEmpty) {
      final int preLongBytes = numPreLongs << 3;
      final Memory sketchMem =
              srcMem.region(preLongBytes, srcMem.getCapacity() - preLongBytes);
      riu.update(sketchMem, serDe);
    }

    return riu;
  }

  /**
   * Returns the maximum allowed reservoir capacity in this union. The current reservoir capacity
   * may be lower.
   *
   * @return The maximum allowed reservoir capacity in this union.
   */
  public int getMaxK() { return maxK_; }

  /**
   * Union the given sketch. This method can be repeatedly called. If the given sketch is null it is
   * interpreted as an empty sketch.
   *
   * @param sketchIn The incoming sketch.
   */
  public void update(final ReservoirItemsSketch<T> sketchIn) {
    if (sketchIn == null) {
      return;
    }

    final ReservoirItemsSketch<T> ris =
        (sketchIn.getK() <= maxK_ ? sketchIn : sketchIn.downsampledCopy(maxK_));

    // can modify the sketch if we downsampled, otherwise may need to copy it
    final boolean isModifiable = (sketchIn != ris);
    if (gadget_ == null) {
      createNewGadget(ris, isModifiable);
    } else {
      twoWayMergeInternal(ris, isModifiable);
    }
  }

  /**
   * Union the given Memory image of the sketch.
   *
   *<p>This method can be repeatedly called. If the given sketch is null it is interpreted as an
   * empty sketch.</p>
   *
   * @param mem Memory image of sketch to be merged
   * @param serDe An instance of ArrayOfItemsSerDe
   */
  public void update(final Memory mem, final ArrayOfItemsSerDe<T> serDe) {
    if (mem == null) {
      return;
    }

    ReservoirItemsSketch<T> ris = ReservoirItemsSketch.heapify(mem, serDe);
    ris = (ris.getK() <= maxK_ ? ris : ris.downsampledCopy(maxK_));

    if (gadget_ == null) {
      createNewGadget(ris, true);
    } else {
      twoWayMergeInternal(ris, true);
    }
  }

  /**
   * Present this union with a single item to be added to the union.
   *
   * @param datum The given datum of type T.
   */
  public void update(final T datum) {
    if (datum == null) {
      return;
    }

    if (gadget_ == null) {
      gadget_ = ReservoirItemsSketch.newInstance(maxK_);
    }
    gadget_.update(datum);
  }

  /**
   * Present this union with raw elements of a sketch. Useful when operating in a distributed
   * environment like Pig Latin scripts, where an explicit SerDe may be overly complicated but
   * keeping raw values is simple. Values are <em>not</em> copied and the input array may be
   * modified.
   *
   * @param n Total items seen
   * @param k Reservoir size
   * @param input Reservoir samples
   */
  public void update(final long n, final int k, final ArrayList<T> input) {
    ReservoirItemsSketch<T> ris = ReservoirItemsSketch.newInstance(input, n,
            ResizeFactor.X8, k); // forcing a resize factor
    ris = (ris.getK() <= maxK_ ? ris : ris.downsampledCopy(maxK_));

    if (gadget_ == null) {
      createNewGadget(ris, true);
    } else {
      twoWayMergeInternal(ris, true);
    }
  }

  /**
   * Resets this Union. MaxK remains intact, otherwise reverts back to its virgin state.
   */
  void reset() {
    gadget_.reset();
  }

  /**
   * Returns a sketch representing the current state of the union.
   *
   * @return The result of any unions already processed.
   */
  public ReservoirItemsSketch<T> getResult() {
    return (gadget_ != null ? gadget_.copy() : null);
  }

  /**
   * Returns a byte array representation of this union
   *
   * @param serDe An instance of ArrayOfItemsSerDe
   * @return a byte array representation of this union
   */
  public byte[] toByteArray(final ArrayOfItemsSerDe<T> serDe) {
    if ((gadget_ == null) || (gadget_.getNumSamples() == 0)) {
      return toByteArray(serDe, null);
    } else {
      return toByteArray(serDe, gadget_.getValueAtPosition(0).getClass());
    }
  }

  /**
   * Returns a human-readable summary of the sketch, without items.
   *
   * @return A string version of the sketch summary
   */
  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();

    final String thisSimpleName = this.getClass().getSimpleName();

    sb.append(LS);
    sb.append("### ").append(thisSimpleName).append(" SUMMARY: ").append(LS);
    sb.append("   Max k: ").append(maxK_).append(LS);
    if (gadget_ == null) {
      sb.append("   Gadget is null").append(LS);
    } else {
      sb.append("   Gadget summary: ").append(gadget_.toString());
    }
    sb.append("### END UNION SUMMARY").append(LS);

    return sb.toString();
  }

  /**
   * Returns a byte array representation of this union. This method should be used when the array
   * elements are subclasses of a common base class.
   *
   * @param serDe An instance of ArrayOfItemsSerDe
   * @param clazz A class to which the items are cast before serialization
   * @return a byte array representation of this union
   */
  @SuppressWarnings("null") // gadgetBytes will be null only if gadget_ == null AND empty == true
  public byte[] toByteArray(final ArrayOfItemsSerDe<T> serDe, final Class<?> clazz) {
    final int preLongs, outBytes;
    final boolean empty = gadget_ == null;
    final byte[] gadgetBytes = (gadget_ != null ? gadget_.toByteArray(serDe, clazz) : null);

    if (empty) {
      preLongs = Family.RESERVOIR_UNION.getMinPreLongs();
      outBytes = 8;
    } else {
      preLongs = Family.RESERVOIR_UNION.getMaxPreLongs();
      outBytes = (preLongs << 3) + gadgetBytes.length; // for longs, we know the size
    }
    final byte[] outArr = new byte[outBytes];
    final WritableMemory mem = WritableMemory.wrap(outArr);

    // build preLong
    PreambleUtil.insertPreLongs(mem, preLongs);                       // Byte 0
    PreambleUtil.insertSerVer(mem, SER_VER);                          // Byte 1
    PreambleUtil.insertFamilyID(mem, Family.RESERVOIR_UNION.getID()); // Byte 2
    if (empty) {
      PreambleUtil.insertFlags(mem, EMPTY_FLAG_MASK);
    } else {
      PreambleUtil.insertFlags(mem, 0);                               // Byte 3
    }
    PreambleUtil.insertMaxK(mem, maxK_);                              // Bytes 4-5

    if (!empty) {
      final int preBytes = preLongs << 3;
      mem.putByteArray(preBytes, gadgetBytes, 0, gadgetBytes.length);
    }

    return outArr;
  }

  private void createNewGadget(final ReservoirItemsSketch<T> sketchIn,
                               final boolean isModifiable) {
    if ((sketchIn.getK() < maxK_) && (sketchIn.getN() <= sketchIn.getK())) {
      // incoming sketch is in exact mode with sketch's k < maxK,
      // so we can create a gadget at size maxK and keep everything
      // NOTE: assumes twoWayMergeInternal first checks if sketchIn is in exact mode
      gadget_ = ReservoirItemsSketch.newInstance(maxK_);
      twoWayMergeInternal(sketchIn, isModifiable); // isModifiable could be fixed to false here
    } else {
      // use the input sketch as gadget, copying if needed
      gadget_ = (isModifiable ? sketchIn : sketchIn.copy());
    }
  }

  // We make a three-way classification of sketch states.
  // "uni" when (n < k); source of unit weights, can only accept unit weights
  // "mid" when (n == k); source of unit weights, can accept "light" general weights.
  // "gen" when (n > k); source of general weights, can accept "light" general weights.

  // source   target   status      update     notes
  // ----------------------------------------------------------------------------------------------
  // uni,mid  uni      okay        standard   target might transition to mid and gen
  // uni,mid  mid,gen  okay        standard   target might transition to gen
  // gen      uni      must swap   N/A
  // gen      mid,gen  maybe swap  weighted   N assumes fractional values during merge
  // ----------------------------------------------------------------------------------------------

  // Here is why in the (gen, gen) merge case, the items will be light enough in at least one
  // direction:
  // Obviously either (n_s/k_s <= n_t/k_t) OR (n_s/k_s >= n_t/k_t).
  // WLOG say its the former, then (n_s/k_s < n_t/(k_t - 1)) provided n_t > 0 and k_t > 1

  /**
   * This either merges sketchIn into gadget_ or gadget_ into sketchIn. If merging into sketchIn
   * with isModifiable set to false, copies elements from sketchIn first, leaving original
   * unchanged.
   *
   * @param sketchIn Sketch with new samples from which to draw
   * @param isModifiable Flag indicating whether sketchIn can be modified (e.g. if it was rebuild
   *        from Memory)
   */
  private void twoWayMergeInternal(final ReservoirItemsSketch<T> sketchIn,
                                   final boolean isModifiable) {
    if (sketchIn.getN() <= sketchIn.getK()) {
      twoWayMergeInternalStandard(sketchIn);
    } else if (gadget_.getN() < gadget_.getK()) {
      // merge into sketchIn, so swap first
      final ReservoirItemsSketch<T> tmpSketch = gadget_;
      gadget_ = (isModifiable ? sketchIn : sketchIn.copy());
      twoWayMergeInternalStandard(tmpSketch);
    } else if (sketchIn.getImplicitSampleWeight() < (gadget_.getN()
        / ((double) (gadget_.getK() - 1)))) {
      // implicit weights in sketchIn are light enough to merge into gadget
      twoWayMergeInternalWeighted(sketchIn);
    } else {
      // Use next line as an assert/exception?
      // gadget_.getImplicitSampleWeight() < sketchIn.getN() / ((double) (sketchIn.getK() - 1))) {
      // implicit weights in gadget are light enough to merge into sketchIn
      // merge into sketchIn, so swap first
      final ReservoirItemsSketch<T> tmpSketch = gadget_;
      gadget_ = (isModifiable ? sketchIn : sketchIn.copy());
      twoWayMergeInternalWeighted(tmpSketch);
    }
  }

  // should be called ONLY by twoWayMergeInternal
  private void twoWayMergeInternalStandard(final ReservoirItemsSketch<T> source) {
    assert (source.getN() <= source.getK());
    final int numInputSamples = source.getNumSamples();
    for (int i = 0; i < numInputSamples; ++i) {
      gadget_.update(source.getValueAtPosition(i));
    }
  }

  // should be called ONLY by twoWayMergeInternal
  private void twoWayMergeInternalWeighted(final ReservoirItemsSketch<T> source) {
    // gadget_ capable of accepting (light) general weights
    assert (gadget_.getN() >= gadget_.getK());

    final int numSourceSamples = source.getK();

    final double sourceItemWeight = (source.getN() / (double) numSourceSamples);
    final double rescaled_prob = gadget_.getK() * sourceItemWeight; // K * weight
    double targetTotal = gadget_.getN(); // assumes fractional values during merge

    final int tgtK = gadget_.getK();

    for (int i = 0; i < numSourceSamples; ++i) {
      // inlining the update procedure, using targetTotal for the fractional N values
      // similar to ReservoirLongsSketch.update()
      // p(keep_new_item) = (k * w) / newTotal
      // require p(keep_new_item) < 1.0, meaning strict lightness

      targetTotal += sourceItemWeight;

      final double rescaled_one = targetTotal;
      assert (rescaled_prob < rescaled_one); // Use an exception to enforce strict lightness?
      final double rescaled_flip = rescaled_one * SamplingUtil.rand.nextDouble();
      if (rescaled_flip < rescaled_prob) {
        // Intentionally NOT doing optimization to extract slot number from rescaled_flip.
        // Grabbing new random bits to ensure all slots in play
        final int slotNo = SamplingUtil.rand.nextInt(tgtK);
        gadget_.insertValueAtPosition(source.getValueAtPosition(i), slotNo);
      } // end of inlined weight update
    } // end of loop over source samples


    // targetTotal was fractional but should now be an integer again. Could validate with low
    // tolerance, but for now just round to check.
    final long checkN = (long) Math.floor(0.5 + targetTotal);
    gadget_.forceIncrementItemsSeen(source.getN());
    assert (checkN == gadget_.getN());
  }
}

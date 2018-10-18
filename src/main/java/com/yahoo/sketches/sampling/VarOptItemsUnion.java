/*
 * Copyright 2017, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.sampling;

import static com.yahoo.sketches.Util.LS;
import static com.yahoo.sketches.sampling.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.sampling.PreambleUtil.FAMILY_BYTE;
import static com.yahoo.sketches.sampling.PreambleUtil.SER_VER;
import static com.yahoo.sketches.sampling.PreambleUtil.extractFlags;
import static com.yahoo.sketches.sampling.PreambleUtil.extractMaxK;
import static com.yahoo.sketches.sampling.PreambleUtil.extractN;
import static com.yahoo.sketches.sampling.PreambleUtil.extractOuterTauDenominator;
import static com.yahoo.sketches.sampling.PreambleUtil.extractOuterTauNumerator;
import static com.yahoo.sketches.sampling.PreambleUtil.extractPreLongs;
import static com.yahoo.sketches.sampling.PreambleUtil.extractSerVer;
import static com.yahoo.sketches.sampling.VarOptItemsSketch.newInstanceFromUnionResult;

import java.util.ArrayList;
import java.util.Iterator;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.ArrayOfItemsSerDe;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.SketchesArgumentException;

/**
 * Provides a unioning operation over varopt sketches. This union allows the sample size k to float,
 * possibly increasing or decreasing as warranted by the available data.
 *
 * @author Jon Malkin
 * @author Kevin Lang
 * @param <T> Type of items
 */
public final class VarOptItemsUnion<T> {
  private VarOptItemsSketch<T> gadget_;
  private final int maxK_;
  private long n_; // cumulative over all input sketches

  // outer tau is the largest tau of any input sketch
  private double outerTauNumer; // total weight of all input R-zones where tau = outerTau

  // total cardinality of the same R-zones, or zero if no input sketch was in estimation mode
  private long outerTauDenom;

  /*
   IMPORTANT NOTE: the "gadget" in the union object appears to be a varopt sketch,
   but in fact is NOT because it doesn't satisfy the mathematical definition
   of a varopt sketch of the concatenated input streams. Therefore it could be different
   from a true varopt sketch with that value of K, in which case it could easily provide
   worse estimation accuracy for subset-sum queries.

   This should not surprise you; the approximation guarantees of varopt sketches
   do not apply to things that merely resemble varopt sketches.

   However, even though the gadget is not a varopt sketch, the result
   of the unioning process IS a varopt sketch. It is constructed by a
   somewhat complicated "resolution" process which determines the largest K
   that a valid varopt sketch could have given the available information,
   then constructs a varopt sketch of that size and returns it.

   However, the gadget itself is not touched during the resolution process,
   and additional sketches could subsequently be merged into the union,
   at which point a varopt result could again be requested.
   */

  /*
   Explanation of "marked items" in the union's gadget:

   The boolean value "true" in an pair indicates that the item
   came from an input sketch's R zone, so it is already the result of sampling.

   Therefore it must not wind up in the H zone of the final result, because
   that would imply that the item is "exact".

   However, it is okay for a marked item to hang out in the gadget's H zone for a while.

   And once the item has moved to the gadget's R zone, the mark is never checked again,
   so no effort is made to ensure that its value is preserved or even makes sense.
   */

  /*
   Note: if the computer could perform exact real-valued arithmetic, the union could finalize
   its result by reducing k until inner_tau > outer_tau. [Due to the vagaries of floating point
   arithmetic, we won't attempt to detect and specially handle the inner_tau = outer_tau special
   case.]

   In fact, we won't even look at tau while while reducing k. Instead the logic will be based
   on the more robust integer quantity num_marks_in_h_ in the gadget. It is conceivable that due
   to round-off error we could end up with inner_tau slightly less than outer_tau, but that should
   be fairly harmless since we will have achieved our goal of getting the marked items out of H.

   Also, you might be wondering why we are bothering to maintain the numerator and denominator
   separately instead of just having a single variable outer_tau. This allows us (in certain
   cases) to add an input's entire R-zone weight into the result sketch, as opposed to subdividing
   it then adding it back up. That would be a source of numerical inaccuracy. And even
   more importantly, this design choice allows us to exactly re-construct the input sketch
   when there is only one of them.
   */

  /**
   * Empty constructor
   *
   * @param maxK Maximum allowed reservoir capacity for this union
   */
  private VarOptItemsUnion(final int maxK) {
    maxK_ = maxK;
    n_ = 0;
    outerTauNumer = 0.0;
    outerTauDenom = 0;
    gadget_ = VarOptItemsSketch.newInstanceAsGadget(maxK);
  }

  /**
   * Creates an empty Union with a maximum capacity of size k.
   *
   * @param <T> The type of item this union contains
   * @param maxK The maximum allowed capacity of the unioned result
   * @return A new VarOptItemsUnion
   */
  public static <T> VarOptItemsUnion<T> newInstance(final int maxK) {
    return new VarOptItemsUnion<>(maxK);
  }

  /**
   * Instantiates a Union from Memory
   *
   * @param <T> The type of item this sketch contains
   * @param srcMem Memory object containing a serialized union
   * @param serDe An instance of ArrayOfItemsSerDe
   * @return A VarOptItemsUnion created from the provided Memory
   */
  public static <T> VarOptItemsUnion<T> heapify(final Memory srcMem,
                                                final ArrayOfItemsSerDe<T> serDe) {
    Family.VAROPT_UNION.checkFamilyID(srcMem.getByte(FAMILY_BYTE));

    long n = 0;
    double outerTauNum = 0.0;
    long outerTauDenom = 0;

    final int numPreLongs = extractPreLongs(srcMem);
    final int serVer = extractSerVer(srcMem);
    final boolean isEmpty = (extractFlags(srcMem) & EMPTY_FLAG_MASK) != 0;
    final int maxK = extractMaxK(srcMem);
    if (!isEmpty) {
      n = extractN(srcMem);
      outerTauNum = extractOuterTauNumerator(srcMem);
      outerTauDenom = extractOuterTauDenominator(srcMem);
    }

    if (serVer != SER_VER) {
      throw new SketchesArgumentException(
              "Possible Corruption: Ser Ver must be " + SER_VER + ": " + serVer);
    }

    final boolean preLongsEqMin = (numPreLongs == Family.VAROPT_UNION.getMinPreLongs());
    final boolean preLongsEqMax = (numPreLongs == Family.VAROPT_UNION.getMaxPreLongs());

    if (!preLongsEqMin && !preLongsEqMax) {
      throw new SketchesArgumentException("Possible corruption: Non-empty union with only "
              + Family.VAROPT_UNION.getMinPreLongs() + "preLongs");
    }

    final VarOptItemsUnion<T> viu = new VarOptItemsUnion<>(maxK);

    if (isEmpty) {
      viu.gadget_ = VarOptItemsSketch.newInstanceAsGadget(maxK);
    } else {
      viu.n_ = n;
      viu.outerTauNumer = outerTauNum;
      viu.outerTauDenom = outerTauDenom;

      final int preLongBytes = numPreLongs << 3;
      final Memory sketchMem = srcMem.region(preLongBytes, srcMem.getCapacity() - preLongBytes);
      viu.gadget_ = VarOptItemsSketch.heapify(sketchMem, serDe);
    }

    return viu;
  }

  /**
   * Union the given sketch.
   *
   *<p>This method can be repeatedly called.</p>
   *
   * @param sketchIn The sketch to be merged
   */
  public void update(final VarOptItemsSketch<T> sketchIn) {
    if (sketchIn != null) {
      mergeInto(sketchIn);
    }
  }

  /**
   * Union the given Memory image of the sketch.
   *
   *<p>This method can be repeatedly called.</p>
   *
   * @param mem Memory image of sketch to be merged
   * @param serDe An instance of ArrayOfItemsSerDe
   */
  public void update(final Memory mem, final ArrayOfItemsSerDe<T> serDe) {
    if (mem != null) {
      final VarOptItemsSketch<T> vis = VarOptItemsSketch.heapify(mem, serDe);
      mergeInto(vis);
    }
  }

  /**
   * Union a reservoir sketch. The reservoir sample is treated as if all items were added with a
   * weight of 1.0.
   *
   * @param reservoirIn The reservoir sketch to be merged
   */
  public void update(final ReservoirItemsSketch<T> reservoirIn) {
    if (reservoirIn != null) {
      mergeReservoirInto(reservoirIn);
    }
  }

  /**
   * Gets the varopt sketch resulting from the union of any input sketches.
   *
   * @return A varopt sketch
   */
  public VarOptItemsSketch<T> getResult() {
    // If no marked items in H, gadget is already valid mathematically. We can return what is
    // basically just a copy of the gadget.
    if (gadget_.getNumMarksInH() == 0) {
      return simpleGadgetCoercer();
    } else {
      // At this point, we know that marked items are present in H. So:
      //   1. Result will necessarily be in estimation mode
      //   2. Marked items currently in H need to be absorbed into reservoir (R)
      final VarOptItemsSketch<T> tmp = detectAndHandleSubcaseOfPseudoExact();
      if (tmp != null) {
        // sub-case detected and handled, so return the result
        return tmp;
      } else {
        // continue with main logic
        return migrateMarkedItemsByDecreasingK();
      }
    }
  }

  /**
   * Resets this sketch to the empty state, but retains the original value of max k.
   */
  public void reset() {
    gadget_.reset();
    n_ = 0;
    outerTauNumer = 0.0;
    outerTauDenom = 0;
  }

  /**
   * Returns a human-readable summary of the sketch, without items.
   *
   * @return A string version of the sketch summary
   */
  @Override
  public String toString() {
    assert gadget_ != null;
    final StringBuilder sb = new StringBuilder();

    final String thisSimpleName = this.getClass().getSimpleName();

    sb.append(LS)
            .append("### ").append(thisSimpleName).append(" SUMMARY: ").append(LS)
            .append("   Max k: ").append(maxK_).append(LS)
            .append("   Gadget summary: ").append(gadget_.toString())
            .append("### END UNION SUMMARY").append(LS);

    return sb.toString();
  }

  /**
   * Returns a byte array representation of this union
   *
   * @param serDe An instance of ArrayOfItemsSerDe
   * @return a byte array representation of this union
   */
  public byte[] toByteArray(final ArrayOfItemsSerDe<T> serDe) {
    assert gadget_ != null;
    if (gadget_.getNumSamples() == 0) {
      return toByteArray(serDe, null);
    } else {
      return toByteArray(serDe, gadget_.getItem(0).getClass());
    }
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
    final boolean empty = gadget_.getNumSamples() == 0;
    final byte[] gadgetBytes = (empty ? null : gadget_.toByteArray(serDe, clazz));

    if (empty) {
      preLongs = Family.VAROPT_UNION.getMinPreLongs();
      outBytes = 8;
    } else {
      preLongs = Family.VAROPT_UNION.getMaxPreLongs();
      outBytes = (preLongs << 3) + gadgetBytes.length; // for longs, we know the size
    }
    final byte[] outArr = new byte[outBytes];
    final WritableMemory mem = WritableMemory.wrap(outArr);

    // build preLong
    PreambleUtil.insertPreLongs(mem, preLongs);                    // Byte 0
    PreambleUtil.insertSerVer(mem, SER_VER);                       // Byte 1
    PreambleUtil.insertFamilyID(mem, Family.VAROPT_UNION.getID()); // Byte 2
    if (empty) {
      PreambleUtil.insertFlags(mem, EMPTY_FLAG_MASK);
    } else {
      PreambleUtil.insertFlags(mem, 0);                            // Byte 3
    }
    PreambleUtil.insertMaxK(mem, maxK_);                           // Bytes 4-7

    if (!empty) {
      PreambleUtil.insertN(mem, n_);                               // Bytes 8-15
      PreambleUtil.insertOuterTauNumerator(mem, outerTauNumer);    // Bytes 16-23
      PreambleUtil.insertOuterTauDenominator(mem, outerTauDenom);  // Bytes 24-31

      final int preBytes = preLongs << 3;
      mem.putByteArray(preBytes, gadgetBytes, 0, gadgetBytes.length);
    }

    return outArr;
  }

  // package-private for testing
  double getOuterTau() {
    if (outerTauDenom == 0) {
      return 0.0;
    } else {
      return outerTauNumer / outerTauDenom;
    }
  }

  private void mergeInto(final VarOptItemsSketch<T> sketch) {
    final long sketchN = sketch.getN();
    if (sketchN == 0) {
      return;
    }

    n_ += sketchN;

    final VarOptItemsSamples<T> sketchSamples = sketch.getSketchSamples();

    // insert H region items
    Iterator<VarOptItemsSamples<T>.WeightedSample> sketchIterator;
    sketchIterator = sketchSamples.getHIterator();
    while (sketchIterator.hasNext()) {
      final VarOptItemsSamples<T>.WeightedSample ws = sketchIterator.next();
      gadget_.update(ws.getItem(), ws.getWeight(), false);
    }

    // insert R region items
    sketchIterator = sketchSamples.getWeightCorrRIter();
    while (sketchIterator.hasNext()) {
      final VarOptItemsSamples<T>.WeightedSample ws = sketchIterator.next();
      gadget_.update(ws.getItem(), ws.getWeight(), true);
    }

    // resolve tau
    if (sketch.getRRegionCount() > 0) {
      final double sketchTau = sketch.getTau();
      final double outerTau = getOuterTau();

      if (outerTauDenom == 0) {
        // detect first estimation mode sketch and grab its tau
        outerTauNumer = sketch.getTotalWtR();
        outerTauDenom = sketch.getRRegionCount();
      } else if (sketchTau > outerTau) {
        // switch to a bigger value of outerTau
        outerTauNumer = sketch.getTotalWtR();
        outerTauDenom = sketch.getRRegionCount();
      } else if (sketchTau == outerTau) {
        // Ok if previous equality test isn't quite perfect. Mistakes in either direction should
        // be fairly benign.
        // Without conceptually changing outerTau, update number and denominator. In particular,
        // add the total weight of the incoming reservoir to the running total.
        outerTauNumer += sketch.getTotalWtR();
        outerTauDenom += sketch.getRRegionCount();
      }

      // do nothing if sketch's tau is smaller than outerTau
    }
  }

  /**
   * Used to merge a reservoir sample into varopt, assuming the reservoir was built with items
   * of weight 1.0. Logic is very similar to mergeInto() for a sketch with no heavy items.
   * @param reservoir Reservoir sketch to merge into this union
   */
  private void mergeReservoirInto(final ReservoirItemsSketch<T> reservoir) {
    final long reservoirN = reservoir.getN();
    if (reservoirN == 0) {
      return;
    }

    n_ += reservoirN;

    final int reservoirK = reservoir.getK();
    if (reservoir.getN() <= reservoirK) {
      // exact mode, so just insert and be done
      for (T item : reservoir.getRawSamplesAsList()) {
        gadget_.update(item, 1.0, false);
      }
    } else {
      // sampling mode. We'll replicate a weight-correcting iterator
      final double reservoirTau = reservoir.getImplicitSampleWeight();

      double cumWeight = 0.0;
      final ArrayList<T> samples = reservoir.getRawSamplesAsList();
      for (int i = 0; i < (reservoirK - 1); ++i) {
        gadget_.update(samples.get(i), reservoirTau, true);
        cumWeight += reservoirTau;
      }
      // correct for any numerical discrepancies with the last item
      gadget_.update(samples.get(reservoirK - 1), reservoir.getN() - cumWeight, true);

      // resolve tau
      final double outerTau = getOuterTau();

      if (outerTauDenom == 0) {
        // detect first estimation mode sketch and grab its tau
        outerTauNumer = reservoirN;
        outerTauDenom = reservoirK;
      } else if (reservoirTau > outerTau) {
        // switch to a bigger value of outerTau
        outerTauNumer = reservoirN;
        outerTauDenom = reservoirK;
      } else if (reservoirTau == outerTau) {
        // Ok if previous equality test isn't quite perfect. Mistakes in either direction should
        // be fairly benign.
        // Without conceptually changing outerTau, update number and denominator. In particular,
        // add the total weight of the incoming reservoir to the running total.
        outerTauNumer += reservoirN;
        outerTauDenom += reservoirK;
      }
      // do nothing if reservoir "tau" is no smaller than outerTau
    }
  }

  /**
   * When there are no marked items in H, teh gadget is mathematically equivalent to a valid
   * varopt sketch. This method simply returns a copy (without perserving marks).
   *
   * @return A shallow copy of the gadget as valid varopt sketch
   */
  private VarOptItemsSketch<T> simpleGadgetCoercer() {
    assert gadget_.getNumMarksInH() == 0;
    return gadget_.copyAndSetN(true, n_);
  }

  /**
   * This coercer directly transfers marked items from the gadget's H into the result's R.
   * Deciding whether that is a valid thing to do is the responsibility of the caller. Currently,
   * this is only used for a subcase of pseudo-exact, but later it might be used by other
   * subcases as well.
   *
   * @return A sketch derived from the gadget, with marked items moved to the reservoir
   */
  private VarOptItemsSketch<T> markMovingGadgetCoercer() {
    final int resultK = gadget_.getHRegionCount() + gadget_.getRRegionCount();

    int resultH = 0;
    int resultR = 0;
    int nextRPos = resultK; // = (resultK+1)-1, to fill R region from back to front

    final ArrayList<T> data         = new ArrayList<>(resultK + 1);
    final ArrayList<Double> weights = new ArrayList<>(resultK + 1);

    // Need arrays filled to use set() and be able to fill from end forward.
    // Ideally would create as arrays but trying to avoid forcing user to pass a Class<?>
    for (int i = 0; i < (resultK + 1); ++i) {
      data.add(null);
      weights.add(null);
    }

    final VarOptItemsSamples<T> sketchSamples = gadget_.getSketchSamples();
    // insert R region items, ignoring weights
    // Currently (May 2017) this next block is unreachable; this coercer is used only in the
    // pseudo-exact case in which case there are no items natively in R, only marked items in H
    // that will be moved into R as part of the coercion process.
    Iterator<VarOptItemsSamples<T>.WeightedSample> sketchIterator;
    sketchIterator = sketchSamples.getRIterator();
    while (sketchIterator.hasNext()) {
      final VarOptItemsSamples<T>.WeightedSample ws = sketchIterator.next();
      data.set(nextRPos, ws.getItem());
      weights.set(nextRPos, -1.0);
      ++resultR;
      --nextRPos;
    }
    double transferredWeight = 0;

    // insert H region items
    sketchIterator = sketchSamples.getHIterator();
    while (sketchIterator.hasNext()) {
      final VarOptItemsSamples<T>.WeightedSample ws = sketchIterator.next();
      if (ws.getMark()) {
        data.set(nextRPos, ws.getItem());
        weights.set(nextRPos, -1.0);
        transferredWeight += ws.getWeight();
        ++resultR;
        --nextRPos;
      } else {
        data.set(resultH, ws.getItem());
        weights.set(resultH, ws.getWeight());
        ++resultH;
      }
    }

    assert (resultH + resultR) == resultK;
    assert Math.abs(transferredWeight - outerTauNumer) < 1e-10;

    final double resultRWeight = gadget_.getTotalWtR() + transferredWeight;
    final long resultN = n_;

    // explicitly set values for the gap
    data.set(resultH, null);
    weights.set(resultH, -1.0);

    // create sketch with the new values
    return newInstanceFromUnionResult(data, weights, resultK, resultN, resultH, resultR, resultRWeight);
  }

  private VarOptItemsSketch<T> detectAndHandleSubcaseOfPseudoExact() {
    // gadget is seemingly exact
    final boolean condition1 = gadget_.getRRegionCount() == 0;

    // but there are marked items in H, so only _pseudo_ exact
    final boolean condition2 = gadget_.getNumMarksInH() > 0;

    // if gadget is pseudo-exact and the number of marks equals outerTauDenom, then we can deduce
    // from the bookkeeping logic of mergeInto() that all estimation mode input sketches must
    // have had the same tau, so we can throw all of the marked items into a common reservoir.
    final boolean condition3 = gadget_.getNumMarksInH() == outerTauDenom;

    if (!(condition1 && condition2 && condition3)) {
      return null;
    } else {

      // explicitly enforce rule that items in H should not be lighter than the sketch's tau
      final boolean antiCondition4 = thereExistUnmarkedHItemsLighterThanTarget(gadget_.getTau());
      if (antiCondition4) {
        return null;
      } else {
        // conditions 1 through 4 hold
        return markMovingGadgetCoercer();
      }
    }
  }

  // this is a condition checked in detectAndHandleSubcaseOfPseudoExact()
  private boolean thereExistUnmarkedHItemsLighterThanTarget(final double threshold) {
    for (int i = 0; i < gadget_.getHRegionCount(); ++i) {
      if ((gadget_.getWeight(i) < threshold) && !gadget_.getMark(i)) {
        return true;
      }
    }

    return false;
  }

  // this is basically a continuation of getResult()
  private VarOptItemsSketch<T> migrateMarkedItemsByDecreasingK() {
    final VarOptItemsSketch<T> gcopy = gadget_.copyAndSetN(false, n_);

    final int rCount = gcopy.getRRegionCount();
    final int hCount = gcopy.getHRegionCount();
    final int k = gcopy.getK();

    assert gcopy.getNumMarksInH() > 0; // ensured by caller
    // either full (of samples), or in pseudo-exact mode, or both
    assert (rCount == 0) || (k == (hCount + rCount));

    // if non-full and pseudo-exact, change k so that gcopy is full
    if ((rCount == 0) && (hCount < k)) {
      gcopy.forceSetK(hCount);
    }

    // Now k equals the number of samples, so reducing k will increase tau.
    // Also, we know that there are at least 2 samples because 0 or 1 would have been handled
    // by the earlier logic in getResult()
    assert gcopy.getK() >= 2;
    gcopy.decreaseKBy1();

    // gcopy is now in estimation mode, just like the final result must be (due to marked items)
    assert gcopy.getRRegionCount() > 0;
    assert gcopy.getTau() > 0.0;

    // keep reducing k until all marked items have been absorbed into the reservoir
    while (gcopy.getNumMarksInH() > 0) {
      assert gcopy.getK() >= 2; // because h_ and r_ are both at least 1
      gcopy.decreaseKBy1();
    }

    gcopy.stripMarks();
    return gcopy;
  }
}

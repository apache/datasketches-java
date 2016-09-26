package com.yahoo.sketches.sampling;

import static com.yahoo.sketches.sampling.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.sampling.PreambleUtil.FAMILY_BYTE;
import static com.yahoo.sketches.sampling.PreambleUtil.SER_VER;
import static com.yahoo.sketches.sampling.PreambleUtil.extractFlags;
import static com.yahoo.sketches.sampling.PreambleUtil.extractMaxK;
import static com.yahoo.sketches.sampling.PreambleUtil.extractSerVer;
import static com.yahoo.sketches.sampling.PreambleUtil.getAndCheckPreLongs;

import com.yahoo.memory.Memory;
import com.yahoo.memory.MemoryRegion;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.ArrayOfItemsSerDe;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.SketchesArgumentException;

/**
 * Class to union reservoir samples of generic items.
 *
 * <p>For efficiency reasons, the unioning process picks one of the two sketches to use as the base. As a result,
 * we provide only a stateful union. Using the same approach for a merge would result in unpredictable side effects on
 * the underlying sketches.</p>
 *
 * <p>A union object is created with a maximum value of <tt>k</tt>, represented using the ReservoirSize class. The
 * unioning process may cause the actual number of samples to fall below that maximum value, but never to exceed it.
 * The result of a union will be a reservoir where each item from teh global input has a uniform probability
 * of selection, but there are no claims about higher order statistics. For instance, in general all possible
 * permutations of the global input are not equally likely.</p>
 *
 * <p>If taking the uinon of two reservoirs of different sizes, the output sample will contain no more than
 * MIN(k_1, k_2) samples.</p>
 *
 * @author Jon Malkin
 * @author Kevin Lang
 */
public class ReservoirItemsUnion<T> {
    private ReservoirItemsSketch<T> gadget_;
    private final short encodedMaxK_;

    /**
     * Empty constructor using ReservoirSize-encoded maxK value
     * @param maxK Maximum allowed reservoir capacity for this union
     */
    private ReservoirItemsUnion(final short maxK) {
        encodedMaxK_ = maxK;
    }

    /**
     * Creates an empty Union with a maximum reservoir capacity of size k, subject to the precision of ReservoirSize
     * @param <T> The type of item this sketch contains
     * @param maxK The maximum allowed reservoir capacity for any sketches in the union
     * @return A new ReservoirItemsUnion
     */
    public static <T> ReservoirItemsUnion<T> getInstance(final int maxK) {
        final short encodedMaxK = ReservoirSize.computeSize(maxK);
        return new ReservoirItemsUnion<>(encodedMaxK);
    }

    /**
     * Instantiates a Union from Memory
     * @param <T> The type of item this sketch contains
     * @param srcMem Memory object containing a serialized union
     * @param serDe An instance of ArrayOfItemsSerDe
     * @return A ReservoirItemsUnion created from the provided Memory
     */
    public static <T> ReservoirItemsUnion<T> getInstance(final Memory srcMem, ArrayOfItemsSerDe<T> serDe) {
        Family.RESERVOIR_UNION.checkFamilyID(srcMem.getByte(FAMILY_BYTE));

        final int numPreLongs = getAndCheckPreLongs(srcMem);
        final long pre0 = srcMem.getLong(0);
        final int serVer = extractSerVer(pre0);
        final boolean isEmpty = (extractFlags(pre0) & EMPTY_FLAG_MASK) != 0;

        final short encodedMaxK = extractMaxK(pre0);

        final boolean preLongsEqMin = (numPreLongs == Family.RESERVOIR.getMinPreLongs());
        final boolean preLongsEqMax = (numPreLongs == Family.RESERVOIR.getMaxPreLongs());

        if (!preLongsEqMin & !preLongsEqMax) {
            throw new SketchesArgumentException(
                    "Possible corruption: Non-empty sketch with only " + Family.RESERVOIR.getMinPreLongs()
                            + "preLongs");
        }
        if (serVer != SER_VER) {
            throw new SketchesArgumentException(
                    "Possible Corruption: Ser Ver must be " + SER_VER + ": " + serVer);
        }

        ReservoirItemsUnion<T> riu = new ReservoirItemsUnion<>(encodedMaxK);

        if (!isEmpty) {
            int preLongBytes = numPreLongs << 3;
            MemoryRegion sketchMem = new MemoryRegion(srcMem, preLongBytes, srcMem.getCapacity() - preLongBytes);
            ReservoirItemsSketch<T> ris = ReservoirItemsSketch.getInstance(sketchMem, serDe);
            riu.update(ris);
        }

        return riu;
    }

    /**
     * Returns the maximum allowed reservoir capacity in this union. The current reservoir capacity may be lower.
     * @return The maximum allowed reservoir capacity in this union.
     */
    public int getMaxK() {
        return ReservoirSize.decodeValue(encodedMaxK_);
    }

    /**
     * Union the given sketch.
     * This method can be repeatedly called.
     * If the given sketch is null it is interpreted as an empty sketch.
     *
     * @param sketchIn The incoming sketch.
     */
    public void update(ReservoirItemsSketch<T> sketchIn) {
        if (sketchIn == null) { return; }

        int maxK = ReservoirSize.decodeValue(encodedMaxK_);
        ReservoirItemsSketch<T> ris = (sketchIn.getK() <= maxK ? sketchIn : sketchIn.downsampledCopy(encodedMaxK_));

        // can modify the sketch if we downsampled, otherwise may need to copy it
        if (gadget_ == null) {
            gadget_ = (sketchIn == ris ? ris.copy() : ris);
        } else {
            boolean isModifiable = (sketchIn != ris);
            twoWayMergeInternal(ris, isModifiable);
        }
    }

    /**
     * Union the given Memory image of the sketch.
     *
     * <p>This method can be repeatedly called.
     * If the given sketch is null it is interpreted as an empty sketch.
     * @param mem Memory image of sketch to be merged
     * @param serDe An instance of ArrayOfItemsSerDe
     */
    public void update(Memory mem, ArrayOfItemsSerDe<T> serDe) {
        if (mem == null) { return; }

        ReservoirItemsSketch<T> ris = ReservoirItemsSketch.getInstance(mem, serDe);

        int maxK = ReservoirSize.decodeValue(encodedMaxK_);
        ris = (ris.getK() <= maxK ? ris : ris.downsampledCopy(encodedMaxK_));

        if (gadget_ == null) {
            gadget_ = ris;
        } else {
            twoWayMergeInternal(ris, true);
        }
    }

    /**
     * Present this union with a reservoir item.
     *
     * @param datum The given datum of type T.
     */
    public void update(T datum) {
        if (gadget_ == null) {
            int maxK = ReservoirSize.decodeValue(encodedMaxK_);
            gadget_ = ReservoirItemsSketch.getInstance(maxK);
        }
        gadget_.update(datum);
    }

    /**
     * Returns a sketch representing the current state of the union.
     * @return The result of any unions already processed.
     */
    public ReservoirItemsSketch<T> getResult() {
        return (gadget_ != null ? gadget_.copy() : null);
    }

    /**
     * Returns a byte array representation of this union
     * @param serDe An instance of ArrayOfItemsSerDe
     * @return a byte array representation of this union
     */
    public byte[] toByteArray(ArrayOfItemsSerDe<T> serDe) {
        if (gadget_ == null || gadget_.getNumSamples() == 0) {
            return toByteArray(serDe, null);
        } else {
            return toByteArray(serDe, gadget_.getValueAtPosition(0).getClass());
        }

    }

    /**
     * Returns a byte array representation of this union. This method should be used when the array elements are
     * subclasses of a common base class.
     * @param serDe An instance of ArrayOfItemsSerDe
     * @param clazz A class to which the items are cast before serialization
     * @return a byte array representation of this union
     */
    public byte[] toByteArray(ArrayOfItemsSerDe<T> serDe, Class<?> clazz) {
        final int preLongs, outBytes;
        final boolean empty = gadget_ == null;
        byte[] gadgetBytes = (gadget_ != null ? gadget_.toByteArray(serDe, clazz) : null);

        if (empty) {
            preLongs = Family.RESERVOIR_UNION.getMinPreLongs();
            outBytes = 8;
        } else {
            preLongs = Family.RESERVOIR_UNION.getMaxPreLongs();
            outBytes = (preLongs << 3) + gadgetBytes.length; // for longs, we know the size
        }
        final byte[] outArr = new byte[outBytes];
        final Memory mem = new NativeMemory(outArr);

        // build preLong
        long pre0 = 0L;
        pre0 = PreambleUtil.insertPreLongs(preLongs, pre0);                        // Byte 0
        pre0 = PreambleUtil.insertSerVer(SER_VER, pre0);                           // Byte 1
        pre0 = PreambleUtil.insertFamilyID(Family.RESERVOIR_UNION.getID(), pre0);  // Byte 2
        pre0 = (empty) ? PreambleUtil.insertFlags(EMPTY_FLAG_MASK, pre0) : PreambleUtil.insertFlags(0, pre0); // Byte 3
        pre0 = PreambleUtil.insertMaxK(encodedMaxK_, pre0);                        // Bytes 4-5
        pre0 = PreambleUtil.insertSerDeId(serDe.getId(), pre0);

        mem.putLong(0, pre0);
        if (!empty) {
            final int preBytes = preLongs << 3;
            mem.putByteArray(preBytes, gadgetBytes, 0, gadgetBytes.length);
        }

        return outArr;

    }

    // We make a three-way classification of sketch states.
    // "uni" when (n < k);   source of unit weights, can only accept unit weights
    // "mid" when (n == k);  source of unit weights, can accept "light" general weights.
    // "gen" when (n > k);   source of general weights, can accept "light" general weights.

    //     source          target          status            update            notes
    // ---------------------------------------------------------------------------------------------------------
    //     uni,mid          uni             okay             standard       target might transition to mid and gen
    //     uni,mid        mid,gen           okay             standard       target might transition to gen
    //       gen            uni          must swap              N/A
    //       gen          mid,gen       maybe swap            weighted     N assumes fractional values during merge
    // ---------------------------------------------------------------------------------------------------------

    // Here is why in the (gen, gen) merge case, the items will be light enough in at least one direction:
    // Obviously either (n_s/k_s <= n_t/k_t) OR (n_s/k_s >= n_t/k_t).
    // WLOG say its the former, then (n_s/k_s < n_t/(k_t - 1)) provided n_t > 0 and k_t > 1

    /**
     * This either merges sketchIn into gadget_ or gadget_ into sketchIn. If merging into sketchIn with isModifiable
     * set to false, copies elements from sketchIn first, leaving original unchanged.
     * @param sketchIn Sketch with new samples from which to draw
     * @param isModifiable Flag indicating whether sketchIn can be modified (e.g. if it was rebuild from Memory)
     */
    private void twoWayMergeInternal(final ReservoirItemsSketch<T> sketchIn, final boolean isModifiable) {
        if (sketchIn.getN() <= sketchIn.getK()) {
            twoWayMergeInternalStandard(sketchIn);
        } else if (gadget_.getN() < gadget_.getK()) {
            // merge into sketchIn, so swap first
            ReservoirItemsSketch<T> tmpSketch = gadget_;
            gadget_ = (isModifiable ? sketchIn : sketchIn.copy());
            twoWayMergeInternalStandard(tmpSketch);
        } else if (sketchIn.getImplicitSampleWeight() < gadget_.getN() / ((double) (gadget_.getK() - 1))) {
            // implicit weights in sketchIn are light enough to merge into gadget
            twoWayMergeInternalWeighted(sketchIn);
        } else {
            // Use next line as an assert/exception?
            // gadget_.getImplicitSampleWeight() < sketchIn.getN() / ((double) (sketchIn.getK() - 1))) {
            // implicit weights in gadget are light enough to merge into sketchIn
            // merge into sketchIn, so swap first
            ReservoirItemsSketch<T> tmpSketch = gadget_;
            gadget_ = (isModifiable ? sketchIn : sketchIn.copy());
            twoWayMergeInternalWeighted(tmpSketch);
        }
    }

    // should be called ONLY by twoWayMergeInternal
    private void twoWayMergeInternalStandard(final ReservoirItemsSketch<T> source) {
        assert (source.getN() <= source.getK());
        int numInputSamples = source.getNumSamples();
        for (int i = 0; i < numInputSamples; ++i) {
            gadget_.update(source.getValueAtPosition(i));
        }
    }

    // should be called ONLY by twoWayMergeInternal
    private void twoWayMergeInternalWeighted(final ReservoirItemsSketch<T> source) {
        assert (gadget_.getN() >= gadget_.getK()); // gadget_ capable of accepting (light) general weights

        int numSourceSamples = source.getK();

        double sourceItemWeight = (source.getN() / (double) numSourceSamples);
        double rescaled_prob = gadget_.getK() * sourceItemWeight; // K * weight
        double targetTotal = gadget_.getN(); // assumes fractional values during merge

        int tgtK = gadget_.getK();

        for (int i = 0; i < numSourceSamples; ++i) {
            // inlining the update procedure, using targetTotal for the fractional N values
            // similar to ReservoirLongsSketch.update()
            // p(keep_new_item) = (k * w) / newTotal
            // require p(keep_new_item) < 1.0, meaning strict lightness

            targetTotal += sourceItemWeight;

            double rescaled_one = targetTotal;
            assert (rescaled_prob < rescaled_one); // Do we want an exception to enforce strict lightness?
            double rescaled_flip = rescaled_one * SamplingUtil.rand.nextDouble();
            if (rescaled_flip < rescaled_prob) {
                // Intentionally NOT doing optimization to extract slot number from rescaled_flip.
                // Grabbing new random bits to ensure all slots in play
                int slotNo = SamplingUtil.rand.nextInt(tgtK);
                gadget_.insertValueAtPosition(source.getValueAtPosition(i), slotNo);
            } // end of inlined weight update
        } // end of loop over source samples


        // targetTotal was fractional but should now be an integer again. Could validate with low tolerance, but for now
        // just round to check.
        long checkN = (long) Math.floor(0.5 + targetTotal);
        gadget_.forceIncrementItemsSeen(source.getN());
        assert (checkN == gadget_.getN());
    }
}

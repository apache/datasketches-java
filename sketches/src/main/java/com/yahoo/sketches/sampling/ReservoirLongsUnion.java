package com.yahoo.sketches.sampling;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.ResizeFactor;

/**
 * Class to union reservoir samples. Because the union process picks one of the two sketches to use as the base,
 * we provide only a union; a merge would result in unpredictable side effects on the underlying sketches.
 *
 * <p>If taking the uinon of two reservoirs of different sizes, the output sample will contain no more than
 * MIN(k_1, k_2) samples.</p>
 *
 * @author Jon Malkin
 * @author Kevin Lang
 */
public class ReservoirLongsUnion {
    private ReservoirLongsSketch gadget_;

    public ReservoirLongsUnion(final int k) {
        gadget_ = ReservoirLongsSketch.getInstance(k);
    }

    public ReservoirLongsUnion(final int k, final ResizeFactor rf) {
        gadget_ = ReservoirLongsSketch.getInstance(k, rf);
    }

    public ReservoirLongsUnion(ReservoirLongsSketch sketchIn) {
        gadget_ = sketchIn.copy();
    }

    public ReservoirLongsUnion(Memory srcMem) {
        gadget_ = ReservoirLongsSketch.getInstance(srcMem);
    }

    /**
     * Union the given sketch.
     * This method can be repeatedly called.
     * If the given sketch is null it is interpreted as an empty sketch.
     *
     * @param sketchIn The incoming sketch.
     */
    void update(ReservoirLongsSketch sketchIn) {
        if (gadget_ == null) {
            gadget_ = sketchIn;
        } else if (sketchIn != null) {
            twoWayMergeInternal(sketchIn, false);
        } // if sketchIn == null, return
    }

    /**
     * Union the given Memory image of the sketch.
     *
     * <p>This method can be repeatedly called.
     * If the given sketch is null it is interpreted as an empty sketch.
     * @param mem Memory image of sketch to be merged
     */
    void update(Memory mem) {
        if (mem != null) {
            ReservoirLongsSketch rls = ReservoirLongsSketch.getInstance(mem);

            if (gadget_ == null) {
                gadget_ = rls;
            } else {
                twoWayMergeInternal(rls, true);
            }
        }
    }

    /**
     * Present this union with a long.
     *
     * @param datum The given long datum.
     */
    public void update(long datum) {
        // TODO: what if gadget_ is null?
        gadget_.update(datum);
    }

    /**
     * This either merges sketchIn into gadget_ or gadget_ into sketchIn. If merging into sketchIn with isModifiable
     * set to false, copies elements from sketchIn first, leaving original unchanged.
     * @param sketchIn Sketch with new samples from which to draw
     * @param isModifiable Flag indicating whether sketchIn can be modified (e.g. if it was rebuild from Memory)
     */
    private void twoWayMergeInternal(final ReservoirLongsSketch sketchIn, final boolean isModifiable) {
        // TODO: need explicit check on reservoir size to set to min(sketchIn.getK() and gadget_.getK())?
        // TODO: better checks for nulls/empty sketches?
        if (sketchIn.getN() <= sketchIn.getK()) {
            twoWayMergeInternalStandard(sketchIn);
        } else if (gadget_.getN() < gadget_.getK()) {
            // merge into sketchIn, so swap first
            ReservoirLongsSketch tmpSketch = gadget_;
            gadget_ = (isModifiable ? sketchIn : sketchIn.copy());
            twoWayMergeInternalStandard(tmpSketch);
        } else if (sketchIn.getImplicitSampleWeight() < gadget_.getN() / ((double) (gadget_.getK() - 1))) {
            // implicit weights in sketchIn are light enough to merge into gadget
            twoWayMergeInternalWeighted(sketchIn);
        } else {
            // TODO: check next line for an assert/exception?
            // gadget_.getImplicitSampleWeight() < sketchIn.getN() / ((double) (sketchIn.getK() - 1))) {
            // implicit weights in gadget are light enough to merge into sketchIn
            // merge into sketchIn, so swap first
            ReservoirLongsSketch tmpSketch = gadget_;
            gadget_ = (isModifiable ? sketchIn : sketchIn.copy());
            twoWayMergeInternalWeighted(tmpSketch);
        }
    }

    // should be called ONLY by twoWayMergeInternal
    private void twoWayMergeInternalStandard(final ReservoirLongsSketch source) {
        assert (source.getN() <= source.getK());
        int numInputSamples = source.getNumSamples();
        for (int i = 0; i < numInputSamples; ++i) {
            gadget_.update(source.getValueAtPosition(i));
        }
    }

    // should be called ONLY by twoWayMergeInternal
    private void twoWayMergeInternalWeighted(final ReservoirLongsSketch source) {
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
            assert (rescaled_prob < rescaled_one); // TODO: exception to enforce strict lightness?
            double rescaled_flip = rescaled_one * gadget_.rand.nextDouble(); // TODO: move to util class w/ other statics
            if (rescaled_flip < rescaled_prob) {
                // Intentionally NOT doing optimizaiton to extract slot number from rescaled_flip.
                // Grabbing new random bits to ensure all slots in play
                int slotNo = gadget_.rand.nextInt(tgtK);
                gadget_.insertValueAtPosition(source.getValueAtPosition(i), slotNo);
            } // end of inlined weight update
        } // end of loop over source samples


        // targetTotal was fractional but should now be an integer again. Could validate with low tolerance, but for now
        // just round to check.
        long checkN = (long) Math.floor(0.5 + targetTotal);
        gadget_.forceIncrementItemsSeen(source.getN());
        assert (checkN == gadget_.getN());
    }

    public ReservoirLongsSketch getResult() {
        return (gadget_ != null ? gadget_.copy() : null);
    }

}

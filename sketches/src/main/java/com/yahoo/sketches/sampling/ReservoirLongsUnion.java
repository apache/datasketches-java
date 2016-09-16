package com.yahoo.sketches.sampling;

import java.util.Map;

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
            double rescaled_flip = rescaled_one * SamplingUtil.rand.nextDouble(); // TODO: move to util class w/
            // other statics
            if (rescaled_flip < rescaled_prob) {
                // Intentionally NOT doing optimizaiton to extract slot number from rescaled_flip.
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

    /**
     * Returns a sketch representing the current state of the union.
     * @return The result of any unions already processed.
     */
    public ReservoirLongsSketch getResult() {
        return (gadget_ != null ? gadget_.copy() : null);
    }

    /**
     * Returns a byte array representation of this union
     * @return a byte array representation of this union
     */
    public byte[] toByteArray() {
        return (gadget_ != null ? gadget_.toByteArray() : null);
    }

    public static void main(String[] args) {
        int iter = 100000;
        int k = 20;
        java.util.TreeMap<Long, Integer> hist = new java.util.TreeMap<>();

        for (int i = 0; i < iter; ++i) {
            long[] out = simpleUnion(k);

            for (int j = 0; j < k; ++j) {
                long key = out[j];
                if (hist.containsKey(key)) {
                    int count = hist.get(key);
                    hist.put(key, ++count);
                } else {
                    hist.put(key, 1);
                }
            }
        }

        //for (Long l : java.util.Collections.sort(hist.keySet().to)) {
        for (Map.Entry e : hist.entrySet()) {
            System.out.println(e.getKey() + ": " + e.getValue().toString());
        }
        System.out.println("H      = " + computeEntropy(k * iter, hist));
        System.out.println("Theo H = " + Math.log(20 * k) / Math.log(2.0));

    }

    public static double computeEntropy(final long denom, java.util.Map<Long, Integer> data) {
        double H = 0.0;
        double scaleFactor = 1.0 / denom;
        double INV_LN_2 = 1.0 / Math.log(2.0);

        for (int count : data.values()) {
            double p = count * scaleFactor;
            H -= p * Math.log(p) * INV_LN_2;
        }

        return H;
    }

    public static long[] simpleUnion(final int k) {
        ReservoirLongsSketch rls1 = ReservoirLongsSketch.getInstance(k);
        ReservoirLongsSketch rls2 = ReservoirLongsSketch.getInstance(k);

        for (long i = 0; i < 10 * k; ++i) {
            rls1.update((long) i);
            rls2.update((long) k * k + i);
        }

        /*
        System.out.println("Samples 1:");
        long[] data1 = rls1.getSamples();
        long[] data2 = rls2.getSamples();
        for (long l : data1) {
            System.out.println(l);
        }
        System.out.println("Samples 2:");
        for (long l : data2) {
            System.out.println(l);
        }
        */

        ReservoirLongsUnion rlu = new ReservoirLongsUnion(rls1);
        rlu.update(rls2);

        long[] result = rlu.getResult().getSamples();
        /*
        System.out.println("\nResult:");
        for (long l : result) {
            System.out.println(l);
        }
        */

        return result;
    }


}

package com.yahoo.sketches.sampling;

import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.Util;

/**
 * This class provides a compact representation of reservoir size by encoding it into a fixed-point 16-bit value.
 * <p>The value itself is a fractional power of 2, with 5 bits of exponent and 11 bits of mantissa. The exponent
 * allows a choice of anywhere from 0-30, and there are 2048 possible reservoir size values within each octave.
 * Because reservoir size must be an integer, this means that some sizes below 2048 may have multiple valid encodings.
 * </p>
 *
 * <p>Reservoir sizes can be specified exactly to 4096, and may be off by up to 0.03% thereafter. The value returned
 * is always at least as large as the requested size, but may be larger.
 * </p>
 *
 * <p>NOTE: Numerical instability may cause an off-by-one error on reservoir size, causing a slight increase in
 * storage over the optimal value.</p>
 *
 * @author jmalkin
 */
public class ReservoirSize {
    /**
     * Number of bins per power of two.
     */
    static final int BINS_PER_OCTAVE = 2048;

    /**
     * Precomputed inverse values for efficiency
     */
    private static final double INV_BINS_PER_OCTAVE = 1.0 / BINS_PER_OCTAVE;
    private static final double INV_LN_2 = 1.0 / Math.log(2.0);

    /**
     * Values for encoding/decoding
     */
    private static final int EXPONENT_MASK  = 0X1F;
    private static final int EXPONENT_SHIFT = 11;
    private static final int INDEX_MASK     = 0X07FF;
    private static final int OUTPUT_MASK    = 0XFFFF;
    private static final int MAX_ABS_VALUE  = 2146959360;
    private static final int MAX_ENC_VALUE  = 0XF7FF; // p=30, i=2047

    /**
     * Given target reservoir size k, computes the smallest representable reservoir size that can hold k entries and
     * returns it in a 16-bit fixed-point format.
     * @param k target reservoir size
     * @return reservoir size as 16-bit encoded value
     */
    public static int computeSize(final int k) throws SketchesArgumentException {
        if (k < 1 || k > MAX_ABS_VALUE) {
            throw new SketchesArgumentException("Can only encode strictly positive sketch sizes less than "
                    + MAX_ABS_VALUE + ": "  + k);
        }

        int p = Util.toLog2(Util.floorPowerOf2(k), "computeSize: p");

        // because of floor() + 1 below, need to check power of 2 here
        if (Util.isPowerOf2(k)) {
            return ((p & EXPONENT_MASK) << EXPONENT_SHIFT) & OUTPUT_MASK;
        }

        // mantissa is scalar in range [1,2); can reconstruct k as m * 2^p
        double m = Math.pow(2.0, Math.log(k) * INV_LN_2 - p);

        // Convert to index offset: ceil(m * BPO) - BPO
        // Typically in range range 0-(BINS_PER_OCTAVE-1) (but see note below)
        int i = (int) Math.floor(m * BINS_PER_OCTAVE) - BINS_PER_OCTAVE + 1;

        // Due to ceiling, possible to overflow BINS_PER_OCTAVE
        // E.g., if BPO = 2048 then for k=32767 we have p=14. Except that 32767 > decodeValue(p=14, i=2047)=32756,
        // so we encode and return p+1
        if (i == BINS_PER_OCTAVE) {
            return (((p + 1) & EXPONENT_MASK) << EXPONENT_SHIFT) & OUTPUT_MASK;
        }

        return ((p & EXPONENT_MASK) << EXPONENT_SHIFT) | (i & INDEX_MASK) & OUTPUT_MASK;
    }

    /**
     * Decodes the 16-bit reservoir size value into an int.
     * @param value Encoded 16-bit value
     * @return int represented by value
     */
    public static int decodeValue(final int value) {
        if (value < 0 || value > MAX_ENC_VALUE) {
            throw new SketchesArgumentException("Value to decode must fit in an unsigned short: " + value);
        }

        int p = (value >> EXPONENT_SHIFT) & EXPONENT_MASK;
        int i = value & INDEX_MASK;

        return (int) ((1 << p) * (i * INV_BINS_PER_OCTAVE + 1.0));
    }
}

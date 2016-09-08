package com.yahoo.sketches.sampling;

/**
 * Class to union reservoir samples. Because the union process picks one of the two sketches to use as the base,
 * we provide only a union; a merge would result in unpredictable side effects on the underlying sketches.
 *
 * If taking the uinon of two reservoirs of different sizes, the output sample will contain no more than
 * MIN(k_1, k_2) samples.
 *
 * Created by jmalkin on 8/24/16.
 */
public class ReservoirLongsUnion {
}

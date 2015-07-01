/**
 * <p>The hll package contains a very compact implementation of Phillipe Flajolet's
 * HLL sketch but with significantly improved error behavior.  If the ONLY use case for sketching is
 * counting uniques and merging, the HLL sketch is the highest performing in terms of accuracy for 
 * space consumed.  For large counts, this HLL version will be 16 times smaller for the same 
 * accuracy than the Theta Sketches mentioned above.</p>
 * 
 * <p>However, large data with many dimensions and dimension coordinates are often highly skewed 
 * in their distribution of unique values. 
 * In this case many sketches tend to have only a few entries. Averaged over all the sketches of 
 * this data, the size advantage of the HLL can be significantly reduced down to near parity with 
 * theta sketches. This property is strictly a function of the distribution of the input data so it
 * is advisable to understand and measure this phenomenon.
 * The HLL sketch is not recommended if you anticipate the need of performing set intersection 
 * or difference operations with reasonable accuracy, 
 * or if you anticipate the need to merge sketches that were constructed with different 
 * values of <i>k</i> or <i>Nominal Entries</i>.  HLL sketches do not retain any of the sampled
 * hash values of the associated unique identifiers, so if there is any anticipation of the need
 * to leverage associations with these sampled hash values, Theta Sketches would be a better choice.
 * HLL sketches cannot be intermixed or merged in any way with Theta Sketches.
 * </p>
 * 
 * @author Lee Rhodes
 */
package com.yahoo.sketches.hll;
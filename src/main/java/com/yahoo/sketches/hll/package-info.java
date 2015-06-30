/**
 * <p>The hll package contains a straight-forward implementation of Phillipe Flajolet's
 * HLL sketch but with significantly improved error behavior.  If the only use case for sketching is
 * counting uniques and merging, the HLL sketch is the highest performing in terms of accuracy for 
 * space consumed.  For large counts, this HLL version will be eight times smaller for the same 
 * accuracy than the Theta Sketches mentioned above.  
 * However, the HLL sketch is not recommended if you anticipate the need of performing set operations 
 * with reasonable accuracy or if you anticipate the need to merge sketches 
 * that were constructed with different values of <i>k</i> or <i>Nominal Entries</i>.  
 * This HLL implementation is provided for those that want to use it, but it cannot be intermixed with
 * the Theta Sketches.
 * </p>
 * 
 * @author Lee Rhodes
 */
package com.yahoo.sketches.hll;
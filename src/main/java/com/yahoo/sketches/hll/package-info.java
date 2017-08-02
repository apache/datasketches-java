/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

/**
 * The hll package contains a high performance implementation of Phillipe
 * Flajolet's HLL sketch with significantly improved error behavior.
 *
 * <p>If the ONLY use case for sketching is counting uniques and merging, the
 * HLL sketch is the highest performing in terms of accuracy for space
 * consumed. For large counts, this HLL version will be 2 to 16 times
 * smaller for the same accuracy than the Theta Sketches.
 *
 * <p>HLL sketches do not retain any of the hash values of the associated unique identifiers,
 * so if there is any anticipation of a future need to leverage associations with these
 * retained hash values, Theta Sketches would be a better choice.</p>
 *
 * <p>HLL sketches cannot be intermixed or merged in any way with Theta Sketches.
 * </p>
 *
 * @author Lee Rhodes
 * @author Kevin Lang
 */

package com.yahoo.sketches.hll;

/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

/**
 * <p>The hllmap package contains a space efficient HLL mapping sketch of keys to approximate unique
 * count of identifiers. For example, counting the number of unique users (identifiers) per IP
 * address.
 *
 * <p>In cases where the number of keys is very large, having an individual HLL sketch per key may
 * not be practical. If the distribution of values per key is highly skewed where the vast
 * majority of keys have only a few values then this mapping sketch will make sense as it will be
 * far more space efficient than dedicating individual HLL sketches per key.
 *
 * <p>From our own testing, sketching 100 million IPv4 addresses with such a
 * highly skewed distribution of identifiers per IP uses only 1.4GB of memory. This translates to
 * an average of about 10 bytes per IP allocated to the equivalent of a full k=1024 HLL sketch
 * and provides an RSE of less than 2.5%. Your results will vary depending on the actual
 * distribution of identifiers per key.
 *
 * @see com.yahoo.sketches.hllmap.UniqueCountMap
 *
 * @author Lee Rhodes
 * @author Kevin Lang
 * @author Alexander Saydakov
 */
package com.yahoo.sketches.hllmap;

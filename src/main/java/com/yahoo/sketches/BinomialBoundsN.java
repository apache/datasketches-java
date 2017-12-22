/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches;

/**
 * This class enables the estimation of error bounds given a sample set size, the sampling
 * probability theta, the number of standard deviations and a simple noDataSeen flag.  This can
 * be used to estimate error bounds for fixed threshold sampling as well as the error bounds
 * calculations for sketches.
 *
 * @author Kevin Lang
 */
// BTW, the suffixes "NStar", "NPrimeB", and "NPrimeF" correspond to variables in the formal
// writeup of this scheme.
public final class BinomialBoundsN {

  private BinomialBoundsN() {}

  private static double[] deltaOfNumSDev =
  {
    0.5000000000000000000, // not actually using this value
    0.1586553191586026479,
    0.0227502618904135701,
    0.0013498126861731796
  };

  // our "classic" bounds, but now with continuity correction

  private static double contClassicLB(final double numSamplesF, final double theta,
      final double numSDev) {
    final double nHat = (numSamplesF - 0.5) / theta;
    final double b = numSDev * Math.sqrt((1.0 - theta) / theta);
    final double d  = 0.5 * b * Math.sqrt((b * b) + (4.0 * nHat));
    final double center = nHat + (0.5 * (b * b));
    return (center - d);
  }

  private static double contClassicUB(final double numSamplesF, final double theta,
      final double numSDev) {
    final double nHat = (numSamplesF + 0.5) / theta;
    final double b   = numSDev * Math.sqrt((1.0 - theta) / theta);
    final double d  = 0.5 * b * Math.sqrt((b * b) + (4.0 * nHat));
    final double center = nHat + (0.5 * (b * b));
    return (center + d);
  }

  // This is a special purpose calculator for NStar, using a computational
  // strategy inspired by its Bayesian definition. It is only appropriate
  // for a very limited set of inputs. However, the procedure computeApproxBinoLB ()
  // below does in fact only call it for suitably limited inputs.
  // Outside of this limited range, two different bad things will happen.
  // First, because we are not using logarithms, the values of intermediate
  // quantities will exceed the dynamic range of doubles. Second, even if that
  // problem were fixed, the running time of this procedure is essentially linear
  // in est = (numSamples / p), and that can be Very, Very Big.

  private static long specialNStar(final long numSamplesI, final double p, final double delta) {
    final double q, numSamplesF;
    double tot, curTerm;
    long m;
    assertTrue(numSamplesI >= 1);
    assertTrue((0.0 < p) && (p < 1.0));
    assertTrue((0.0 < delta) && (delta < 1.0));
    q = 1.0 - p;
    numSamplesF = numSamplesI;
    // Use a different algorithm if the following isn't true; this one will be too slow, or worse.
    assertTrue((numSamplesF / p) < 500.0);
    curTerm = Math.pow(p, numSamplesF);  // curTerm = posteriorProbability (k, k, p)
    assertTrue(curTerm > 1e-100); // sanity check for non-use of logarithms
    tot = curTerm;
    m = numSamplesI;
    while (tot <= delta) { // this test can fail even the first time
      curTerm = (curTerm * q * (m)) / ((m + 1) - numSamplesI);
      tot += curTerm;
      m += 1;
    }
    // we have reached a state where tot > delta, so back up one
    return (m - 1);
  }

  //  The following procedure has very limited applicability.
  //  The above remarks about specialNStar() also apply here.
  private static long specialNPrimeB(final long numSamplesI, final double p, final double delta) {
    final double q, numSamplesF, oneMinusDelta;
    double tot, curTerm;
    long m;
    assertTrue(numSamplesI >= 1);
    assertTrue((0.0 < p) && (p < 1.0));
    assertTrue((0.0 < delta) && (delta < 1.0));
    q = 1.0 - p;
    oneMinusDelta = 1.0 - delta;
    numSamplesF = numSamplesI;
    curTerm = Math.pow(p, numSamplesF);  // curTerm = posteriorProbability (k, k, p)
    assertTrue(curTerm > 1e-100); // sanity check for non-use of logarithms
    tot = curTerm;
    m = numSamplesI;
    while (tot < oneMinusDelta) {
      curTerm = (curTerm * q * (m)) / ((m + 1) - numSamplesI);
      tot += curTerm;
      m += 1;
    }
    return (m); // don't need to back up
  }

  private static long specialNPrimeF(final long numSamplesI, final double p, final double delta) {
    // Use a different algorithm if the following isn't true; this one will be too slow, or worse.
    assertTrue(((numSamplesI) / p) < 500.0); //A super-small delta could also make it slow.
    return (specialNPrimeB(numSamplesI + 1, p, delta));
  }

  // The following computes an approximation to the lower bound of
  // a Frequentist confidence interval based on the tails of the Binomial distribution.
  private static double computeApproxBinoLB(final long numSamplesI, final double theta,
      final int numSDev) {
    if (theta == 1.0) {
      return (numSamplesI);
    }

    else if (numSamplesI == 0) {
      return (0.0);
    }

    else if (numSamplesI == 1) {
      final double delta = deltaOfNumSDev[numSDev];
      final double rawLB = (Math.log(1.0 - delta)) / (Math.log(1.0 - theta));
      return (Math.floor(rawLB)); // round down
    }

    else if (numSamplesI > 120) {
      // plenty of samples, so gaussian approximation to binomial distribution isn't too bad
      final double rawLB = contClassicLB( numSamplesI, theta, numSDev);
      return (rawLB - 0.5); // fake round down
    }

    // at this point we know 2 <= numSamplesI <= 120

    else if (theta > (1.0 - 1e-5)) {  // empirically-determined threshold
      return (numSamplesI);
    }

    else if (theta < ((numSamplesI) / 360.0)) {  // empirically-determined threshold
      // here we use the gaussian approximation, but with a modified "numSDev"
      final int index;
      final double rawLB;
      index = (3 * ((int) numSamplesI)) + (numSDev - 1);
      rawLB = contClassicLB(numSamplesI, theta, EquivTables.getLB(index));
      return (rawLB - 0.5); // fake round down
    }

    else { // This is the most difficult range to approximate; we will compute an "exact" LB.
      // We know that est <= 360, so specialNStar() shouldn't be ridiculously slow.
      final double delta = deltaOfNumSDev[numSDev];
      final long nstar = specialNStar(numSamplesI, theta, delta);
      return (nstar); // don't need to round
    }
  }

  // The following computes an approximation to the upper bound of
  // a Frequentist confidence interval based on the tails of the Binomial distribution.
  private static double computeApproxBinoUB(final long numSamplesI, final double theta,
      final int numSDev) {
    if (theta == 1.0) {
      return (numSamplesI);
    }

    else if (numSamplesI == 0) {
      final double delta = deltaOfNumSDev[numSDev];
      final double rawUB = (Math.log(delta)) / (Math.log(1.0 - theta));
      return (Math.ceil(rawUB)); // round up
    }

    else if (numSamplesI > 120) {
      // plenty of samples, so gaussian approximation to binomial distribution isn't too bad
      final double rawUB = contClassicUB(numSamplesI, theta, numSDev);
      return (rawUB + 0.5); // fake round up
    }

    // at this point we know 1 <= numSamplesI <= 120

    else if (theta > (1.0 - 1e-5)) { // empirically-determined threshold
      return (numSamplesI + 1);
    }

    else if (theta < ((numSamplesI) / 360.0)) {  // empirically-determined threshold
      // here we use the gaussian approximation, but with a modified "numSDev"
      final int index;
      final double rawUB;
      index = (3 * ((int) numSamplesI)) + (numSDev - 1);
      rawUB = contClassicUB(numSamplesI, theta, EquivTables.getUB(index));
      return (rawUB + 0.5); // fake round up
    }

    else { // This is the most difficult range to approximate; we will compute an "exact" UB.
      // We know that est <= 360, so specialNPrimeF() shouldn't be ridiculously slow.
      final double delta = deltaOfNumSDev[numSDev];
      final long nprimef = specialNPrimeF(numSamplesI, theta, delta);
      return (nprimef); // don't need to round
    }
  }

  // The following two procedures enforce some extra rules that help
  // to prevent the return of bounds that might be confusing to users.
  /**
   * Returns the approximate lower bound value
   * @param numSamples the number of samples in the sample set
   * @param theta the sampling probability
   * @param numSDev the number of "standard deviations" from the mean for the tail bounds.
   * This must be an integer value of 1, 2 or 3.
   * @param noDataSeen this is normally false. However, in the case where you have zero samples
   * and a theta &lt; 1.0, this flag enables the distinction between a virgin case when no actual
   * data has been seen and the case where the estimate may be zero but an upper error bound may
   * still exist.
   * @return the approximate upper bound value
   */
  public static double getLowerBound(final long numSamples, final double theta, final int numSDev,
      final boolean noDataSeen) {
    //in earlier code numSamples was called numSamplesI
    if (noDataSeen) { return 0.0; }
    checkArgs(numSamples, theta, numSDev);
    final double lb = computeApproxBinoLB(numSamples, theta, numSDev);
    final double numSamplesF = numSamples;
    final double est = numSamplesF / theta;
    return (Math.min(est, Math.max(numSamplesF, lb)));
  }

  /**
   * Returns the approximate upper bound value
   * @param numSamples the number of samples in the sample set
   * @param theta the sampling probability
   * @param numSDev the number of "standard deviations" from the mean for the tail bounds.
   * This must be an integer value of 1, 2 or 3.
   * @param noDataSeen this is normally false. However, in the case where you have zero samples
   * and a theta &lt; 1.0, this flag enables the distinction between a virgin case when no actual
   * data has been seen and the case where the estimate may be zero but an upper error bound may
   * still exist.
   * @return the approximate upper bound value
   */
  public static double getUpperBound(final long numSamples, final double theta, final int numSDev,
      final boolean noDataSeen) {
    //in earlier code numSamples was called numSamplesI
    if (noDataSeen) { return 0.0; }
    checkArgs(numSamples, theta, numSDev);
    final double ub = computeApproxBinoUB(numSamples, theta, numSDev);
    final double numSamplesF = numSamples;
    final double est = numSamplesF / theta;
    return (Math.max(est, ub));
  }

  //exposed only for test
  static final void checkArgs(final long numSamples, final double theta, final int numSDev) {
    if ((numSDev | (numSDev - 1) | (3 - numSDev) | numSamples) < 0) {
      throw new SketchesArgumentException(
          "numSDev must only be 1,2, or 3 and numSamples must >= 0: numSDev="
              + numSDev + ", numSamples=" + numSamples);
    }
    if ((theta < 0.0) || (theta > 1.0)) {
      throw new SketchesArgumentException("0.0 < theta <= 1.0: " + theta);
    }
  }

  private static void assertTrue(final boolean truth) {
    assert (truth);
  }

} // end of class "BinomialBoundsN"

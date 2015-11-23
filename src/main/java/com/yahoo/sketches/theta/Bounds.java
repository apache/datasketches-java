/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;


/**
 * @author Kevin Lang
 */
@SuppressWarnings({"cast"})
final class Bounds {
  
  private Bounds() {}
  
  private static double[] deltaOfNumSDev = 
  {
    0.5000000000000000000, // not actually using this value
    0.1586553191586026479,
    0.0227502618904135701,
    0.0013498126861731796
  };
  
  // our "classic" bounds, but now with continuity correction
  
  private static double contClassicLB(double numSamplesF, double theta, double numSDev) {
    double nHat = (numSamplesF - 0.5) / theta;
    double b = numSDev * Math.sqrt ((1.0 - theta) / theta);
    double d  = 0.5 * b * Math.sqrt (b * b + 4.0 * nHat);
    double center = nHat + 0.5 * (b * b);
    return (center - d);
  }
  
  private static double contClassicUB(double numSamplesF, double theta, double numSDev) {
    double nHat = (numSamplesF + 0.5) / theta;
    double b   = numSDev * Math.sqrt ((1.0 - theta) / theta);
    double d  = 0.5 * b * Math.sqrt (b * b + 4.0 * nHat);
    double center = nHat + 0.5 * (b * b);
    return (center + d);
  }

  // This is a special purpose calculator for n_star, using a computational
  // strategy inspired by its Bayesian definition. It is only appropriate 
  // for a very limited set of inputs. However, the procedure computeApproxBinoLB ()
  // below does in fact only call it for suitably limited inputs.
  // Outside of this limited range, two different bad things will happen.
  // First, because we are not using logarithms, the values of intermediate
  // quantities will exceed the dynamic range of doubles. Second, even if that
  // problem were fixed, the running time of this procedure is essentially linear
  // in est = (numSamples / p), and that can be Very, Very Big.
      
  // BTW, these names "n_star", "n_prime_b", and "n_prime_f" correspond to symbols
  // that are defined and discussed in the technical writeup of this scheme.
  private static long specialNStar(long numSamplesI, double p, double delta) {
    double q, tot, numSamplesF, curTerm;
    long m;
    assertTrue (numSamplesI >= 1);
    assertTrue (0.0 < p && p < 1.0);
    assertTrue (0.0 < delta && delta < 1.0);
    q = 1.0 - p;
    numSamplesF = (double) numSamplesI;
    // Use a different algorithm if the following isn't true; this one will be too slow, or worse. 
    assertTrue ((numSamplesF / p) < 500.0); 
    curTerm = Math.pow (p, numSamplesF);  // curTerm = posteriorProbability (k, k, p) 
    assertTrue (curTerm > 1e-100); // sanity check for non-use of logarithms 
    tot = curTerm;
    m = numSamplesI;
    while (tot <= delta) { // this test can fail even the first time 
      curTerm = curTerm * q * ((double) m) / ((double) (m + 1 - numSamplesI));
      tot += curTerm;
      m += 1;    
    }
    // we have reached a state where tot > delta, so back up one 
    return (m-1);
  }
  
  //  The following procedure has very limited applicability. 
  //  The above remarks about specialNStar() also apply here.
  private static long specialNPrimeB(long numSamplesI, double p, double delta) {
    double q, tot, numSamplesF, curTerm, oneMinusDelta;
    long m;
    assertTrue (numSamplesI >= 1);
    assertTrue (0.0 < p && p < 1.0);
    assertTrue (0.0 < delta && delta < 1.0);
    q = 1.0 - p;
    oneMinusDelta = 1.0 - delta;
    numSamplesF = (double) numSamplesI;
    curTerm = Math.pow (p, numSamplesF);  // curTerm = posteriorProbability (k, k, p)
    assertTrue (curTerm > 1e-100); // sanity check for non-use of logarithms
    tot = curTerm;
    m = numSamplesI;
    while (tot < oneMinusDelta) {
      curTerm = curTerm * q * ((double) m) / ((double) (m + 1 - numSamplesI));
      tot += curTerm;
      m += 1;    
    }
    return (m); // don't need to back up
  }
  
  private static long specialNPrimeF(long numSamplesI, double p, double delta) {
    /* Use a different algorithm if the following isn't true; this one will be too slow, or worse. */
    assertTrue ((((double) numSamplesI) / p) < 500.0);    /* A super-small delta could also make it slow. */
    return (specialNPrimeB (numSamplesI+1, p, delta));
  }
  
  // The following computes an approximation to the lower bound of 
  // a Frequentist confidence interval based on the tails of the Binomial distribtuion.
  private static double computeApproxBinoLB(long numSamplesI, double theta, int numSDev) {
    assertTrue (numSDev >= 1 && numSDev <= 3);
    assertTrue (numSamplesI >= 0);
    assertTrue (0.0 < theta && theta <= 1.0);

    if (theta == 1.0) {
      return ((double) numSamplesI);
    }

    else if (numSamplesI == 0) {
      return (0.0);
    }  

    else if (numSamplesI == 1) {
      double delta = deltaOfNumSDev[numSDev];
      double rawLB = (Math.log(1.0 - delta)) / (Math.log(1.0 - theta));
      return (Math.floor(rawLB)); // round down 
    }

    else if (numSamplesI > 120) {
      // plenty of samples, so gaussian approximation to binomial distribution isn't too bad 
      double rawLB = contClassicLB ((double) numSamplesI, theta, (double) numSDev);
      return (rawLB - 0.5); // fake round down 
    }

    // at this point we know 2 <= numSamplesI <= 120 

    else if (theta > (1.0 - 1e-5)) {  // empirically-determined threshold 
      return ((double) numSamplesI);
    }  

    else if (theta < ((double) numSamplesI) / 360.0) {  // empirically-determined threshold 
      // here we use the gaussian approximation, but with a modified "numSDev" 
      int index;
      double rawLB;
      index = 3 * ((int) numSamplesI) + (numSDev - 1);
      rawLB = contClassicLB ((double) numSamplesI, theta, EquivTables.lbEquivTable[index]);
      return (rawLB - 0.5); // fake round down 
    }

    else { // This is the most difficult range to approximate; we will compute an "exact" LB. 
      // We know that est <= 360, so specialNStar() shouldn't be ridiculously slow. 
      double delta = deltaOfNumSDev[numSDev];
      long nstar = specialNStar (numSamplesI, theta, delta);
      return ((double) nstar); // don't need to round 
    }
  }
  
  // The following computes an approximation to the upper bound of
  // a Frequentist confidence interval based on the tails of the Binomial distribution.
  private static double computeApproxBinoUB(long numSamplesI, double theta, int numSDev) {
    assertTrue (numSDev >= 1 && numSDev <= 3);
    assertTrue (numSamplesI >= 0);
    assertTrue (0.0 < theta && theta <= 1.0);

    if (theta == 1.0) {
      return ((double) numSamplesI);
    }

    else if (numSamplesI == 0) {
      double delta = deltaOfNumSDev[numSDev];
      double rawUB = (Math.log(delta)) / (Math.log(1.0 - theta));
      return (Math.ceil(rawUB)); // round up 
    }

    else if (numSamplesI > 120) {
      // plenty of samples, so gaussian approximation to binomial distribution isn't too bad 
      double rawUB = contClassicUB ((double) numSamplesI, theta, (double) numSDev);
      return (rawUB + 0.5); // fake round up 
    }

    // at this point we know 1 <= numSamplesI <= 120 

    else if (theta > (1.0 - 1e-5)) { // empirically-determined threshold 
      return ((double) (numSamplesI+1));
    }  

    else if (theta < ((double) numSamplesI) / 360.0) {  // empirically-determined threshold 
      // here we use the gaussian approximation, but with a modified "numSDev" 
      int index; 
      double rawUB;
      index = 3 * ((int) numSamplesI) + (numSDev - 1);
      rawUB = contClassicUB ((double) numSamplesI, theta, EquivTables.ubEquivTable[index]);
      return (rawUB + 0.5); // fake round up 
    }

    else { // This is the most difficult range to approximate; we will compute an "exact" UB. 
        // We know that est <= 360, so specialNPrimeF() shouldn't be ridiculously slow. 
      double delta = deltaOfNumSDev[numSDev];
      long nprimef = specialNPrimeF (numSamplesI, theta, delta);
      return ((double) nprimef); // don't need to round 
    }
  }

  // The following two procedures enforce some extra rules that help
  // to prevent the return of bounds that might be confusing to users.
  static double approxLBforUsers(
      long numSamplesI, double theta, int numSDev, boolean noDataSeen) {
    if (noDataSeen) return 0.0;
    double lb = computeApproxBinoLB (numSamplesI, theta, numSDev);
    double numSamplesF = (double) numSamplesI;
    double est = numSamplesF / theta;
    return (Math.min (est, Math.max (numSamplesF, lb)));
  }
  
  static double approxUBforUsers(
      long numSamplesI, double theta, int numSDev, boolean noDataSeen) {
    if (noDataSeen) return 0.0;
    double ub = computeApproxBinoUB (numSamplesI, theta, numSDev);
    double numSamplesF = (double) numSamplesI;
    double est = numSamplesF / theta;
    return (Math.max (est, ub));
  }
  
  private static void assertTrue(final boolean truth) {
    assert(truth);
  }
  
} // end of class "Bounds"
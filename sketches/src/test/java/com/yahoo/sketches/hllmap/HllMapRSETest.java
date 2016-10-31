/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hllmap;

import static com.yahoo.sketches.hllmap.MapTestingUtil.LS;
import static com.yahoo.sketches.hllmap.MapTestingUtil.bytesToString;
import static com.yahoo.sketches.hllmap.MapTestingUtil.evenlyLgSpaced;
import static com.yahoo.sketches.hllmap.MapTestingUtil.intToBytes;
import static com.yahoo.sketches.hllmap.MapTestingUtil.longToBytes;
import static com.yahoo.sketches.hllmap.MapTestingUtil.milliSecToString;

//import org.testng.annotations.Test;

import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.hll.HllSketch;
import com.yahoo.sketches.hll.HllSketchBuilder;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.Sketches;
import com.yahoo.sketches.theta.UpdateSketch;
import com.yahoo.sketches.theta.UpdateSketchBuilder;

public class HllMapRSETest {

  enum SketchEnum { HLL_MAP, THETA, HLL, COUPON_HASH_MAP, UNIQUE_COUNT_MAP}

  //@Test
  public void testHllMap() {
    testRSE(SketchEnum.HLL_MAP);
    println(LS);
  }

  //@Test
  public void testTheta() {
    testRSE(SketchEnum.THETA);
    println(LS);
  }

  //@Test
  public void testHll() {
    testRSE(SketchEnum.HLL);
    println(LS);
  }

  //@Test
  public void testCouponHashMap() {
    testRSE(SketchEnum.COUPON_HASH_MAP);
    println(LS);
  }

  //@Test
  public void testUniqueCountMap() {
    testRSE(SketchEnum.UNIQUE_COUNT_MAP);
    println(LS);
  }

  @SuppressWarnings("null")
  public void testRSE(SketchEnum skEnum) {
    /***************************************/
    //test parameters
    int startLgX = 0; //1
    int endLgX = 16;  //was 16 = 65K # of uniques per trial
    if (skEnum == SketchEnum.COUPON_HASH_MAP) {
      endLgX = 7;
    }
    int startLgTrials = 20; //was 16 # of Keys
    int endLgTrials = 10;
    int ppo = 4; //Points per Octave

    int points = ppo * (endLgX - startLgX) + 1;
    int[] xPoints = evenlyLgSpaced(startLgX, endLgX, points);
    int[] tPoints = evenlyLgSpaced(startLgTrials, endLgTrials, points);

    //HllMap config
    int keySize = 4;
    int lgK = 10;
    int k = 1 << lgK;
    float rf = 2.0F;
    int initEntries = 1 << (startLgTrials +1);

    /***************************************/
    //Other
    Map map = null;
    UniqueCountMap ucMap = null;
    UpdateSketchBuilder thBldr =
        Sketches.updateSketchBuilder().setResizeFactor(ResizeFactor.X1).setNominalEntries(k);
    UpdateSketch thSketch = null;
    HllSketchBuilder hllBldr = HllSketch.builder().setLogBuckets(lgK).setHipEstimator(true);
    HllSketch hllSk = null;
    long v = 0;
    byte[] ipv4bytes = new byte[4];
    //byte[] ipv6bytes = new byte[16];
    byte[] valBytes = new byte[8];

    if (skEnum == SketchEnum.HLL_MAP) {
      println("HllMap: k:\t" + k);
    }
    else if (skEnum == SketchEnum.THETA) {
      thSketch = thBldr.build();       //set here because we can reset it
      println("Theta Sketch: k:\t" + k);
    }
    else if (skEnum == SketchEnum.HLL){
      println("HLL Sketch: k:\t" + k);
    }
    else if (skEnum == SketchEnum.COUPON_HASH_MAP) {
      println("CouponHashMap: k:\t" + k);
    }
    else { //Unique Count Map
      println("UniqueCountMap: k:\t" + k);
    }

    println("U\tTrials\tMean\tBias\tRSE");
    double sum=0, sumErr=0,sumErrSq=0;

    //at each point do multiple trials.
    long startMs = System.currentTimeMillis(); //start the clock
    long totnS = 0;
    long lastX = 0;
    long memUsage = 0;
    for (int pt = 0; pt < points ; pt++) {
      int x = xPoints[pt];
      if (x == lastX) continue;
      lastX = x;
      sum = sumErr = sumErrSq = 0;
      int trials = tPoints[pt];
      int ipv4 = 10 << 24; //10.0.0.0

      if (skEnum == SketchEnum.HLL_MAP) {
        map = HllMap.getInstance(initEntries, keySize, k, rf); //renew per trial set
      }

      else if (skEnum == SketchEnum.COUPON_HASH_MAP) {
        map = CouponHashMap.getInstance(4, 256); //renew per trial set
      }

      else if (skEnum == SketchEnum.UNIQUE_COUNT_MAP) {
        ucMap = new UniqueCountMap(10, 4, k); //renew per trial set
      }
      //else do nothing to the other sketches

      for (int t = 0; t < trials; t++) { //each trial
        double est = 0;
        long startnS = 0, endnS = 0;

        if (skEnum == SketchEnum.HLL_MAP) {
          ipv4++;  //different IP for each trial
          HllMap hllMap = (HllMap) map;
          ipv4bytes = intToBytes(ipv4, ipv4bytes);
          startnS = System.nanoTime();
          int index = hllMap.findOrInsertKey(ipv4bytes);
          for (long i=0; i< x; i++) { //x is the #uniques per trial
            v++;  //different values for the uniques
            valBytes = longToBytes(v, valBytes);
            int coupon = Map.coupon16(valBytes);
            est = hllMap.findOrInsertCoupon(index, (short)coupon);
            //est = map.update(ipv4bytes, coupon);
          }
          endnS = System.nanoTime();
          memUsage = hllMap.getMemoryUsageBytes();
        }

        else if (skEnum == SketchEnum.COUPON_HASH_MAP) {
          ipv4++;  //different IP for each trial
          ipv4bytes = intToBytes(ipv4, ipv4bytes);
          CouponMap cMap = (CouponMap) map;
          startnS = System.nanoTime();
          int index = cMap.findOrInsertKey(ipv4bytes);
          for (long i=0; i< x; i++) { //x is the #uniques per trial
            v++;  //different values for the uniques
            valBytes = longToBytes(v, valBytes);
            int coupon = Map.coupon16(valBytes);
            est = cMap.findOrInsertCoupon(index, (short)coupon);
            //est = map.update(ipv4bytes, coupon);
          }
          endnS = System.nanoTime();
          memUsage = map.getMemoryUsageBytes();
        }

        else if (skEnum == SketchEnum.UNIQUE_COUNT_MAP) {
          ipv4++;  //different IP for each trial
          ipv4bytes = intToBytes(ipv4, ipv4bytes);
          startnS = System.nanoTime();
          for (long i=0; i< x; i++) { //x is the #uniques per trial
            v++;  //different values for the uniques
            valBytes = longToBytes(v, valBytes);
            est = ucMap.update(ipv4bytes, valBytes);
          }
          endnS = System.nanoTime();
          memUsage = ucMap.getMemoryUsageBytes();
        }

        else if (skEnum == SketchEnum.THETA) {
          thSketch.reset();
          startnS = System.nanoTime();
          for (long i=0; i< x; i++) { //x is the #uniques per trial
            v++;  //different values for the uniques
            thSketch.update(v);
          }
          endnS = System.nanoTime();
        }

        else { // if (skEnum == SketchEnum.HLL) {
          hllSk = hllBldr.build(); //no reset on HLL !
          startnS = System.nanoTime();
          for (long i=0; i< x; i++) { //x is the #uniques per trial
            v++;  //different values for the uniques
            hllSk.update(v);
          }
          endnS = System.nanoTime();
        }

        totnS += endnS - startnS;

        if (skEnum == SketchEnum.THETA) {
          thSketch.rebuild();
          est = thSketch.getEstimate();
        }

        else if (skEnum == SketchEnum.HLL) {
          est = hllSk.getEstimate();
        }

        sum += est;
        double err = est - x;
        sumErr += err;
        sumErrSq += err * err;
      }
      double mean = sum /trials;
      double meanErr = sumErr/trials;
      double varErr = (sumErrSq - meanErr * sumErr/trials)/(trials -1);
      double relErr = Math.sqrt(varErr)/x;
      double bias = mean/x - 1.0;

      String line = String.format("%d\t%d\t%.2f\t%.2f%%\t%.2f%%\t%,d", x, trials, mean, bias*100, relErr*100, memUsage);
      println(line);
    }
    println(String.format("\nUpdates          :\t%,d", v));
    if ((skEnum == SketchEnum.HLL_MAP) || (skEnum == SketchEnum.COUPON_HASH_MAP)) {
      println(String.format("Table  Entries   :\t%,d",map.getTableEntries()));
      println(String.format("Capacity Entries :\t%,d",map.getCapacityEntries()));
      println(String.format("Count Entries    :\t%,d",map.getCurrentCountEntries()));
      println(              "Entry bytes      :\t" + map.getEntrySizeBytes());
      println(String.format("RAM Usage Bytes  :\t%,d",map.getMemoryUsageBytes()));
    }

    else if (skEnum == SketchEnum.UNIQUE_COUNT_MAP) {

    }

    else if (skEnum == SketchEnum.THETA) {
      println(String.format("Sketch Size Bytes:\t%,d", Sketch.getMaxUpdateSketchBytes(k)));
    }

    else { //HLL
      println(String.format("Sketch Size Bytes:\t%,d", k + 16));
    }

    long endMs = System.currentTimeMillis();
    long deltamS = endMs - startMs;
    double updnS2 = ((double)totnS)/v;
    println(String.format(  "nS/Update        :\t%.1f", updnS2));
    println(                "Total: H:M:S.mS  :\t"+milliSecToString(deltamS));
  }

  @SuppressWarnings("unused")
  private static void printIPandValue(byte[] ip, byte[] value) {
    println("IP:\t"+bytesToString(ip, false, false, ".")
      + "\tVal:\t"+bytesToString(value, false, false, "."));
  }

  public static void main(String[] args) {
    HllMapRSETest test = new HllMapRSETest();
    test.testHllMap();
//    test.testTheta();
//    test.testHll();
//    test.testCouponHashMap();
//    test.testUniqueCountMap();
  }

  static void println(String s) { System.out.println(s); }
  static void print(String s) { System.out.print(s); }
}

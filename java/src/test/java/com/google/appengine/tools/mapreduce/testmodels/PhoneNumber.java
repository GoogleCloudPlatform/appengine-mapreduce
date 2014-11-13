package com.google.appengine.tools.mapreduce.testmodels;

public class PhoneNumber {
  public int areaCode;
  public int number;
  /**
   * @param areaCode
   * @param number
   */
  public PhoneNumber(int areaCode, int number) {
    this.areaCode = areaCode;
    this.number = number;
  }
  
}

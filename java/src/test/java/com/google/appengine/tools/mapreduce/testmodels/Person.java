package com.google.appengine.tools.mapreduce.testmodels;


public class Person {
  public String fullName;
  public int age;
  public double height;
  public float weight;
  public String gender;
  public PhoneNumber phoneNumber;
  /**
   * @param fullName
   * @param age
   * @param height
   * @param weight
   * @param gender
   * @param phoneNumber
   */
  public Person(String fullName,
      int age,
      double height,
      float weight,
      String gender,
      PhoneNumber phoneNumber) {
    this.fullName = fullName;
    this.age = age;
    this.height = height;
    this.weight = weight;
    this.gender = gender;
    this.phoneNumber = phoneNumber;
  }
}
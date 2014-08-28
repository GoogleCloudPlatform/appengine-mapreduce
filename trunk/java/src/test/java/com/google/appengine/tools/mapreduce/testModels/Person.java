package com.google.appengine.tools.mapreduce.testModels;



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

//{"kind": "person", "fullName": "John Doe", "age": 22, "gender": "Male", "phoneNumber": { "areaCode": "206", "number": "1234567"}, "children": [{ "name": "Jane", "gender": "Female", "age": "6"}, {"name": "John", "gender": "Male", "age": "15"}], "citiesLived": [{ "place": "Seattle", "yearsLived": ["1995"]}, {"place": "Stockholm", "yearsLived": ["2005"]}]}
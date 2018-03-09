package com.amazonaws.services.kinesis.producer.sample.record;

public class StreamAlertBody {
  private String alertId;
  private String category;
  private String subCategory;
  private String subscriberId;
  private String alertTime;

  public String getAlertId() {  return alertId;  }

  public void setAlertId(String alertId) {  this.alertId = alertId;  }

  public String getCategory() { return category;  }

  public void setCategory(String category) {  this.category = category;  }

  public String getSubCategory() {  return subCategory;  }

  public void setSubCategory(String subCategory) {  this.subCategory = subCategory;  }

  public String getSubscriberId() { return subscriberId;  }

  public void setSubscriberId(String subscriberId) {  this.subscriberId = subscriberId;  }

  public String getAlertTime() {  return alertTime;  }

  public void setAlertTime(String alertTime) {  this.alertTime = alertTime;  }

  @Override
  public String toString() {
    return "StreamAlertBody{" +
        "alertId='" + alertId + '\'' +
        ", category='" + category + '\'' +
        ", subCategory='" + subCategory + '\'' +
        ", subscriberId='" + subscriberId + '\'' +
        ", alertTime='" + alertTime + '\'' +
        '}';
  }
}

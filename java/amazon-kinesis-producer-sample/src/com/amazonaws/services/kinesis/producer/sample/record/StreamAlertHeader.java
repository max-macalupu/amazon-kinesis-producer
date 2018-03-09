package com.amazonaws.services.kinesis.producer.sample.record;

public class StreamAlertHeader {
  private String sourceApp;
  private String environment;
  private String time;
  private String requestId;
  private String requestType;
  private String hostId;

  public String getEnvironment() {  return environment;  }

  public void setEnvironment(String environment) {  this.environment = environment;  }

  public String getSourceApp() {  return sourceApp;  }

  public void setSourceApp(String sourceApp) {  this.sourceApp = sourceApp;  }

  public String getTime() { return time;  }

  public void setTime(String time) {  this.time = time;  }

  public String getRequestId() {  return requestId;  }

  public void setRequestId(String requestId) {  this.requestId = requestId;  }

  public String getRequestType() {  return requestType;  }

  public void setRequestType(String requestType) {  this.requestType = requestType;  }

  public String getHostId() { return hostId;  }

  public void setHostId(String hostId) {  this.hostId = hostId;  }

  @Override
  public String toString() {
    return "StreamAlertHeader{" +
        "sourceApp='" + sourceApp + '\'' +
        ", environment='" + environment + '\'' +
        ", time='" + time + '\'' +
        ", requestId='" + requestId + '\'' +
        ", requestType='" + requestType + '\'' +
        ", hostId='" + hostId + '\'' +
        '}';
  }
}

package com.amazonaws.services.kinesis.producer.sample.record;

public class StreamAlert {

  private StreamAlertHeader header;
  private StreamAlertBody body;

  public StreamAlertHeader getHeader() {  return header;  }

  public void setHeader(StreamAlertHeader header) { this.header = header;  }

  public StreamAlertBody getBody() {  return body;  }

  public void setBody(StreamAlertBody body) { this.body = body;  }

  @Override
  public String toString() {
    return "StreamAlert{" +
        "header=" + header +
        ", body=" + body +
        '}';
  }
}

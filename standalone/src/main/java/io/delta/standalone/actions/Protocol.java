package io.delta.standalone.actions;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class Protocol implements Action {
  int minReaderVersion;
  int minWriterVersion;

  public Protocol(int readerVersion, int writerVersion) {
    minReaderVersion = readerVersion;
    minWriterVersion = writerVersion;
  }

  @Override
  public SingleAction wrap() {
    return new SingleAction(this);
  }

  @JsonIgnore
  public String simpleString() {
    return "("+minReaderVersion+","+minWriterVersion+")";
  }
}

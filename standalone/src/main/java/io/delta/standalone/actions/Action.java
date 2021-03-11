package io.delta.standalone.actions;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.delta.standalone.internal.util.JsonUtils;
import java.io.IOException;

public interface Action {
  public static int readerVersion = 1;
  public static int writerVersion = 2;
  public static Protocol protocolVersion = new Protocol(readerVersion, writerVersion);

  SingleAction wrap();

  default String json() throws JsonProcessingException {
   return JsonUtils.mapper().writeValueAsString(wrap());
  }

  public static Action fromJson(String json) throws IOException {
    return JsonUtils.mapper().readValue(json, SingleAction.class).unwrap();
  }
}

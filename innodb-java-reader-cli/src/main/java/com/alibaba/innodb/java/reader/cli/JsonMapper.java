package com.alibaba.innodb.java.reader.cli;

import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.map.annotate.JsonSerialize.Inclusion;

import lombok.extern.slf4j.Slf4j;

/**
 * JsonMapper
 *
 * @author xu.zx
 */
@Slf4j
public class JsonMapper {

  /**
   * ObjectMapper
   */
  private ObjectMapper mapper;

  /**
   * Creates a new instance of JsonMapper.
   */
  public JsonMapper(Inclusion inclusion) {
    mapper = new ObjectMapper();
    mapper.setSerializationInclusion(inclusion);
    mapper.configure(SerializationConfig.Feature.FAIL_ON_EMPTY_BEANS, false);
    mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.configure(DeserializationConfig.Feature.FAIL_ON_NUMBERS_FOR_ENUMS, true);
  }

  /**
   * contains all fields.
   */
  public static JsonMapper buildNormalMapper() {
    return new JsonMapper(Inclusion.ALWAYS);
  }

  public String toJson(Object object) {
    if (object == null) {
      return null;
    }
    try {
      return mapper.writeValueAsString(object);
    } catch (Exception e) {
      throw new IllegalStateException("Serialize object to json failed: " + object, e);
    }
  }

  public String toPrettyJson(Object object) {
    if (object == null) {
      return null;
    }
    try {
      return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(object);
    } catch (Exception e) {
      throw new IllegalStateException("Serialize object to json failed: " + object, e);
    }
  }

}

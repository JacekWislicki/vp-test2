package com.example.test2.processingstatus.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ObjectConverter {

    private static final ObjectMapper mapper = new ObjectMapper();
    static {
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public static String toString(Object object) {
        if (object == null) {
            return null;
        } else if (object instanceof Throwable) {
            Throwable throwable = (Throwable) object;
            return ExceptionUtils.getMessage(throwable) + "\n" + ExceptionUtils.getStackTrace(throwable);
        }
        try {
            return mapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error on converting object to JSON string", e);
        }
    }

    public static <T> T toObject(String json, Class<T> clazz) {
        if (StringUtils.isBlank(json)) {
            return null;
        }
        try {
            return mapper.readValue(json, clazz);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error on deserialising JSON string to object", e);
        }
    }

    private ObjectConverter() {}
}

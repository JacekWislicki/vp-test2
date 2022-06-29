package com.example.test2.processingstatus.model;

import com.example.test2.processingstatus.utils.ObjectConverter;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class Problem {

    private Integer code;
    private String details;

    public Problem(Integer code, Object details) {
        this.code = code;
        this.details = ObjectConverter.toString(details);
    }

    public Problem(Object details) {
        this(null, details);
    }
}

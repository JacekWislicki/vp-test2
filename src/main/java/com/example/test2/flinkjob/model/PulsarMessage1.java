package com.example.test2.flinkjob.model;

import java.io.Serializable;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class PulsarMessage1 implements Serializable {

    private static final long serialVersionUID = 1L;

    private String field01;
    private String field02;
    private String field03;
    private String field04;
    private String field05;
    private String field06;
    private String field07;
    private String field08;
    private String field09;
    private String field10;
    private String field11;
    private String field12;
    private String field13;
    private String field14;
    private String field15;

    // the problem begins
    private String field16;
}
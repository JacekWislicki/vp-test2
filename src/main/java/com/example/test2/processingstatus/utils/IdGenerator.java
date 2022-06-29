package com.example.test2.processingstatus.utils;

import java.security.SecureRandom;

import org.apache.commons.codec.binary.Base32;

public class IdGenerator {

    private static final SecureRandom secureRandom = new SecureRandom();

    public static String generate() {
        byte[] random = new byte[16];
        secureRandom.nextBytes(random);
        return new Base32().encodeToString(random).replace("=", "").toLowerCase();
    }

    private IdGenerator() {
    }
}

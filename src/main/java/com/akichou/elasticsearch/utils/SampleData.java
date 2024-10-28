package com.akichou.elasticsearch.utils;

import com.akichou.elasticsearch.entity.Student;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class SampleData {

    public static List<Student> get() throws IOException {

        File file = new File("students.json") ;

        return new ObjectMapper().readValue(file, new TypeReference<>() {}) ;
    }
}

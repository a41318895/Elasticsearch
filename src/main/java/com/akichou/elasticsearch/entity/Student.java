package com.akichou.elasticsearch.entity;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

import java.util.Date;
import java.util.List;

@Data
public class Student {

    // As index -> documentation identifier
    @NotBlank
    private String studentId ;

    @NotBlank
    private String name ;

    // Department of student (ex: Japanese, computer science, politics, law...)
    @NotEmpty
    private List<String> departments ;

    // Courses student taken
    private List<Course> courses ;

    @NotNull
    private Integer grade ;

    private Integer chineseScore ;

    private Integer mathScore ;

    private Job job ;

    @NotBlank
    private String introduction ;

    private Date englishTestIssuedDate ;

    private String bloodType ;

    private List<String> phoneNumbers ;
}

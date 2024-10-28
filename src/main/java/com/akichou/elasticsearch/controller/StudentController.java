package com.akichou.elasticsearch.controller;

import com.akichou.elasticsearch.repository.StudentElasticsearchRepository;
import com.akichou.elasticsearch.entity.Student;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequiredArgsConstructor
@RequestMapping(value = "/students", produces = MediaType.APPLICATION_JSON_VALUE)
public class StudentController {

    private final StudentElasticsearchRepository studentElasticsearchRepository ;

    // Post single student documentation to ES
    @PostMapping
    public ResponseEntity<Student> create(@Validated @RequestBody Student requestStudent) {

        Student createdStudent = studentElasticsearchRepository.insertStudent(requestStudent) ;

        return ResponseEntity.status(HttpStatus.CREATED).body(createdStudent) ;
    }

    // Post multiple student documentations to ES
    @PostMapping("/multi")
    public ResponseEntity<List<Student>> create(@Validated @RequestBody List<Student> requestStudents) {

        List<Student> createdStudents = studentElasticsearchRepository.insertStudents(requestStudents) ;

        return ResponseEntity.status(HttpStatus.CREATED).body(createdStudents) ;
    }

    // Put single student documentation in ES
    @PutMapping("/{studentId}")
    public ResponseEntity<Student> update(@PathVariable("studentId") String studentId,
                                          @Validated @RequestBody Student requestStudent) {

        // New student id
        requestStudent.setStudentId(studentId) ;

        Student updatedStudent = studentElasticsearchRepository.saveStudent(requestStudent) ;

        return ResponseEntity.status(HttpStatus.ACCEPTED).body(updatedStudent) ;
    }

    // Delete single student documentation in ES via studentId (set identifier of index)
    @DeleteMapping("/{studentId}")
    public ResponseEntity<Void> delete(@PathVariable("studentId") String studentId) {

        studentElasticsearchRepository.deleteStudentById(studentId) ;

        return ResponseEntity.status(HttpStatus.NO_CONTENT).build() ;
    }

    // Get a single student documentation
    @GetMapping("/{studentId}")
    public ResponseEntity<Student> get(@PathVariable("studentId") String studentId) {

        Student foundStudent = studentElasticsearchRepository.findStudentById(studentId).orElse(null) ;

        return foundStudent == null
                ? ResponseEntity.status(HttpStatus.NOT_FOUND).build()
                : ResponseEntity.status(HttpStatus.OK).body(foundStudent) ;
    }
}

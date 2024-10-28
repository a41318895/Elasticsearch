package com.akichou.elasticsearch.repository;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.mapping.DateProperty;
import co.elastic.clients.elasticsearch._types.mapping.Property;
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;
import co.elastic.clients.elasticsearch.core.*;

import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.core.bulk.CreateOperation;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.DeleteIndexRequest;
import co.elastic.clients.elasticsearch.indices.ExistsRequest;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import com.akichou.elasticsearch.entity.search.SearchInfo;
import com.akichou.elasticsearch.entity.Student;
import com.akichou.elasticsearch.functionalInterface.IOSupplier;
import com.akichou.elasticsearch.repository.mapping.FieldValuePropertyMapping;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
public class StudentElasticsearchRepository implements FieldValuePropertyMapping {

    private final ElasticsearchClient elasticsearchClient;
    private final String indexName;

    // Constructor
    public StudentElasticsearchRepository(ElasticsearchClient elasticsearchClient, String indexName) {
        this.elasticsearchClient = elasticsearchClient;
        this.indexName = indexName;
    }

    // Initialization
    public void init() {

        if (isIndexExists(indexName)) deleteIndex() ;

        createIndex() ;
    }

    private boolean isIndexExists(String indexName) {

        ExistsRequest existsRequest = ExistsRequest.of(b -> b.index(indexName)) ;

        BooleanResponse booleanResponse = execute(() -> elasticsearchClient.indices().exists(existsRequest)) ;

        return booleanResponse.value() ;
    }

    public void deleteIndex() {

        // Set the request - Delete the index (indexName)
        DeleteIndexRequest deleteIndexRequest = DeleteIndexRequest.of(b -> b.index(indexName)) ;

        // indices() method - Index-level operation API
        execute(() -> elasticsearchClient.indices().delete(deleteIndexRequest)) ;
    }

    public void createIndex() {

        Map<String, Property> propertyMapping = getPropertyMappings() ;

        // Create index with 'propertyMapping' mapping strategy
        CreateIndexRequest createIndexRequest =
                new CreateIndexRequest.Builder()
                    .index(indexName)
                    .mappings(TypeMapping.of(b -> b.properties(propertyMapping)))
                    .build() ;

        // Index-level operation
        execute(() -> elasticsearchClient.indices().create(createIndexRequest)) ;
    }

    // Mapping column "englishTestIssuedData"'s value as DateProperty format
    @Override
    public Map<String, Property> getPropertyMappings() {

        Property englishTestIssuedDateProperty = DateProperty.of(b -> b)._toProperty() ;

        return Map.of("englishTestIssuedDate", englishTestIssuedDateProperty) ;
    }

    // Controller Relations...
    public Student insertStudent(Student studentDocumentation) {

        // Set index, id(make studentId as identifier), document entity into request for ES.
        CreateRequest<Student> createRequest =
                new CreateRequest.Builder<Student>()
                    .index(indexName)
                    .id(studentDocumentation.getStudentId())
                    .document(studentDocumentation)
                    .build();

        return execute(() -> {

            // Create documentation in the index indicated
            CreateResponse createResponse = elasticsearchClient.create(createRequest) ;

            studentDocumentation.setStudentId(createResponse.id()) ;

            return studentDocumentation ;
        }) ;
    }

    public List<Student> insertStudents(List<Student> studentDocumentations) {

        // Set a bulk-request builder binding with 'indexName'
        BulkRequest.Builder builder = new BulkRequest.Builder().index(indexName) ;

        studentDocumentations.forEach(studentDocumentation -> {

            // Create an operation for a studentDocumentation
            CreateOperation<Student> createOperation =
                    new CreateOperation.Builder<Student>()
                        .id(studentDocumentation.getStudentId())
                        .document(studentDocumentation)
                        .build() ;

            // Make an operation a bulk-operation
            BulkOperation bulkOperation = BulkOperation.of(b -> b.create(createOperation)) ;

            // Add the bulk-operation into bulk-request builder
            builder.operations(bulkOperation) ;
        }) ;
        BulkRequest bulkRequest = builder.build() ;

        return execute(() -> {

            // Execute bulk insert operation, and get corresponding response
            BulkResponse bulkResponse = elasticsearchClient.bulk(bulkRequest) ;


            List<BulkResponseItem> items = bulkResponse.items() ;

            for (var i = 0 ; i < items.size() ; i ++) {

                String studentId = items.get(i).id() ;

                studentDocumentations.get(i).setStudentId(studentId) ;
            }

            return studentDocumentations ;
        }) ;
    }

    public Student saveStudent(Student studentDocumentation) {

        // Index-level request - with new studentDocumentation data of indicated id
        IndexRequest<Student> indexRequest =
                new IndexRequest.Builder<Student>()
                    .index(indexName)
                    .id(studentDocumentation.getStudentId())
                    .document(studentDocumentation)
                    .build() ;

        return execute(() -> {

            IndexResponse indexResponse = elasticsearchClient.index(indexRequest) ;

            studentDocumentation.setStudentId(indexResponse.id()) ;

            return studentDocumentation ;
        }) ;
    }

    public void deleteStudentById(String studentId) {

        // Set a delete request - with indicated index and the documentation id
        DeleteRequest deleteRequest = new DeleteRequest.Builder()
                .index(indexName)
                .id(studentId)
                .build() ;

        execute(() -> elasticsearchClient.delete(deleteRequest)) ;
    }

    public Optional<Student> findStudentById(String studentId) {

        // Set a get request - with indicated index and the studentId
        GetRequest getRequest = new GetRequest.Builder()
                .index(indexName)
                .id(studentId)
                .build() ;

        // Send the request and define the class of documentation to find
        GetResponse<Student> getResponse =
                execute(() -> elasticsearchClient.get(getRequest, Student.class)) ;

        return Optional.ofNullable(getResponse.source()) ;
    }

    // For full-text search
    public List<Student> find(SearchInfo searchInfo) {

        // Set a search request - with index,
        // ( query condition, functions, score mode, boost mode, max boost of FunctionScoreQuery ),
        // sort order, start-require index, actual-require documentation number
        SearchRequest searchRequest = new SearchRequest.Builder()
                .index(indexName)
                .query(searchInfo.toQuery())
                .sort(searchInfo.getSortOptions())
                .from(searchInfo.getFrom())
                .size(searchInfo.getSize())
                .build() ;

        SearchResponse<Student> searchResponse =
                execute(() -> elasticsearchClient.search(searchRequest, Student.class)) ;

        // Print out scores of students
        searchResponse.hits().hits()
                .forEach(hit ->
                        System.out.print("ID: " + hit.id() + ", Score: " + String.format("%.3f", hit.score()) + "\n")) ;

        // hits.hits -> result.result array
        // hits.hits : [
        //
        //      {
        //              "_index":
        //              "_id":
        //              "_score":
        //              "_source": { documentation columns and values }
        //      }, ...
        // ]
        return searchResponse.hits()
                .hits()
                .stream()
                .map(Hit::source)
                .toList() ;
    }

    // IOException Handling
    private <V> V execute(IOSupplier<V> ioSupplier) {

        try {

            return ioSupplier.get() ;
        } catch (IOException e) {

            log.error(e.getMessage()) ;

            throw new RuntimeException(e) ;
        }
    }
}

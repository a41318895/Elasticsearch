package com.akichou.elasticsearch.config;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.akichou.elasticsearch.repository.StudentElasticsearchRepository;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ElasticsearchConfig {

    @Value("${elasticsearch.index.name}")
    private String indexName ;

    @Bean
    public ElasticsearchClient elasticsearchClient() {

        HttpHost httpHost = new HttpHost("localhost", 9200, "http") ;
        RestClient restClient = RestClient.builder(httpHost).build() ;
        RestClientTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper()) ;

        return new ElasticsearchClient(transport) ;
    }

    @Bean
    public StudentElasticsearchRepository studentElasticsearchRepository(ElasticsearchClient elasticsearchClient) {

        StudentElasticsearchRepository studentElasticsearchRepository = new StudentElasticsearchRepository(elasticsearchClient, indexName) ;

        studentElasticsearchRepository.init() ;

        return studentElasticsearchRepository ;
    }
}

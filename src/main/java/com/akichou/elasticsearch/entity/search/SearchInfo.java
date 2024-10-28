package com.akichou.elasticsearch.entity.search;

import co.elastic.clients.elasticsearch._types.SortOptions;
import co.elastic.clients.elasticsearch._types.query_dsl.*;
import lombok.Data;
import org.springframework.util.CollectionUtils;

import java.util.List;

@Data
public class SearchInfo {

    // The conditions to query
    private BoolQuery boolQuery ;

    // The custom function to calculate the score
    private List<FunctionScore> functionScores = List.of() ;

    // The sort order
    private List<SortOptions> sortOptions = List.of() ;

    // The start-require index
    private Integer from ;

    // The number of data requiring
    private Integer size ;

    // Init query condition -> match_all
    public SearchInfo() {

        Query queryToMatchAll = MatchAllQuery.of(b -> b)._toQuery() ;

        this.boolQuery = BoolQuery.of(b -> b.filter(queryToMatchAll)) ;
    }

    // Static method - Set BoolQuery of SearchInfo
    public static SearchInfo of(BoolQuery boolQuery) {

        SearchInfo searchInfo = new SearchInfo() ;

        searchInfo.boolQuery = boolQuery ;

        return searchInfo ;
    }

    // Static method - Transfer Query to BoolQuery, and call of(BoolQuery boolQuery) method
    public static SearchInfo of(Query query) {

        BoolQuery boolQuery = BoolQuery.of(b -> b.filter(query)) ;

        return of(boolQuery) ;
    }


    public Query toQuery() {

        // If FunctionScore hasn't set, return basic Query
        if (CollectionUtils.isEmpty(functionScores)) return boolQuery._toQuery() ;

        return new FunctionScoreQuery.Builder()
                .query(boolQuery._toQuery())
                .functions(functionScores)      // Function to calculate score of every documentation
                .scoreMode(FunctionScoreMode.Sum)       // Sum all scores of functional calculating
                .boostMode(FunctionBoostMode.Replace)       // Set BoostMode Replace (Replace ES Score with custom functional Score)
                .maxBoost(30.0)     // Set a limit score
                .build()
                ._toQuery() ;
    }
}

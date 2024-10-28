package com.akichou.elasticsearch;

import co.elastic.clients.elasticsearch._types.SortMode;
import co.elastic.clients.elasticsearch._types.SortOptions;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.query_dsl.*;
import com.akichou.elasticsearch.entity.Student;
import com.akichou.elasticsearch.entity.search.SearchInfo;
import com.akichou.elasticsearch.repository.StudentElasticsearchRepository;
import com.akichou.elasticsearch.utils.SampleData;
import com.akichou.elasticsearch.utils.SearchUtils;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Set;

@SpringBootTest
@Slf4j
class ElasticsearchApplicationTests {

    @Autowired
    private StudentElasticsearchRepository studentElasticsearchRepository ;

    @BeforeEach
    public void setup() throws IOException, InterruptedException {

        // Create the index
        studentElasticsearchRepository.init() ;

        // Get documentation configurations from json file
        List<Student> studentDocumentations = SampleData.get() ;

        // Insert student documentations into the index
        studentElasticsearchRepository.insertStudents(studentDocumentations) ;

        // Mock IO blocking (Inserting)
        Thread.sleep(2000) ;
    }

    // Documentation ID Assertion Common Method
    private void assertDocumentIds(boolean ignoreOrder, List<Student> actualDocumentations, String... expectedIdArray) {

        List<String> expectedIds = List.of(expectedIdArray) ;

        List<String> actualIds = actualDocumentations.stream()
                .map(Student::getStudentId)
                .toList() ;

        if (ignoreOrder) {

            Assertions.assertTrue(expectedIds.containsAll(actualIds)) ;

            Assertions.assertTrue(actualIds.containsAll(expectedIds)) ;

            log.info("Passed the test with ignoring id order") ;
        } else {

            Assertions.assertEquals(expectedIds, actualIds) ;

            log.info("Passed the test with id in-order") ;
        }
    }

    // 測試 -- 特定欄位值因子得分
    @Test
    public void testFunctionScore_FieldValueFactor() {

        FunctionScore fieldValueFactorScore =
                SearchUtils.createFieldValueFactor("grade", 0.5, FieldValueFactorModifier.Square, 0.0) ;

        // Init match_all query condition
        SearchInfo searchInfo = new SearchInfo() ;

        searchInfo.setFunctionScores(List.of(fieldValueFactorScore)) ;

        List<Student> studentsDocumentations = studentElasticsearchRepository.find(searchInfo) ;

        assertDocumentIds(false, studentsDocumentations, "101", "102", "103", "104") ;
        assertDocumentIds(true, studentsDocumentations, "103", "101", "102", "104") ;
    }

    // 測試 -- 兩個精準查詢並設其權重分數 + 特定欄位值因子得分並設其權重
    @Test
    public void testFunctionScore_ConditionalWeight() {

        // Precise Query - department
        Query departmentPreciseQuery =
                SearchUtils.createTermQuery("departments.keyword", "財務金融") ;

        // FunctionScore of Precise Query - department
        FunctionScore departmentScore =
                SearchUtils.createConditionalWeightFunctionScore(departmentPreciseQuery, 3.0) ;

        // Precise Query - course
        Query coursePreciseQuery =
                SearchUtils.createTermQuery("courses.courseName.keyword", "程式設計") ;

        // FunctionScore of Precise Query - course
        FunctionScore courseScore =
                SearchUtils.createConditionalWeightFunctionScore(coursePreciseQuery, 1.5) ;

        // Query Reference FieldValue Factor
        FunctionScore fieldValueFactorScore =
                SearchUtils.createFieldValueFactor("grade", 1.0, FieldValueFactorModifier.None, 0.0) ;

        // FunctionScore.FieldValueFactorScoreFunction
        FunctionScore gradeScore =
                SearchUtils.createWeightedFieldValueFactor(fieldValueFactorScore.fieldValueFactor(), 0.5) ;

        SearchInfo searchInfo = new SearchInfo() ;

        // Set functions into SearchInfo
        searchInfo
                .setFunctionScores(List.of(
                    departmentScore,
                    courseScore,
                    gradeScore
        ));

        List<Student> students = studentElasticsearchRepository.find(searchInfo) ;

        // 103 (5.5) -> 101 (2.0) -> 102 (1.5) -> 104 (0.5)
        assertDocumentIds(false, students, "103", "101", "102", "104") ;
    }

    // 測試 -- 高斯衰減函數計分 ( 純數值 )
    @Test
    public void testFunctionScore_DecayFunction_Number() {

        // Good status : 115~100~85 , Border : 75 , Turn worst sharply ( score -> decay 0.5 ) : lte 75
        DecayPlacement decayPlacement = SearchUtils
                .createDecayPlacement(100, 15, 10, 0.5) ;

        FunctionScore decayFunctionScore = SearchUtils
                .createGaussFunction("chineseScore", decayPlacement) ;

        SearchInfo searchInfo = new SearchInfo() ;
        searchInfo.setFunctionScores(List.of(decayFunctionScore)) ;

        List<Student> students = studentElasticsearchRepository.find(searchInfo) ;

        // Vincent (1.0) -> Malinda (0.973) -> Dan (0.432) -> William (0.257)
        assertDocumentIds(false, students, "103", "102", "101", "104") ;
    }

    // 測試 -- 高斯衰減函數計分 ( 日期 )
    @Test
    public void testFunctionScore_DecayFunction_Date() {

        DecayPlacement decayPlacement = SearchUtils
                .createDecayPlacement("now", "90d", "270d", 0.5) ;

        FunctionScore decayFunctionScore = SearchUtils
                .createGaussFunction("englishTestIssuedDate", decayPlacement) ;

        var searchInfo = new SearchInfo();
        searchInfo.setFunctionScores(List.of(decayFunctionScore)) ;

        List<Student> students = studentElasticsearchRepository.find(searchInfo) ;

        // Malinda (1.000) -> William (0.998) -> Dan (0.476) -> Vincent (0.078)
        assertDocumentIds(false, students, "102", "104", "101", "103") ;
    }

    // 測試 -- 精準查詢 ( 一對一 )
    @Test
    public void testTermQuery_OneToOne_NumberField() {

        Query query = SearchUtils.createTermQuery("grade", 3) ;

        SearchInfo searchInfo = SearchInfo.of(query) ;

        List<Student> students = studentElasticsearchRepository.find(searchInfo) ;

        // Malinda (0.000) -- No TermQuery's weighted score, and no functions here, so get 0 score
        assertDocumentIds(true, students, "102") ;
    }

    // 測試 -- 精準查詢 ( 一對多 )
    @Test
    public void testTermQuery_OneToMany_TextField() {

        // Values for termsQuery
        List<String> values = List.of("資訊管理", "企業管理") ;

        Query termsQuery = SearchUtils.createTermsQuery("departments.keyword", values) ;

        SearchInfo searchInfo = SearchInfo.of(termsQuery) ;

        List<Student> students = studentElasticsearchRepository.find(searchInfo) ;

        // Vincent (0.000), William (0.000) -- No TermQuery's weighted score, and no functions here, so get 0 score
        assertDocumentIds(true, students, "103", "104") ;
    }

    // 測試 -- 範圍查詢 ( 純數值 )
    @Test
    public void testRangeQuery_NumberField() {

        Query rangeQuery = SearchUtils.createRangeQuery("grade", 2, 4) ;

        SearchInfo searchInfo = SearchInfo.of(rangeQuery) ;

        List<Student> students = studentElasticsearchRepository.find(searchInfo) ;

        // Dan (0.000), Malinda (0.000), Vincent (0.000) -- No TermQuery's weighted score, and no functions here, so get 0 score
        assertDocumentIds(true, students, "101", "102", "103") ;
    }

    // 測試 -- 範圍查詢 ( 日期 )
    @Test
    public void testRangeQuery_Date() throws ParseException {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd") ;
        Date fromDate = sdf.parse("2024-07-01") ;
        Date toDate = sdf.parse("2024-11-01") ;

        Query rangeQuery = SearchUtils.createRangeQuery("englishTestIssuedDate", fromDate, toDate) ;

        SearchInfo searchInfo = SearchInfo.of(rangeQuery) ;

        List<Student> students = studentElasticsearchRepository.find(searchInfo) ;

        // William (0.000), Malinda (0.000) -- No TermQuery's weighted score, and no functions here, so get 0 score
        assertDocumentIds(true, students, "102", "104") ;
    }

    // 測試 -- 全文搜索 ( 必定 : (name 要至少包含 searchText 其一 或 introduction 要至少包含 searchText 其一)
    //                  必定 : (score 要 >= 0, <= 1)
    //                  不一定 : (mathScore 要 >= 50, <= 80)
    //                  不一定要符合的條件至少要有 0 個 )
    @Test
    public void testFullTextSearch_BoolQuery() {

        Set<String> fields = Set.of("name", "introduction") ;
        String searchText = "vincent career" ;

        // Fuzzy query (name, introduction)
        Query nameIntroductionFuzzyQuery = SearchUtils.createMatchQuery(fields, searchText) ;

        // Range Query (grade)
        Query gradeRangeQuery = SearchUtils.createRangeQuery("grade", 0, 1) ;

        // Range Query (mathScore) -- Optional
        Query mathScoreRangeQuery = SearchUtils.createRangeQuery("mathScore", 50, 80) ;

        BoolQuery boolQuery = BoolQuery.of(b -> b
                .must(nameIntroductionFuzzyQuery)
                .must(gradeRangeQuery)
                .should(mathScoreRangeQuery)
                .minimumShouldMatch("0")
        ) ;

        SearchInfo searchInfo = SearchInfo.of(boolQuery) ;

        List<Student> students = studentElasticsearchRepository.find(searchInfo) ;

        // William (1.767)
        assertDocumentIds(true, students, "104") ;
    }

    // 測試 -- 存在電話號碼欄位 查詢
    @Test
    public void testFieldExistsQuery() {

        Query phoneNumbersFieldExistQuery = SearchUtils.createFieldExistsQuery("phoneNumbers") ;

        SearchInfo searchInfo = SearchInfo.of(phoneNumbersFieldExistQuery) ;

        List<Student> students = studentElasticsearchRepository.find(searchInfo) ;

        // Dan (0.000), Vincent (0.000) -- No TermQuery's weighted score, and no functions here, so get 0 score
        assertDocumentIds(true, students, "101", "103") ;
    }

    // 測試 -- 根據修習課程中最高學分數進行降序排列, 之後再根據名字進行升序排列
    @Test
    public void testSortByMultipleFields() {

        // Find the max point in courses array, and sort it descend
        SortOptions coursePointsSort =
                SearchUtils.createSortOption("courses.point", SortOrder.Desc, SortMode.Max) ;

        // Precise query the name of documentation, and sort it ascend
        SortOptions nameSort =
                SearchUtils.createSortOption("name.keyword", SortOrder.Asc) ;

        Query matchAllQuery = MatchAllQuery.of(b -> b)._toQuery() ;

        SearchInfo searchInfo = SearchInfo.of(matchAllQuery) ;
        searchInfo.setSortOptions(List.of(coursePointsSort, nameSort)) ;     // Sort courses.point first, then name

        List<Student> students = studentElasticsearchRepository.find(searchInfo) ;

        assertDocumentIds(false, students, "102", "103", "101", "104") ;
    }

    // 測試 -- 根據年級進行降序排列, 並從排序結果第一個(最高年級)開始取, 取樣前兩個
    @Test
    public void testSortBySingleField_And_Paging() {

        SortOptions gradeSort = SearchUtils.createSortOption("grade", SortOrder.Desc) ;

        Query matchAllQuery = MatchAllQuery.of(b -> b)._toQuery() ;

        SearchInfo searchInfo = SearchInfo.of(matchAllQuery) ;
        searchInfo.setSortOptions(List.of(gradeSort)) ;
        searchInfo.setFrom(0) ;     // Paging result of sorting from first(0) documentation
        searchInfo.setSize(2) ;     // Paging first 2 result of sorting

        List<Student> students = studentElasticsearchRepository.find(searchInfo) ;

        assertDocumentIds(false, students, "101", "102") ;
    }
}

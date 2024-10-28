package com.akichou.elasticsearch.utils;

import co.elastic.clients.elasticsearch._types.*;
import co.elastic.clients.elasticsearch._types.query_dsl.*;
import co.elastic.clients.json.JsonData;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SearchUtils {

    // 回傳一個 計算分數的函數 - 指定某欄位(field)之值, 先進行modifier的運算後, 再乘以因子(factor). 若某欄位不存在, 則該value為missing
    public static FunctionScore createFieldValueFactor(
            String field,
            Double factor,
            FieldValueFactorModifier modifier,
            Double missing) {

        return new FieldValueFactorScoreFunction.Builder()
                .field(field)        // indicated documentation column
                .factor(factor)      // calculate factor
                .modifier(modifier)  // calculation modifier
                .missing(missing)    // default value of missing field
                .build()
                ._toFunctionScore() ;
    }

    // 回傳一個 計算分數的函數 (帶有權重分數) - 根據某欄位的值來計算分數的函數 得到結果後, 再乘以指定的權重
    public static FunctionScore createWeightedFieldValueFactor(
            FieldValueFactorScoreFunction fieldValueFactor,
            Double weight) {

        return new FunctionScore.Builder()
                .fieldValueFactor(fieldValueFactor)
                .weight(weight)
                .build() ;
    }

    // 回傳一個 計算分數的函數 (帶有權重分數) - 對(精準、模糊、範圍)的查詢結果, 乘以權重, 從而得到分數
    public static FunctionScore createConditionalWeightFunctionScore(Query query, Double weight) {

        return new FunctionScore.Builder()
                .filter(query)
                .weight(weight)
                .build() ;
    }

    // 回傳一個 對某欄位 value 的衰減布局 - 接受數值
    public static DecayPlacement createDecayPlacement(
            Number origin, Number offset, Number scale, Double decay) {

        return createDecayPlacementInternal(origin, offset, scale, decay) ;
    }

    // 回傳一個 對某欄位 value 的衰減布局 - 接受日期
    public static DecayPlacement createDecayPlacement(
            String originExp, String offsetExp, String scaleExp, Double decay) {

        return createDecayPlacementInternal(originExp, offsetExp, scaleExp, decay) ;
    }

    // 回傳一個 對某欄位 value 的衰減布局 -
    // Origin 為最佳值 分數為1, Offset 為擴張最佳值的範圍, Origin 正負 Offset 分數都為1
    // 當 value 上升到 Origin + Offset + Scale 或 下降到 Origin - Offset - Scale 時, 分數會急遽下降至 decay
    private static <V> DecayPlacement createDecayPlacementInternal(
            V origin, V offset, V scale, Double decay) {

        return new DecayPlacement.Builder()
                .origin(JsonData.of(origin))
                .offset(JsonData.of(offset))
                .scale(JsonData.of(scale))
                .decay(decay)
                .build() ;
    }

    // 回傳一個 計算分數的函數 (衰減函數) - 接收 field 的 value, 即為 衰減布局的對象
    public static FunctionScore createGaussFunction(
            String field, DecayPlacement decayPlacement) {

        DecayFunction decayFunction =
                new DecayFunction.Builder()
                    .field(field)
                    .placement(decayPlacement)
                    .build() ;

        return new FunctionScore.Builder()
                .gauss(decayFunction)
                .build() ;
    }

    // 回傳一個 精準查詢 (一對一)
    public static Query createTermQuery(String field, Object value) {

        TermQuery.Builder builder = new TermQuery.Builder() ;

        if (value instanceof Integer) {

            builder
                    .field(field)
                    .value((int) value) ;

        } else if (value instanceof String) {

            builder
                    .field(field)
                    .value((String) value) ;

        } else {

            throw new UnsupportedOperationException("This type of value is not supported !") ;
        }

        return builder.build()._toQuery() ;
    }

    // 回傳一個 精準查詢 (一對多)
    public static Query createTermsQuery(String field, Collection<?> values) {

        // Get an any element, to realize the element type in collection
        Object elementInValues = values.stream().findAny().orElseThrow() ;

        Stream<FieldValue> fieldValueStream = getFieldValueStream(values, elementInValues) ;

        List<FieldValue> fieldValues = fieldValueStream.collect(Collectors.toList()) ;

        TermsQueryField termsQueryField = TermsQueryField.of(b -> b.value(fieldValues)) ;

        return new TermsQuery.Builder()
                .field(field)
                .terms(termsQueryField)
                .build()
                ._toQuery() ;
    }

    // 回傳一個 欄位值 的 資料流 - 根據傳入的 element 來轉型
    private static Stream<FieldValue> getFieldValueStream(Collection<?> values, Object element) {

        Stream<FieldValue> fieldValueStream ;

        if (element instanceof Integer) {

            fieldValueStream = values.stream()
                    .map(value -> FieldValue.of(b -> b.longValue((int) value))) ;

        } else if (element instanceof String) {

            fieldValueStream = values.stream()
                    .map(value -> FieldValue.of(b -> b.stringValue((String) value))) ;

        } else {

            throw new UnsupportedOperationException("This type of value is not supported !") ;
        }

        return fieldValueStream ;
    }

    // 回傳一個 範圍查詢 - 接受數值
    public static Query createRangeQuery(String field, Number gte, Number lte) {

        return createRangeQueryInternal(field, gte, lte) ;
    }

    // 回傳一個 範圍查詢 - 接受日期
    public static Query createRangeQuery(String field, Date gte, Date lte) {

        return createRangeQueryInternal(field, gte, lte) ;
    }

    // 回傳一個 範圍查詢 - 設置範圍查詢上下限
    private static <T> Query createRangeQueryInternal(String field, T gte, T lte) {

        RangeQuery.Builder builder = new RangeQuery.Builder().field(field) ;

        if (gte != null) {
            builder.gte(JsonData.of(gte)) ;
        }

        if (lte != null) {
            builder.lte(JsonData.of(lte)) ;
        }

        return builder.build()._toQuery() ;

    }

    // 回傳一個 標準查詢 之 模糊查詢
    public static Query createMatchQuery(Set<String> fields, String searchText) {

        BoolQuery.Builder bool = new BoolQuery.Builder() ;

        fields.stream()
                // The value of the field should be in the searchText
                .map(field -> {

                    MatchQuery matchQuery = new MatchQuery.Builder()
                            .field(field)
                            .query(searchText)
                            .build() ;

                    return matchQuery._toQuery() ;
                })
                .forEach(bool::should) ;    // 對各個 field 的查詢方式採取 OR 策略

        return bool.build()._toQuery() ;
    }

    // 回傳一個 欄位是否存在 查詢
    public static Query createFieldExistsQuery(String field) {

        return new ExistsQuery.Builder()
                .field(field)
                .build()
                ._toQuery() ;
    }

    // 回傳一個 排序方針 (field 之值具有多個) - 透過傳入的 SortMode 來先對多個值進行處理
    public static SortOptions createSortOption(String field, SortOrder order, SortMode mode) {

        FieldSort fieldSort = new FieldSort.Builder()
                .field(field)
                .order(order)
                .mode(mode)
                .build();

        return SortOptions.of(b -> b.field(fieldSort)) ;
    }

    // 回傳一個 排序方針 (field 之值僅有單個)
    public static SortOptions createSortOption(String field, SortOrder order) {

        return createSortOption(field, order, null) ;
    }
}

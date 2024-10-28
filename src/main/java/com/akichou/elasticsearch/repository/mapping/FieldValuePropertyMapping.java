package com.akichou.elasticsearch.repository.mapping;

import co.elastic.clients.elasticsearch._types.mapping.Property;

import java.util.Map;

public interface FieldValuePropertyMapping {

    Map<String, Property> getPropertyMappings() ;
}

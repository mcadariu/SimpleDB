package com.simpledb.parse;

public record CreateIndexData(String indexName, String tableName, String fieldName) {
}

package com.simpledb.parse;

import com.simpledb.scan.Predicate;

public record DeleteData(String tableName, Predicate pred) {
}

package com.simpledb.parse;

import com.simpledb.scan.Constant;

import java.util.List;

public record InsertData(String tableName, List<String> fields, List<Constant> vals) {
}

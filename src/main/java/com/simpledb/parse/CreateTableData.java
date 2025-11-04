package com.simpledb.parse;

import com.simpledb.record.Schema;

public record CreateTableData(String tableName, Schema newSchema) {
}

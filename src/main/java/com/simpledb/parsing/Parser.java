package com.simpledb.parsing;

import com.simpledb.record.Schema;
import com.simpledb.scan.Constant;
import com.simpledb.scan.IntConstant;
import com.simpledb.scan.StringConstant;
import com.simpledb.scan.Expression;
import com.simpledb.scan.Predicate;
import com.simpledb.scan.Term;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class Parser {
    private Lexer lex;

    public Parser(String s) throws BadSyntaxException {
        lex = new Lexer(s);
    }

    public String field() throws BadSyntaxException {
        return lex.eatId();
    }

    public Constant constant() throws BadSyntaxException {
        if (lex.matchStringConstant())
            return new StringConstant(lex.eatStringConstant());
        else
            return new IntConstant(lex.eatIntConstant());
    }

    public Expression expression() throws BadSyntaxException {
        if (lex.matchId())
            return new Expression(field());
        else
            return new Expression(constant());
    }

    public Term term() throws BadSyntaxException {
        Expression lhs = expression();
        lex.eatDelim('=');
        Expression rhs = expression();
        return new Term(lhs, rhs);
    }

    public Predicate predicate() throws BadSyntaxException {
        Predicate pred = new Predicate(term());

        if (lex.matchKeyword("and")) {
            lex.eatKeyword("and");
            pred.conjoinWith(predicate());
        }
        return pred;
    }

    public QueryData query() throws BadSyntaxException {
        lex.eatKeyword("select");
        List<String> fields = selectList();
        lex.eatKeyword("from");
        Collection<String> tables = tableList();
        Predicate pred = new Predicate();

        if (lex.matchKeyword("where")) {
            lex.eatKeyword("where");
            pred = predicate();
        }
        return new QueryData(fields, tables, pred);
    }

    private List<String> selectList() throws BadSyntaxException {
        List<String> L = new ArrayList<>();
        L.add(field());
        if (lex.matchDelim(',')) {
            lex.eatDelim(',');
            L.addAll(selectList());
        }
        return L;
    }

    private Collection<String> tableList() throws BadSyntaxException {
        Collection<String> L = new ArrayList<String>();
        L.add(lex.eatId());

        if (lex.matchDelim(',')) {
            lex.eatDelim(',');
            L.addAll(tableList());
        }

        return L;
    }

    public Object updateCmd() throws BadSyntaxException {
        if (lex.matchKeyword("insert"))
            return insert();
        else if (lex.matchKeyword("delete"))
            return delete();
        else return create();
    }

    private DeleteData delete() throws BadSyntaxException {
        lex.eatKeyword("delete");
        lex.eatKeyword("from");
        String tblname = lex.eatId();
        Predicate pred = new Predicate();
        if (lex.matchKeyword("where")) {
            lex.eatKeyword("where");
            pred = predicate();
        }

        return new DeleteData(tblname, pred);
    }


    private InsertData insert() throws BadSyntaxException {
        lex.eatKeyword("insert");
        lex.eatKeyword("into");
        String tblname = lex.eatId();
        lex.eatDelim('(');
        List<String> flds = fieldList();
        lex.eatDelim(')');
        lex.eatKeyword("values");
        lex.eatDelim('(');
        List<Constant> vals = constList();
        lex.eatDelim(')');
        return new InsertData(tblname, flds, vals);
    }

    private List<Constant> constList() throws BadSyntaxException {
        List<Constant> L = new ArrayList<>();
        L.add(constant());
        if (lex.matchDelim(',')) {
            lex.eatDelim(',');
            L.addAll(constList());
        }

        return L;
    }

    public CreateTableData createTable() throws BadSyntaxException {
        lex.eatKeyword("table");
        String tblname = lex.eatId();
        lex.eatDelim('(');
        Schema sch = fieldDefs();
        lex.eatDelim(')');
        return new CreateTableData(tblname, sch);
    }

    private Schema fieldDefs() throws BadSyntaxException {
        Schema schema = fieldDef();
        if (lex.matchDelim(',')) {
            lex.eatDelim(',');
            Schema schema2 = fieldDefs();
            schema.addAll(schema2);
        }
        return schema;
    }

    private Schema fieldDef() throws BadSyntaxException {
        String fldname = field();
        return fieldType(fldname);
    }

    private Schema fieldType(String fldname) throws BadSyntaxException {
        Schema schema = new Schema();
        if (lex.matchKeyword("int")) {
            lex.eatKeyword("int");
            schema.addIntField(fldname);
        } else {
            lex.eatKeyword("varchar");
            lex.eatDelim('(');
            int strLen = lex.eatIntConstant();
            lex.eatDelim(')');
            schema.addStringField(fldname, strLen);
        }
        return schema;
    }

    private List<String> fieldList() throws BadSyntaxException {
        List<String> L = new ArrayList<>();
        L.add(field());
        if (lex.matchDelim(',')) {
            lex.eatDelim(',');
            L.addAll(fieldList());
        }
        return L;
    }

    private Object create() throws BadSyntaxException {
        lex.eatKeyword("create");
        if (lex.matchKeyword("table"))
            return createTable();
        else
            return createIndex();
    }

    private CreateIndexData createIndex() throws BadSyntaxException {
        lex.eatKeyword("index");
        String idxname = lex.eatId();
        lex.eatKeyword("on");
        String tblname = lex.eatId();
        lex.eatDelim('(');
        String fldname = field();
        lex.eatDelim(')');
        return new CreateIndexData(idxname, tblname, fldname);
    }

}

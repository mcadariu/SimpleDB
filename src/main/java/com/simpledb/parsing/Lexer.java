package com.simpledb.parsing;

import java.io.IOException;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Collection;

public class Lexer {
    private Collection<String> keywords;
    private StreamTokenizer tok;

    public Lexer(String s) throws BadSyntaxException {
        initKeywords();
        tok = new StreamTokenizer(new StringReader(s));
        tok.ordinaryChar('.');
        tok.wordChars('_', '_');
        tok.lowerCaseMode(true);
        nextToken();
    }

    public boolean matchDelim(char d) {
        return d == (char) tok.ttype;
    }

    public boolean matchIntConstant() {
        return tok.ttype == StreamTokenizer.TT_NUMBER;
    }

    public boolean matchStringConstant() {
        return '\'' == (char) tok.ttype;
    }

    public boolean matchKeyword(String w) {
        return tok.ttype == StreamTokenizer.TT_WORD && tok.sval.equals(w);
    }

    public boolean matchId() {
        return tok.ttype == StreamTokenizer.TT_WORD && !keywords.contains(tok.sval);
    }

    public void eatDelim(char d) throws BadSyntaxException {
        if (!matchDelim(d))
            throw new BadSyntaxException();
        nextToken();
    }

    public int eatIntConstant() throws BadSyntaxException {
        if (!matchIntConstant())
            throw new BadSyntaxException();
        int i = (int) tok.nval;
        nextToken();
        return i;
    }

    public String eatStringConstant() throws BadSyntaxException {
        if (!matchStringConstant())
            throw new BadSyntaxException();
        String s = tok.sval;
        nextToken();
        return s;
    }

    public void eatKeyword(String w) throws BadSyntaxException {
        if (!matchKeyword(w))
            throw new BadSyntaxException();
        nextToken();
    }

    public String eatId() throws BadSyntaxException {
        if (!matchId())
            throw new BadSyntaxException();

        String s = tok.sval;
        nextToken();
        return s;
    }

    private void nextToken() throws BadSyntaxException {
        try {
            tok.nextToken();
        } catch (IOException exception) {
            throw new BadSyntaxException();
        }
    }

    private void initKeywords() {
        keywords = Arrays.asList("select", "from", "where", "and", "insert", "into", "values", "delete", "create",
                "table", "varchar", "int", "as", "index", "on");
    }
}

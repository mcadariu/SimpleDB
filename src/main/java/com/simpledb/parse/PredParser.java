package com.simpledb.parse;

public class PredParser {
    private Lexer lex;

    public PredParser(String s) throws BadSyntaxException {
        lex = new Lexer(s);
    }

    public void field() throws BadSyntaxException {
        lex.eatId();
    }

    public void constant() throws BadSyntaxException {
        if (lex.matchStringConstant())
            lex.eatStringConstant();
        else
            lex.eatIntConstant();
    }

    public void expression() throws BadSyntaxException {
        if (lex.matchId())
            field();
        else
            constant();
    }

    public void term() throws BadSyntaxException {
        expression();
        lex.eatDelim('=');
        expression();
    }

    public void predicate() throws BadSyntaxException {
        term();
        if (lex.matchKeyword("and")) {
            lex.eatKeyword("and");
            predicate();
        }
    }
}

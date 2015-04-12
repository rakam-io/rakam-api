grammar NamedParam;

query: (QUERY_CONTENT optionalParameter)*;

optionalParameter: '[' (STRING namedParameter)* ']';

namedParameter: ':' IDENTIFIER;

IDENTIFIER
    : (ALPHANUMERIC)+;

fragment ALPHANUMERIC
    : [A-Za-z0-9];

STRING : ~(':' | ']')* ;
QUERY_CONTENT : ~('[')* ;
lexer grammar CommonLexerRules; // "lexer grammars for DQDL"

COLON: ':';
COMMA: ',';
SINGLE_QUOTE: '\'';
QUOTE: '"';
LCURL: '{';
RCURL: '}';
LBRAC: '[';
RBRAC: ']';
LPAREN: '(';
RPAREN: ')';
AND: 'and' | 'AND';
OR: 'or' | 'OR';

BETWEEN: 'between' | 'BETWEEN';
EQUAL_TO: '=';
GREATER_THAN: '>';
GREATER_THAN_EQUAL_TO: '>=';
LESS_THAN: '<';
LESS_THAN_EQUAL_TO: '<=';
IN: 'in';

DIGIT: [0-9];
DATE:
	QUOTE DIGIT DIGIT DIGIT DIGIT '-' DIGIT DIGIT '-' DIGIT DIGIT QUOTE;
INT: DIGIT+;
DECIMAL: INT '.' INT;
QUOTED_STRING: QUOTE (ESC | .)*? QUOTE;
NEGATIVE: '-';

LINE_COMMENT:
	'//' .*? '\r'? '\n' -> skip; // Match "//" stuff '\n'
COMMENT: '/*' .*? '*/' -> skip; // Match "/*" stuff "*/"

IDENTIFIER: [a-zA-Z0-9]+;

WS: [ \t\n]+ -> skip;

fragment ESC: '\\"' | '\\\\'; // 2-char sequences \" and \\
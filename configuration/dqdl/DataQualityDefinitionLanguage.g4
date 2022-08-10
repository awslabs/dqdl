grammar DataQualityDefinitionLanguage; // "parser grammars for DQDL"
import CommonLexerRules;

// Data Structures
intArray: LBRAC (INT | DIGIT) (COMMA (INT | DIGIT))* RBRAC;
quotedStringArray:
	LBRAC QUOTED_STRING (COMMA QUOTED_STRING)* RBRAC;

// Sections
rulesSectionStart: 'Rules';

// Expressions
dateNow: 'now()';

dateExpression:
	DATE
	| dateNow
	| LPAREN dateNow ('-' | '+') (DIGIT | INT) RPAREN;
dateArray: LBRAC dateExpression (COMMA dateExpression)* RBRAC;

jobStatusExpression:
	EQUAL_TO QUOTED_STRING
	| IN quotedStringArray;

number:
	DIGIT
	| NEGATIVE DIGIT
	| INT
	| NEGATIVE INT
	| DECIMAL
	| NEGATIVE DECIMAL;

numericThresholdExpression:
	BETWEEN number AND number
	| GREATER_THAN number
	| GREATER_THAN_EQUAL_TO number
	| LESS_THAN number
	| LESS_THAN_EQUAL_TO number
	| EQUAL_TO number;

dateThresholdExpression:
	BETWEEN dateExpression AND dateExpression
	| GREATER_THAN dateExpression
	| GREATER_THAN_EQUAL_TO dateExpression
	| LESS_THAN dateExpression
	| LESS_THAN_EQUAL_TO dateExpression
	| EQUAL_TO dateExpression;

setExpression:
	IN dateArray
	| IN intArray
	| IN quotedStringArray;

ruleType: IDENTIFIER;
parameter: (QUOTED_STRING | INT | DIGIT);
condition:
	jobStatusExpression
	| numericThresholdExpression
	| dateThresholdExpression
	| setExpression;

dqRule: ruleType parameter* condition?;

topLevelRule:
	dqRule
	| '(' dqRule ')' (AND '(' dqRule ')')*
	| '(' dqRule ')' (OR '(' dqRule ')')*;

// Rules Definition
dqRules: topLevelRule (COMMA topLevelRule)*;

// Top Level Document
rules:
	rulesSectionStart EQUAL_TO LBRAC dqRules RBRAC
	| rulesSectionStart EQUAL_TO LBRAC RBRAC; // empty array

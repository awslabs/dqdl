grammar DataQualityDefinitionLanguage; // "parser grammars for DQDL"
import CommonLexerRules;

// Data Structures
intArray: LBRAC (INT | DIGIT) (COMMA (INT | DIGIT))* RBRAC;
quotedStringArray:
	LBRAC QUOTED_STRING (COMMA QUOTED_STRING)* RBRAC;

// Sections
metadataSectionStart: 'Metadata';
sourcesSectionStart: 'Sources';
rulesSectionStart: 'Rules';

// Expressions
dateNow: 'now()';

timeUnit: 'days' | 'hours';

timeExpression: (DIGIT | INT) timeUnit;

dateExpression:
	DATE
	| dateNow
	| LPAREN dateNow ('-' | '+') timeExpression RPAREN;
dateArray: LBRAC dateExpression (COMMA dateExpression)* RBRAC;

jobStatusExpression:
	EQUAL_TO QUOTED_STRING
	| IN quotedStringArray;

timeThresholdExpression:
    BETWEEN timeExpression AND timeExpression
    | GREATER_THAN timeExpression
    | GREATER_THAN_EQUAL_TO timeExpression
    | LESS_THAN timeExpression
    | LESS_THAN_EQUAL_TO timeExpression
    | EQUAL_TO timeExpression;

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

matchesRegexExpression: 'matches' QUOTED_STRING;

ruleType: IDENTIFIER;
parameter: (QUOTED_STRING | INT | DIGIT);
condition:
    matchesRegexExpression
	| setExpression
	| numericThresholdExpression
	| timeThresholdExpression
	| dateThresholdExpression
	| jobStatusExpression;

withThresholdCondition: 'with' 'threshold' numericThresholdExpression;

dqRule: ruleType parameter* condition? withThresholdCondition?;

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

// This dictionary does not support nested dictionaries. Just strings and arrays.
dictionary: LCURL pair (COMMA pair)* RCURL;
pair: QUOTED_STRING COLON pairValue;
pairValue: QUOTED_STRING | array;
array: LBRAC QUOTED_STRING (COMMA QUOTED_STRING)* RBRAC;

metadata: metadataSectionStart EQUAL_TO dictionary;
sources: sourcesSectionStart EQUAL_TO dictionary;

document: metadata? sources? rules;
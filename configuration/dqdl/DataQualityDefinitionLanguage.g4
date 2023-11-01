grammar DataQualityDefinitionLanguage; // "parser grammars for DQDL"
import CommonLexerRules;

// Sections
metadataSectionStart: 'Metadata';
dataSourcesSectionStart: 'DataSources';
rulesSectionStart: 'Rules';
analyzersSectionStart: 'Analyzers';

// Expressions
dateNow: 'now()';

durationUnit: 'days' | 'hours';

durationExpression: (DIGIT | INT) durationUnit;

dateExpressionOp: ('-' | '+');
dateExpression:
	DATE
	| dateNow
	| LPAREN dateNow dateExpressionOp durationExpression RPAREN;

atomicNumber:
	DIGIT
	| NEGATIVE DIGIT
	| INT
	| NEGATIVE INT
	| DECIMAL
	| NEGATIVE DECIMAL;

functionParameters:
    number
    | number (COMMA number)*;

functionCall:
  IDENTIFIER LPAREN RPAREN
  | IDENTIFIER LPAREN functionParameters RPAREN;

numberOp: '+' | '-' | '/' | '*';

number:
    number numberOp number
    | functionCall
    | LPAREN number RPAREN
    | atomicNumber;

quotedString: QUOTED_STRING;

matchesRegexCondition: 'matches' quotedString;

numberArray: LBRAC number (COMMA number)* RBRAC;
numberBasedCondition:
    BETWEEN number AND number
    | GREATER_THAN number
    | GREATER_THAN_EQUAL_TO number
    | LESS_THAN number
    | LESS_THAN_EQUAL_TO number
    | EQUAL_TO number
    | IN numberArray;

quotedStringArray: LBRAC quotedString (COMMA quotedString)* RBRAC;
stringBasedCondition:
    EQUAL_TO quotedString
    | IN quotedStringArray
    | matchesRegexCondition;

dateExpressionArray: LBRAC dateExpression (COMMA dateExpression)* RBRAC;
dateBasedCondition:
	BETWEEN dateExpression AND dateExpression
	| GREATER_THAN dateExpression
	| GREATER_THAN_EQUAL_TO dateExpression
	| LESS_THAN dateExpression
	| LESS_THAN_EQUAL_TO dateExpression
	| EQUAL_TO dateExpression
	| IN dateExpressionArray;

durationExpressionArray: LBRAC durationExpression (COMMA durationExpression)* RBRAC;
durationBasedCondition:
    BETWEEN durationExpression AND durationExpression
    | GREATER_THAN durationExpression
    | GREATER_THAN_EQUAL_TO durationExpression
    | LESS_THAN durationExpression
    | LESS_THAN_EQUAL_TO durationExpression
    | EQUAL_TO durationExpression
    | IN durationExpressionArray;

ruleType: IDENTIFIER;
analyzerType: IDENTIFIER;
parameter: QUOTED_STRING
           | IDENTIFIER;
connectorWord: OF | AND;
parameterWithConnectorWord: connectorWord? parameter;

condition:
    numberBasedCondition
	| stringBasedCondition
	| dateBasedCondition
	| durationBasedCondition;

withThresholdCondition: 'with' 'threshold' numberBasedCondition;

dqRule: ruleType parameterWithConnectorWord* condition? withThresholdCondition?;
dqAnalyzer: analyzerType parameterWithConnectorWord*;

topLevelRule:
	dqRule
	| '(' dqRule ')' (AND '(' dqRule ')')*
	| '(' dqRule ')' (OR '(' dqRule ')')*;

// Rules Definition
dqRules: topLevelRule (COMMA topLevelRule)*;
dqAnalyzers: dqAnalyzer (COMMA dqAnalyzer)*;

// Top Level Document
rules:
	rulesSectionStart EQUAL_TO LBRAC dqRules RBRAC
	| rulesSectionStart EQUAL_TO LBRAC RBRAC; // empty array

analyzers: analyzersSectionStart EQUAL_TO LBRAC dqAnalyzers RBRAC;

// This dictionary does not support nested dictionaries. Just strings and arrays.
dictionary: LCURL pair (COMMA pair)* RCURL;
pair: QUOTED_STRING COLON pairValue;
pairValue: QUOTED_STRING | array;
array: LBRAC QUOTED_STRING (COMMA QUOTED_STRING)* RBRAC;

metadata: metadataSectionStart EQUAL_TO dictionary;
dataSources: dataSourcesSectionStart EQUAL_TO dictionary;
rulesOrAnalyzers: rules | analyzers | rules analyzers;

document: metadata? dataSources? rulesOrAnalyzers;

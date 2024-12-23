grammar DataQualityDefinitionLanguage; //  "parser grammars for DQDL"
import CommonLexerRules;

// Sections
metadataSectionStart: 'Metadata';
dataSourcesSectionStart: 'DataSources';
rulesSectionStart: 'Rules';
analyzersSectionStart: 'Analyzers';

// Expressions
dateNow: 'now()';

durationUnit: 'days' | 'hours' | 'minutes';

durationExpression: (DIGIT | INT) durationUnit;

sizeUnit:
    'B'
    | 'KB'
    | 'MB'
    | 'GB'
    | 'TB';

sizeExpression: (DIGIT | INT) sizeUnit;

timeExpression: TIME | MIL_TIME;

dateExpressionOp: ('-' | '+');
dateExpression:
	DATE
	| dateNow
	| LPAREN dateNow dateExpressionOp durationExpression RPAREN
	| timeExpression
	| NULL;

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
    | atomicNumber
    | NULL;

quotedString: QUOTED_STRING;

matchesRegexCondition: 'matches' quotedString;

numberArray: LBRAC number (COMMA number)* RBRAC;
numberBasedCondition:
    NOT? BETWEEN number AND number
    | GREATER_THAN number
    | GREATER_THAN_EQUAL_TO number
    | LESS_THAN number
    | LESS_THAN_EQUAL_TO number
    | NEGATION? EQUAL_TO number
    | NOT? IN numberArray;

variableDereference: '$' IDENTIFIER;

stringValues:
    quotedString
    | variableDereference
    | NULL
    | EMPTY
    | WHITESPACES_ONLY;

stringValuesArray: LBRAC stringValues (COMMA stringValues)* RBRAC;
stringBasedCondition:
    NEGATION? EQUAL_TO stringValues
    | NOT? IN stringValuesArray
    | NOT? IN variableDereference
    | NOT? matchesRegexCondition;
tagValues: IDENTIFIER;

dateExpressionArray: LBRAC dateExpression (COMMA dateExpression)* RBRAC;
dateBasedCondition:
	NOT? BETWEEN dateExpression AND dateExpression
	| GREATER_THAN dateExpression
	| GREATER_THAN_EQUAL_TO dateExpression
	| LESS_THAN dateExpression
	| LESS_THAN_EQUAL_TO dateExpression
	| NEGATION? EQUAL_TO dateExpression
	| NOT? IN dateExpressionArray;

durationExpressionArray: LBRAC durationExpression (COMMA durationExpression)* RBRAC;
durationBasedCondition:
    NOT? BETWEEN durationExpression AND durationExpression
    | GREATER_THAN durationExpression
    | GREATER_THAN_EQUAL_TO durationExpression
    | LESS_THAN durationExpression
    | LESS_THAN_EQUAL_TO durationExpression
    | NEGATION? EQUAL_TO durationExpression
    | NOT? IN durationExpressionArray;

sizeExpressionArray: LBRAC sizeExpression (COMMA sizeExpression)* RBRAC;
sizeBasedCondition:
    NOT? BETWEEN sizeExpression AND sizeExpression
    | GREATER_THAN sizeExpression
    | GREATER_THAN_EQUAL_TO sizeExpression
    | LESS_THAN sizeExpression
    | LESS_THAN_EQUAL_TO sizeExpression
    | NEGATION? EQUAL_TO sizeExpression
    | NOT? IN sizeExpressionArray;

ruleType: IDENTIFIER;
analyzerType: IDENTIFIER;
parameter: QUOTED_STRING
           | IDENTIFIER;
connectorWord: OF | AND;
parameterWithConnectorWord: connectorWord? parameter;
tagWithCondition: 'with' tagValues (stringBasedCondition | numberBasedCondition);

condition:
    numberBasedCondition
	| stringBasedCondition
	| dateBasedCondition
	| durationBasedCondition
	| sizeBasedCondition;

whereClause: 'where' quotedString;

dqRule: ruleType parameterWithConnectorWord* condition? whereClause? tagWithCondition*;
dqAnalyzer: analyzerType parameterWithConnectorWord*;

// Variable Declarations
expression:
    stringValues
    | stringValuesArray;

variableDeclaration:
    IDENTIFIER EQUAL_TO expression;
variableDeclarations: variableDeclaration*;

topLevelRule:
    LPAREN topLevelRule RPAREN
    | topLevelRule AND topLevelRule
    | topLevelRule OR topLevelRule
    | dqRule;

// Rules Definition
dqRules: topLevelRule (COMMA topLevelRule)*;
dqAnalyzers: dqAnalyzer (COMMA dqAnalyzer)*;

// Top Level Document
rules:
	rulesSectionStart EQUAL_TO LBRAC dqRules RBRAC
	| rulesSectionStart EQUAL_TO LBRAC RBRAC; // empty array

analyzers:
    analyzersSectionStart EQUAL_TO LBRAC dqAnalyzers RBRAC
    | analyzersSectionStart EQUAL_TO LBRAC RBRAC; // empty array

// This dictionary does not support nested dictionaries. Just strings and arrays.
dictionary: LCURL pair (COMMA pair)* RCURL;
pair: QUOTED_STRING COLON pairValue;
pairValue: QUOTED_STRING | array;
array: LBRAC QUOTED_STRING (COMMA QUOTED_STRING)* RBRAC;

metadata: metadataSectionStart EQUAL_TO dictionary;
dataSources: dataSourcesSectionStart EQUAL_TO dictionary;
rulesOrAnalyzers: rules | analyzers | rules analyzers;

document: metadata? dataSources? variableDeclarations? rulesOrAnalyzers;

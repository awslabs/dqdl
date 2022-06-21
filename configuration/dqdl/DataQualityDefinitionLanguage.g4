grammar DataQualityDefinitionLanguage;

// Reserved
COMMA : ',' ;
SINGLE_QUOTE: '\'';
QUOTE : '"';
LCURL : '{' ;
RCURL : '}' ;
LBRAC: '[' ;
RBRAC: ']' ;
LPAREN: '(' ;
RPAREN: ')' ;
AND : 'and'
    | 'AND'
    ;
OR : 'or'
   | 'OR'
   ;
BETWEEN : 'between' ;
EQUAL_TO : '=' ;
GREATER_THAN : '>' ;
GREATER_THAN_EQUAL_TO : '>=' ;
LESS_THAN : '<' ;
LESS_THAN_EQUAL_TO : '<=' ;
IN: 'in' ;

DIGIT: [0-9] ;
DATE : QUOTE DIGIT DIGIT DIGIT DIGIT '-' DIGIT DIGIT '-' DIGIT DIGIT QUOTE;
STRING : QUOTE [a-zA-Z0-9_-]+ QUOTE ;
INT : DIGIT+ ;
DECIMAL: ( '0.' INT  | '1.0');
WS: [ \t\n]+ -> skip ;

REGEX: SINGLE_QUOTE .+? SINGLE_QUOTE ;

// Data Structures
intArray: LBRAC (INT|DIGIT) (COMMA (INT|DIGIT))* RBRAC ;
stringArray: LBRAC STRING (COMMA STRING)* RBRAC ;

// Sections
RULES_SECTION_START : 'rules' ;

// Expressions
dateNow: 'now()' ;
dateExpression: DATE
              | dateNow
              | LPAREN dateNow ('-'|'+') (DIGIT|INT) RPAREN
              ;

jobStatusExpression: EQUAL_TO STRING
                   | IN stringArray
                   ;

numericThresholdExpression: BETWEEN (DIGIT|INT) AND (DIGIT|INT) // TODO: Figure out why (DIGIT|INT) is needed
                          | GREATER_THAN (DIGIT|INT)
                          | GREATER_THAN_EQUAL_TO (DIGIT|INT)
                          | LESS_THAN (DIGIT|INT)
                          | LESS_THAN_EQUAL_TO (DIGIT|INT)
                          | EQUAL_TO (DIGIT|INT)
                          ;

percentageThresholdExpression: BETWEEN DECIMAL AND DECIMAL
                             | GREATER_THAN DECIMAL
                             | GREATER_THAN_EQUAL_TO DECIMAL
                             | LESS_THAN DECIMAL
                             | LESS_THAN_EQUAL_TO DECIMAL
                             | EQUAL_TO DECIMAL
                             ;

dateThresholdExpression: BETWEEN dateExpression AND dateExpression
                       | GREATER_THAN dateExpression
                       | GREATER_THAN_EQUAL_TO dateExpression
                       | LESS_THAN dateExpression
                       | LESS_THAN_EQUAL_TO dateExpression
                       | EQUAL_TO dateExpression
                       ;

setExpression: IN intArray
             | IN stringArray
             ;

// Parameters
columnName: STRING ;
columnType: STRING ;

// Rule Types
jobStatusConstraint: 'JobStatus' jobStatusExpression ;
jobDurationConstraint: 'JobDuration' numericThresholdExpression ;
rowCountConstraint: 'RowCount' numericThresholdExpression ;
// dataSynchronizationConstraint
fileCountConstraint: 'FileCount' numericThresholdExpression ;
isCompleteConstraint: 'IsComplete' columnName ;
columnHasDataTypeConstraint: 'ColumnHasDataType' columnName columnType ;
columnNamesMatchPatternConstraint: 'ColumnNamesMatchPattern' REGEX ;
columnExistsConstraint: 'ColumnExists' columnName ;
datasetColumnCountConstraint: 'DatasetColumnCount' numericThresholdExpression ;
columnCorrelationConstraint: 'ColumnCorrelation' columnName columnName percentageThresholdExpression ;
isUniqueConstraint: 'IsUnique' columnName ;
isPrimaryKeyConstraint: 'IsPrimaryKey' columnName ;
columnValuesConstraint: 'ColumnValues' columnName (numericThresholdExpression|dateThresholdExpression|setExpression);

// Rule Definition
constraint : jobStatusConstraint
           | jobDurationConstraint
           | rowCountConstraint
           | fileCountConstraint
           | isCompleteConstraint
           | columnHasDataTypeConstraint
           | columnNamesMatchPatternConstraint
           | columnExistsConstraint
           | datasetColumnCountConstraint
           | columnCorrelationConstraint
           | isUniqueConstraint
           | isPrimaryKeyConstraint
           | columnValuesConstraint
           ;

dqRule: constraint
      | '(' constraint ')' (AND '(' constraint ')')*
      | '(' constraint ')' (OR '(' constraint ')')*
      ;

// Rules Definition
dqRules: dqRule (COMMA dqRule)* ;

// Top Level Document
rules : RULES_SECTION_START LCURL dqRules RCURL ;

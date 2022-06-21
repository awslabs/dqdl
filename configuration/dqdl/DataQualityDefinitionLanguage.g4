grammar DataQualityDefinitionLanguage;

// Reserved
COMMA : ',' ;
QUOTE : '"';
LCURL : '{' ;
RCURL : '}' ;
LBRAC: '[' ;
RBRAC: ']' ;
AND : 'and'
    | 'AND'
    ;
OR : 'or'
   | 'OR'
   ;
BETWEEEN : 'between' ;
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
INT_ARRAY: LBRAC DIGIT (COMMA DIGIT)* RBRAC ;

WS: [ \t\n]+ -> skip ;

// Sections
RULES_SECTION_START : 'rules' ;

// Expressions
numericThresholdExpression: BETWEEEN INT AND INT
                          | GREATER_THAN INT
                          | GREATER_THAN_EQUAL_TO INT
                          | LESS_THAN INT
                          | LESS_THAN_EQUAL_TO INT
                          | EQUAL_TO INT
                          ;

dateThresholdExpression: BETWEEEN DATE AND DATE
                       | GREATER_THAN DATE
                       | GREATER_THAN_EQUAL_TO DATE
                       | LESS_THAN DATE
                       | LESS_THAN_EQUAL_TO DATE
                       | EQUAL_TO DATE
                       ;

setExpression: IN LBRAC (INT|DIGIT) (COMMA (INT|DIGIT))* RBRAC
             | IN LBRAC STRING (COMMA STRING)* RBRAC
             ;

// Parameters
columnName: STRING ;
columnType: STRING ;

// Rule Types
rowCountConstraint: 'RowCount' numericThresholdExpression ;
isCompleteConstraint: 'IsComplete' columnName ;
isUniqueConstraint: 'IsUnique' columnName ;
columnHasDataTypeConstraint: 'ColumnHasDataType' columnName columnType ;
isPrimaryKeyConstraint: 'IsPrimaryKey' columnName ;
columnValuesConstraint: 'ColumnValues' columnName (numericThresholdExpression|dateThresholdExpression|setExpression);

// Rule Definition
constraint : rowCountConstraint
           | isCompleteConstraint
           | isUniqueConstraint
           | columnHasDataTypeConstraint
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

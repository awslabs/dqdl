grammar DataQualityDefinitionLanguage;

rules : RULES_SECTION_START LCURL dqRules RCURL ;

dqRules: dqRule (COMMA dqRule)* ;

dqRule : columnRuleType COLUMN_NAME
       | columnRuleType COLUMN_NAME
       | datasetRuleType INT;

columnRuleType : 'IsComplete'
               | 'IsUnique'
               ;

datasetRuleType: 'HasRowCount';

RULES_SECTION_START : 'rules' ;
COMMA : ',' ;
QUOTE : '"';
LCURL : '{' ;
RCURL : '}' ;

COLUMN_NAME : QUOTE [a-zA-Z0-9_]+ QUOTE ;
INT : [0-9]+ ;

WS: [ \t\n]+ -> skip ;

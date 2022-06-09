grammar DataQualityDefinitionLanguage;

rules : RULES_SECTION_START LCURL dqRules RCURL ;

dqRules: dqRule (COMMA dqRule)* ;

dqRule : 'IsComplete' | 'IsUnique' | 'HasRowCount' ;

RULES_SECTION_START : 'rules' ;
COMMA : ',' ;
LCURL : '{' ;
RCURL : '}' ;
WS: [ \t]+ -> skip ;

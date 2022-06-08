grammar DataQualityDefinitionLanguage;
r : 'rules { dqRule+ }' ;
dqRule : 'IsComplete' | 'IsUnique' | 'HasRowCount' ;

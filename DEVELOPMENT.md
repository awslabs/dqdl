# Development Guide

## Recommended Environment

- Package has been compiled With Java 11 and Java 17
- Maven 3.9+

## Building the Project

```bash
mvn clean install
```

This will:
1. Generate ANTLR parser from the grammar file (`DataQualityDefinitionLanguage.g4`)
2. Compile source code with Lombok annotation processing
3. Run tests
4. Package the JAR

## Project Structure

```
dqdl/
├── configuration/dqdl/          # ANTLR grammar files
├── src/                         # Source code
├── tst/                         # Test code
└── pom.xml
```


### Running Tests

```bash
mvn test
```

### Building Without Tests

```bash
mvn clean install -DskipTests
```

## Notes

- The project currently compiles to Java 11 bytecode (pom.xml has maven.compiler.source/target=11)
- ANTLR-generated code is placed in `target/generated-sources/antlr4/`

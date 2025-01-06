## AWS Glue Data Quality Definition Language

This package contains the grammar in Antlr g4 format for the Data Quality Definition Language (DQDL).

## Example

The first step is to add a dependency to DQDL. DQDL uses logging and requires an implementation of Slf4j to be available on the classpath. If you are using Maven, then the required dependencies can be added as following.

```
<dependencies>
    <dependency>
        <groupId>software.amazon.glue</groupId>
        <artifactId>dqdl</artifactId>
        <version>1.0.0</version>
    </dependency>
    <dependency>
        <groupId>org.slf4j</groupId>
        <artifactId>slf4j-log4j12</artifactId>
        <version>2.0.16</version>
    </dependency>
</dependencies>
```

Once the dependencies are added, you can start using the `DQDLParser` class which allows you to parse a DQDL ruleset in the form of a string into a modeled `DQRuleset` object. The following snippet of code provides an example for how to do so.

```
import software.amazon.glue.dqdl.exception.InvalidDataQualityRulesetException;
import software.amazon.glue.dqdl.model.DQRuleset;
import software.amazon.glue.dqdl.parser.DQDLParser;

public class App {
    public static void main(String[] args) throws InvalidDataQualityRulesetException {
        String ruleset = "Rules = [ RowCount > 1, ColumnCount > max(avg(last(10))) ]";
        DQDLParser parser = new DQDLParser();

        DQRuleset dqRuleset = parser.parse(ruleset);
        System.out.println("--- Printing out ruleset ---");
        System.out.println(dqRuleset);

        System.out.println("--- Printing out number of rules in the ruleset ---");
        System.out.println(dqRuleset.getRules().size());

        System.out.println("--- Printing out individual rules ---");
        for (int i = 0; i < dqRuleset.getRules().size(); i++) {
            System.out.printf("Rule #%d - %s%n", i+1, dqRuleset.getRules().get(i));
        }
    }
}
```

The output of the code above is as following.

```

--- Printing out ruleset ---
Rules = [
    RowCount > 1,
    ColumnCount > max(avg(last(10)))
]

--- Printing out number of rules in the ruleset ---
2

--- Printing out individual rules ---
Rule #1 - RowCount > 1
Rule #2 - ColumnCount > max(avg(last(10)))
```

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.

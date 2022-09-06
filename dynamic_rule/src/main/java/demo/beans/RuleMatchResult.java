package demo.beans;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class RuleMatchResult {
    String keyByValue;
    String ruleId;
    long trigEventTimestamp;
    long matchTimestamp;
}
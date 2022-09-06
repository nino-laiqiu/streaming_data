package marketing.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TimerCondition {

    private long timeLate;

    private List<EventCombinationCondition> eventCombinationConditionList;

}
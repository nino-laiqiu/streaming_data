package marketing.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class EventCondition {

    /**
     * 规则条件中的一个事件的id
     */
    private String eventId;

    /**
     * 规则条件中的一个事件的属性约束
     */
    private Map<String,String> eventProps;

    /**
     * 规则条件中的一个事件要求的发生时间段起始
     */
    private long timeRangeStart;

    /**
     * 规则条件中的一个事件要求的发生时间段终点
     */
    private long timeRangeEnd;


    /**
     * 规则条件中的一个事件要求的发生次数最小值
     */
    private int minLimit;


    /**
     * 规则条件中的一个事件要求的发生次数最大值
     */
    private int maxLimit;


}
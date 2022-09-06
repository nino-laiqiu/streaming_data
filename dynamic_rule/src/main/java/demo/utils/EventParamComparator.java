package demo.utils;

import demo.beans.EventParam;
import marketing.beans.EventBean;

import java.util.Set;

public class EventParamComparator {
    public static  boolean compare(EventParam param, EventParam target){
        if (param.getEventId().equals(target.getEventId()))  {
            Set<String> keys = param.getEventProperties().keySet();
            for (String key : keys) {
                String targetValue = target.getEventProperties().get(key);
                if (!param.getEventProperties().get(key).equals(targetValue)) return false;
            }
            return true;
        }
        return  false;
    }

    public static boolean compare(EventParam param, EventBean target){
        if (param.getEventId().equals(target.getEventId()))  {
            Set<String> keys = param.getEventProperties().keySet();
            for (String key : keys) {
                String targetValue = target.getProperties().get(key);
                //todo 另一种情况是Properties中有多个keys,但是呢,我的业务要求是只要满足一个就返回true
                if (!param.getEventProperties().get(key).equals(targetValue)) return false;
            }
            return true;

        }
        return  false;
    }
}

package marketing.functions;


import com.alibaba.fastjson.JSON;
import marketing.beans.EventBean;
import org.apache.flink.api.common.functions.MapFunction;

public class Json2BeanMapFunction implements MapFunction<String, EventBean> {
    @Override
    public EventBean map(String value)  {
        return JSON.parseObject(value, EventBean.class);
    }
}

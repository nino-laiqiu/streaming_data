package demo.function;


import org.apache.flink.api.common.functions.MapFunction;
import marketing.beans.EventBean;
import com.alibaba.fastjson.JSON;

public class Json2BeanMapFunction implements MapFunction<String, EventBean> {
    @Override
    public EventBean map(String value)  {
        return JSON.parseObject(value, EventBean.class);
    }
}

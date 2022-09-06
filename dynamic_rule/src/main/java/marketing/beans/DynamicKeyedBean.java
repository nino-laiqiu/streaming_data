package marketing.beans;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class DynamicKeyedBean {

    // 山东省:济南市
    private String keyValue;

    // province,city
    private String keyNames;

    // 携带的数据本身
    private EventBean eventBean;


}

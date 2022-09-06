package marketing.dao;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class HbaseQuerier {
    Table table ;
    String familyName;
    public HbaseQuerier(Connection conn,String profileTableName,String familyName) throws IOException {
        this.familyName = familyName;
        table = conn.getTable(TableName.valueOf(profileTableName));

    }
    public boolean queryProfileConditionIsMatch(Map<String,String> profileCondition ,String deviceId) throws IOException {
        Get get = new Get(deviceId.getBytes());
        //获取key
        Set<String> tags = profileCondition.keySet();
        for (String tag : tags) {
            //要查询的条件
            get.addColumn(familyName.getBytes(), tag.getBytes());
        }
        Result result = table.get(get);
        for (String tag : tags) {
            byte[] v = result.getValue(familyName.getBytes(), tag.getBytes());
            //获取value
            String value = new String(v);
            //比较
            if ( StringUtils.isBlank(value) || profileCondition.get(tag).equals(value) ) return false;
        }
  return true;
    }

}

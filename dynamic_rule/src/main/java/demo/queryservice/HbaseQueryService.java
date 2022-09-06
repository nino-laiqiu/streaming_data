package demo.queryservice;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class HbaseQueryService implements  QueryService{
    Connection hbaseConnection;
    public  HbaseQueryService(Connection hbaseConnection){
        this.hbaseConnection = hbaseConnection;
    }

    public boolean queryProfileCondition(String deviceId, Map<String,String> profileConditions) throws IOException {
        Table profile_table = hbaseConnection.getTable(TableName.valueOf("profile_test"));
        //设置hbase的查询条件
        Get get = new Get(deviceId.getBytes());
        //设置要查询的family和qualifier
        Set<String> tags = profileConditions.keySet();
        for (String tag : tags) {
            get.addColumn("f".getBytes(),tag.getBytes());
        }

        //请求hbase查询
        Result result = profile_table.get(get);
        for (String tag : tags) {
            byte[] resultValue = result.getValue("f".getBytes(), tag.getBytes());
            if (!profileConditions.get(tag).equals(new String(resultValue))) return false;
        }

        return true;
    }
}

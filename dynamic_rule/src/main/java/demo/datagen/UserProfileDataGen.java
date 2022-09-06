package demo.datagen;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;

public class UserProfileDataGen {

    public static void main(String[] args) throws IOException {

        System.setProperty("hadoop.home.dir", "F:\\hadoop\\hadoop-2.2.0-bin-master");
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "192.168.10.100:2181");
        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf("profile_test"));

        ArrayList<Put> puts = new ArrayList<>();
        for (int i = 0; i < 100000; i++) {

            // 生成一个用户的画像标签数据
            String deviceId = StringUtils.leftPad(i + "", 6, "0");
            Put put = new Put(Bytes.toBytes(deviceId));
            for (int k = 1; k <= 100; k++) {
                String key = "tag" + k;
                String value = "v" + RandomUtils.nextInt(1, 3);
                put.addColumn(Bytes.toBytes("f"), Bytes.toBytes(key), Bytes.toBytes(value));
            }

            // 将这一条画像数据，添加到list中
            puts.add(put);

            // 攒满100条一批
            if(puts.size()==100) {
                table.put(puts);
                puts.clear();
            }
        }

        // 提交最后一批
        if(puts.size()>0) table.put(puts);

        conn.close();
    }

}



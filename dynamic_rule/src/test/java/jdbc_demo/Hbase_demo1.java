package jdbc_demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class Hbase_demo1 {

    public static void main(String[] args) throws  IOException {
        System.setProperty("hadoop.home.dir", "F:\\hadoop\\hadoop-2.2.0-bin-master");
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hadoop100");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        Connection connection = ConnectionFactory.createConnection(conf);

        TableName tableName = TableName.valueOf("profile");

        Table table = connection.getTable(tableName);
        Scan scan = new Scan();

        ResultScanner scanner = table.getScanner(scan);

        for (Result result : scanner) {
            //展示数据
            for (Cell cell : result.rawCells()) {
                System.out.println("rowKey=" + Bytes.toString(CellUtil.cloneRow(cell)));
                System.out.println("family=" + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("column=" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("value=" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }

        table.close();
        connection.close();

    }
}

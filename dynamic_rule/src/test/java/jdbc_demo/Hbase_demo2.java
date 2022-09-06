package jdbc_demo;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import demo.utils.ConnectionUtils;

import java.io.IOException;

public class Hbase_demo2 {

    public static void main(String[] args) throws IOException {
        System.setProperty("hadoop.home.dir", "F:\\hadoop\\hadoop-2.2.0-bin-master");
        Connection hbaseConnection = ConnectionUtils.getHbaseConnection();
        System.out.println("------");
        Table table = hbaseConnection.getTable(TableName.valueOf("profile_test"));
        System.out.println(table);
        //Scan所有数据
        Scan scan = new Scan();
        ResultScanner rss = table.getScanner(scan);
        System.out.println(rss);
        for (Result r : rss) {
            System.out.println("----");
            System.out.println("\n row: " + new String(r.getRow()));
        }
    }
}


package demo.utils;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.sql.DriverManager;

@Slf4j
public class ConnectionUtils {
    static Config config = ConfigFactory.load();

    // 获取hbase连接的方法
    public static Connection getHbaseConnection() throws IOException {

        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum",config.getString(ConfigNames.HBASE_ZK_QUORUM));
        Connection hbaseConn = ConnectionFactory.createConnection(conf);
        log.info("hbase connection successfully created");
        return hbaseConn;
    }

    // 获取clickhouse连接的方法
    public static java.sql.Connection getClickhouseConnection() throws Exception {

        String ckDriver = config.getString(ConfigNames.CK_JDBC_DRIVER);
        String ckUrl = config.getString(ConfigNames.CK_JDBC_URL);

        Class.forName(ckDriver);
        java.sql.Connection conn = DriverManager.getConnection(ckUrl);
        log.info("clickhouse connection successfully created");

        return conn;
    }
}

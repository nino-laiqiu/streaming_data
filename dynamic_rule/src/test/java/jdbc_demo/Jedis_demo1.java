package jdbc_demo;

import redis.clients.jedis.Jedis;

public class Jedis_demo1 {

    public static void main(String[] args) {
        Jedis jedis=new Jedis("192.168.10.100",6379);
        //密码
        jedis.auth("123456");
        //保存数据
        jedis.set("name","张三");
        // 获取数据
        String name=jedis.get("name");
        System.out.println("name="+name);
        jedis.close();
    }
}

package marketing.buffer;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.NoArgsConstructor;
import marketing.beans.BufferDataOld;
import marketing.utils.BufferUtil;
import marketing.utils.ConfigNames;
import marketing.utils.ConnectionUtils;
import redis.clients.jedis.Jedis;


public class BufferManagerOldImpl implements BufferManagerOld {
    Jedis redis;
    long ttl;
    public  BufferManagerOldImpl(){
        redis = ConnectionUtils.getRedisConnection();
        Config config = ConfigFactory.load();
         ttl = config.getLong(ConfigNames.REDIS_BUFFER_TTL);

    }

    @Override
    public BufferDataOld getDataFromBuffer(String bufferKey) {
        String value = redis.get(bufferKey);
        return BufferUtil.of(bufferKey,value);
    }

    @Override
    public boolean putDataToBuffer(BufferDataOld bufferDataOld) {
        try {
            redis.psetex(bufferDataOld.getBufferKey(), ttl, bufferDataOld.getEventSeqStr());
        } catch (Exception e) {
            redis.close();
            redis = ConnectionUtils.getRedisConnection();
            return false;
        }
        return true;
    }

    @Override
    public boolean putDataToBuffer(String bufferKey, String bufferValue) {

        try {
            redis.psetex(bufferKey, ttl, bufferValue);
        } catch (Exception e) {
            redis.close();
            redis = ConnectionUtils.getRedisConnection();
            return false;
        }
        return true;
    }



    @Override
    public void delBufferEntry(String bufferKey, String key) {

    }
}

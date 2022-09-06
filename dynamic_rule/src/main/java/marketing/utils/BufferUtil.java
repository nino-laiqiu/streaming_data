package marketing.utils;

import lombok.extern.slf4j.Slf4j;
import marketing.beans.BufferDataOld;

@Slf4j
public class BufferUtil {

    public static BufferDataOld of(String bufferKey , String value){
        BufferDataOld bufferDataOld = new BufferDataOld();
        String[] data = bufferKey.split(":");
        String[] valueFields = value.split(":");
        try {
            bufferDataOld.setDeviceId(data[0]);
            bufferDataOld.setCacheId(data[1]);

            bufferDataOld.setQueryStartTime(Long.parseLong(valueFields[1]));
            bufferDataOld.setGetQueryEndTime(Long.parseLong(valueFields[2]));
            bufferDataOld.setEventSeqStr(valueFields[0]);
        } catch (NumberFormatException e) {
            log.info("缓存数据构造失败,bufferKey={} , value={}",bufferKey,value);
        }
        return bufferDataOld;
    }

    public  static  String genBufferKey(String deviceId ,String cacheId ,long timeStart ,long timeEnd){
          return deviceId +";" + cacheId  + ";" + timeStart +";" +timeEnd;
    }



    public  static  String genBufferValue(String eventSeqStr ,long timeStart ,long timeEnd){
        return eventSeqStr + ";" + timeStart +";" +timeEnd;
    }
}

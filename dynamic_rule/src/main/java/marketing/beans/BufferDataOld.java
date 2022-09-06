package marketing.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import marketing.utils.BufferUtil;

@Slf4j
@Data
@AllArgsConstructor
@NoArgsConstructor
public class BufferDataOld {
    private  String deviceId;
    private  String cacheId;
    private  long queryStartTime;
    private  long getQueryEndTime;
    private  String eventSeqStr;




    public String getBufferKey(){

        return BufferUtil.genBufferKey(this.deviceId,this.cacheId,this.queryStartTime,this.getQueryEndTime);
    }


    public String getBufferValue(){

        return BufferUtil.genBufferValue(this.eventSeqStr,this.queryStartTime,this.getQueryEndTime);
    }
}

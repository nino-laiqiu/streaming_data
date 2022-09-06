package marketing.buffer;

import marketing.beans.BufferData;
import marketing.beans.BufferDataOld;

import java.util.Map;

public interface BufferManagerOld {
    public BufferDataOld getDataFromBuffer(String bufferKey);

    public boolean putDataToBuffer(BufferDataOld bufferDataOld);

    public boolean putDataToBuffer(String bufferKey, String bufferValue);

    public void delBufferEntry(String bufferKey, String key);
}

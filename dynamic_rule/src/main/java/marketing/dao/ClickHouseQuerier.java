package marketing.dao;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import marketing.beans.BufferData;
import marketing.beans.BufferDataOld;
import marketing.beans.EventCombinationCondition;
import marketing.beans.EventCondition;
import marketing.buffer.BufferManager;
import marketing.buffer.BufferManagerImpl;
import marketing.buffer.BufferManagerOld;
import marketing.buffer.BufferManagerOldImpl;
import marketing.utils.ConfigNames;
import marketing.utils.EventUtil;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
public class ClickHouseQuerier {
    Connection connection;
    BufferManager bufferManager;
    long bufferTtl;

    public ClickHouseQuerier(Connection connection) {
        this.connection = connection;
        bufferManager = new BufferManagerImpl();
        Config config = ConfigFactory.load();
        bufferTtl = config.getLong(ConfigNames.REDIS_BUFFER_TTL);

    }

    //按照业务要求的事件,过滤出事件
    public String getEventCombinationConditionStr(String deviceId, EventCombinationCondition combinationCondition, long queryRangeStart, long queryRangeEnd) throws SQLException {
        String querySql = combinationCondition.getQuerySql();
        PreparedStatement statement = connection.prepareStatement(querySql);
        statement.setString(1, deviceId);
        statement.setLong(2, queryRangeStart);
        statement.setLong(3, queryRangeEnd);
        ResultSet resultSet = statement.executeQuery();
        //从条件中取出组合所关系的事件列表例如[A C F]
        //todo 有个问题,如果是AHA,但是过滤后是AA,实际上并不是AA连续,怎么搞
        List<EventCondition> eventConditionList = combinationCondition.getEventConditionList();

        List<String> eventIds = eventConditionList.stream().map(EventCondition::getEventId).collect(Collectors.toList());

        //遍历ck返回结果
        StringBuilder sb = new StringBuilder();
        while (resultSet.next()) {
            String eventId = resultSet.getString(1);
            //统一正则表达式:根据eventId到组合条件的事件列表中找到对应的索引号,来作为最终结果拼接
            //todo
            //indexOf 从o开始算
            sb.append((eventIds.indexOf(eventId) + 1));
        }
        return sb.toString();
    }

    public int queryEventCombinationConditionCount(String deviceId, EventCombinationCondition combinationCondition, long queryRangeStart, long queryRangeEnd) throws SQLException {

        /**
         * 缓存在什么情况下有用？
         *   缓存数据的时间范围： [t3 -> t8]
         *   查询条件的时间范围：
         *             [t3 -> t8]  直接用缓存的结果直接作为方法的返回值
         *             [t3 -> t10] 判断缓存数据的count值是否 >= 条件的count阈值，如果成立，则直接返回缓存结果；否则，用  “缓存的结果+t8->t10”   作为整个返回结果
         *             [t1 -> t8]  判断缓存数据的count值是否 >= 条件的count阈值，如果成立，则直接返回缓存结果；否则，用 “t1->t3+缓存的结果 ”  作为整个返回结果
         *             [t1 -> t10] 判断缓存数据的count值是否 >= 条件的count阈值，如果成立，则直接返回缓存结果；否则，无用
         *
         *   如下逻辑实现，其实没有考虑一个问题：
         *      valueMap中，可能同时存在多个上述的区间范围可能性
         *      下面在遍历缓存数据的时候，可能遇到一个满足条件的就直接判断并往下走了
         *
         *   最好的实现应该是：
         *       比如，条件是  2->12
         *           而valuemap中可能存在的缓存数据：
         *                 2->10
         *                 4->12
         *                 4->10
         *                 1->13
         *      先遍历一遍valueMap，从中找到最优的 缓存区间数据
         *      然后再去判断，并往下走
         */

        String bufferKey = deviceId + ":" + combinationCondition.getCacheId();
        BufferData bufferData = bufferManager.getDataFromBuffer(bufferKey);
        Map<String, String> valueMap = bufferData.getValueMap();

        long current = System.currentTimeMillis();

        for (String key : valueMap.keySet()) {
            String[] split = key.split(":");
            long bufferStartTime = Long.parseLong(split[0]);
            long bufferEndTime = Long.parseLong(split[1]);
            long bufferInsertTime = Long.parseLong(split[2]);
            String eventSequenceStr = valueMap.get(key);

            //  判断缓存是否已过期，做清除动作
            if (System.currentTimeMillis() - bufferInsertTime >= bufferTtl) {
                bufferManager.delBufferEntry(bufferKey, key);
                log.debug("dao-ck,清除过期缓存,bufferKey: {},key: {}", bufferKey, key);
            }


            //查询范围和缓存范围完全相同,直接返回缓存中的数据
            if (bufferStartTime == queryRangeStart && bufferEndTime == queryRangeStart) {
                return EventUtil.sequenceStrMatchRegexCount(eventSequenceStr, combinationCondition.getMatchPattern());
            }


            //左端点对其,且条件的时间范围包含缓存的时间范围
            if (queryRangeStart == bufferStartTime && queryRangeEnd > bufferEndTime) {
                int bufferCount = EventUtil.sequenceStrMatchRegexCount(eventSequenceStr, combinationCondition.getMatchPattern());
                if (combinationCondition.getMinLimit() <= bufferCount) {
                    return bufferCount;
                } else {
                    //调整查询时间,去clickhouse中查一小段
                    String eventSequenceStrCk = getEventCombinationConditionStr(deviceId, combinationCondition, bufferEndTime, queryRangeEnd);

                    // 将原buffer数据从redis中清除
                    bufferManager.delBufferEntry(bufferKey, key);
                    if (StringUtils.isNotBlank(eventSequenceStrCk)) {
                        HashMap<String, String> toPutMap = new HashMap<>();

                        // 写缓存，包含3种区间： 原buffer区间，  右边分段区间 ，  原buffer区间+右半边
                        // |---origin---|
                        //              |--right---|
                        // |---origin------right---|


                        toPutMap.put(bufferStartTime + ":" + bufferEndTime + ":" + current, eventSequenceStr);
                        toPutMap.put(bufferEndTime + ":" + queryRangeEnd + ":" + current, eventSequenceStrCk);
                        toPutMap.put(bufferStartTime + ":" + queryRangeEnd + ":" + current, eventSequenceStr + eventSequenceStrCk);
                        bufferManager.putDataToBuffer(bufferKey, toPutMap);

                        return EventUtil.sequenceStrMatchRegexCount(eventSequenceStr + eventSequenceStrCk, combinationCondition.getMatchPattern());
                    }
                }
            }
            //右端点对其,且条件的时间范围包含缓存的时间范围
            if (queryRangeEnd == bufferEndTime && queryRangeStart < bufferStartTime) {
                int bufferCnt = EventUtil.sequenceStrMatchRegexCount(eventSequenceStr, combinationCondition.getMatchPattern());
                if (combinationCondition.getMinLimit() <= bufferCnt) {
                    return bufferCnt;
                } else {
                    //调整查询时间,去clickhouse中查一小段
                    String eventSequenceStrCk = getEventCombinationConditionStr(deviceId, combinationCondition, queryRangeStart, bufferStartTime);
                    return EventUtil.sequenceStrMatchRegexCount(eventSequenceStrCk + eventSequenceStr, combinationCondition.getMatchPattern());
                }
            }
        }



            String eventSequenceSt = getEventCombinationConditionStr(deviceId, combinationCondition, queryRangeStart, queryRangeEnd);
            return EventUtil.sequenceStrMatchRegexCount(eventSequenceSt, combinationCondition.getMatchPattern());
/**
 * //先查询到用户在组合条件中做过的事件的字符串
 *
 *         //然后取出组合条件中的正则表达式
 *         String pattern = combinationCondition.getMatchPattern();
 *         //匹配正则表达式
 *         Pattern r = Pattern.compile(pattern);
 *         Matcher matcher = r.matcher(eventSequenceStr);
 *         int cnt = 0;
 *         // todo 事件可重叠正则表达式如何表达
 *         while (matcher.find()) {
 *             cnt ++;
 *         }
 *         return  cnt ;
 */


        }

}


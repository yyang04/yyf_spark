package utils;

import com.taobao.tair3.client.Result;
import com.taobao.tair3.client.ResultMap;
import com.taobao.tair3.client.TairClient;
import com.taobao.tair3.client.config.impl.ClientParameters;
import com.taobao.tair3.client.config.impl.SimpleTairConfig;
import com.taobao.tair3.client.config.impl.TairConfig;
import com.taobao.tair3.client.error.TairException;
import com.taobao.tair3.client.error.TairFlowLimit;
import com.taobao.tair3.client.error.TairRpcError;
import com.taobao.tair3.client.error.TairTimeout;
import com.taobao.tair3.client.impl.MultiTairClient;
import com.taobao.tair3.client.util.ByteArray;
import com.taobao.tair3.client.util.StringSerializer;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TairUtil {

    private MultiTairClient client;

    public TairUtil() {
        try {
            MultiTairClient tClient = initFromMcc();
            if (tClient == null) {
                throw new TairException("failed to init Tair");
            }
            tClient.init();
            client = tClient;
        } catch (TairException e) {
            System.out.println(e);
        }
    }

    public void close() {
        client.close();
    }

    public MultiTairClient initFromMcc() throws TairException {
        ClientParameters clientParameters = new ClientParameters(20, 80).setEnableCopyIdcSensitive(true);
        TairConfig config = new SimpleTairConfig("com.sankuai.waimai.ad", "com.sankuai.tair.waimai.ad", clientParameters);
        // 如果需要指定客户端参数，可以传入ClientParameters对象
        return new MultiTairClient(config);
    }

    public String getString(String id, int area, int timeout) throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException {

        Result<byte[]> result = client.get((short) area, id.getBytes(), new TairClient.TairOption(timeout, (short) 0, 0));

        if (result.getCode() != Result.ResultCode.OK) {
            return null;
        }
        return new String(result.getResult());
    }


    public void putString(String key, String value, int area, TairClient.TairOption opt) throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException {
        byte[] keyByte = StringSerializer.serialize(key);
        byte[] valByte = StringSerializer.serialize(value);
        client.put((short) area, keyByte, valByte, opt);
    }


    public void putDouble(String key, double value, int area, TairClient.TairOption opt) throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException, IOException {
        byte[] keyByte = StringSerializer.serialize(key);
        client.setDoubleCount((short) area, keyByte, value, opt);
    }

    public Map<String, String> batchGetString(List<String> ids, int area, int timeout) throws TairRpcError, TairFlowLimit, TairTimeout, InterruptedException {

        List<byte[]> keyList = new ArrayList<>();
        for (String id : ids) {
            if (StringUtils.isNotBlank(id)) {
                keyList.add(id.getBytes());
            }
        }

        ResultMap<ByteArray, Result<byte[]>> result = client.batchGet((short) area, keyList, new TairClient.TairOption(timeout, (short) 0, 0));
        Map<String, String> resultMap = new HashMap<>();

        for (Map.Entry<ByteArray, Result<byte[]>> entry : result.entrySet()) {
            if (entry.getValue().getCode() != Result.ResultCode.OK) {
                continue;
            }
            String keyStr = new String(entry.getKey().getBytes());
            String valueStr = new String(entry.getValue().getResult());
            resultMap.put(keyStr,valueStr);
        }
        return resultMap;
    }




    public Double getDoubleCount(String id, int area, int timeout) throws TairTimeout, InterruptedException, TairFlowLimit, TairRpcError {

        Result<Double> result = client.getDoubleCount((short) area, id.getBytes(), new TairClient.TairOption(timeout, (short) 0, 0));
        if (result.getCode() != Result.ResultCode.OK) {
            return null;
        }
        return result.getResult();
    }
}
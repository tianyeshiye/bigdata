package function2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

public class PairTimeIntervalAggregationFunction implements PairFlatMapFunction<Iterator<Tuple2<TelegramHash, CanUnitBean>>,TelegramHash, CanUnitBean>{
	private static final long serialVersionUID = - 2022345678L;
	private final int timeInterval;

	public PairTimeIntervalAggregationFunction (int timeInterval) {
		this.timeInterval = timeInterval;
	}
	@Override
	public Iterable<Tuple2<TelegramHash, CanUnitBean>> call(
			Iterator<Tuple2<TelegramHash, CanUnitBean>> its) throws Exception {


		//VIN =>  Map<<CanID 最终结果>>
		HashMap<String, TreeMap<Integer, CanUnitBean>> Telegram = new HashMap<String, TreeMap<Integer, CanUnitBean>>();
		//VIN => ReliveTime MAP
		HashMap<String, Long> Timestamp = new HashMap<String, Long>();
		String timeStampInterval = "";
		while (its.hasNext()) {
			Tuple2<TelegramHash, CanUnitBean> next = its.next();
			//以100毫秒取整,000,100,200,etc
			short tmStamp = (short)(next._2.getCanTime()%1000);
			tmStamp = (short) (tmStamp - tmStamp%timeInterval);
			timeStampInterval = "" + next._1.timestamp + tmStamp;
			//VIN=>存放最终结果的Map<canId, Bean>
			TreeMap<Integer, CanUnitBean> rvTelegram = Telegram.get(next._1.deviceId + "," + timeStampInterval);

			if(rvTelegram == null) {
				Timestamp.put(next._1.deviceId + "," + timeStampInterval, next._1.timestamp);

				rvTelegram =new TreeMap<Integer, CanUnitBean>();
				Telegram.put(next._1.deviceId+ "," + timeStampInterval, rvTelegram);
			}

			Short CanId = next._2.getCanId();

			//高16位百毫秒 + 低16位CanID 标识每百毫秒切片的结果
			Integer keyBean = (tmStamp << 16 | CanId);
			CanUnitBean rvCanUnitBean = rvTelegram.get(keyBean);

			if(rvCanUnitBean == null) {
				rvCanUnitBean = new CanUnitBean(next._2);//新规深复制函数

				rvTelegram.put(keyBean, rvCanUnitBean);

				rvCanUnitBean.setCanTime(tmStamp);
			}else{//百毫秒未改变
				//处理假定 某个VIN/CanId标识的所有报文的Label数目相同
				Map<String ,Object> rvMap = (Map<String,Object>)rvCanUnitBean.getConvertedDataMap();
				for (Map.Entry<String, Object> entry  : rvMap.entrySet()) {

					//当前时戳下值为空，pass
					Object refreshValue = next._2.getConvertedDataMap().get(entry.getKey());
					if(refreshValue == null)
						continue;

					rvMap.replace(entry.getKey(),refreshValue);
				}
			}
		}

		//遍历VIN
		ArrayList<Tuple2<TelegramHash, CanUnitBean>> tuble2s = new ArrayList<>();
        for (Map.Entry<String, TreeMap<Integer, CanUnitBean>>telEntry : Telegram.entrySet()) {
        	String vin = telEntry.getKey().split(",")[0];
        	TelegramHash tel = new TelegramHash(vin, Timestamp.get(telEntry.getKey()));


        	//100毫秒内同CanId下结果CanUnitBean作成，No1,No2,No3,No1,NO2,No3,No4,No1,No2......

        	CanUnitBean rvCanBean = new CanUnitBean();

        	Integer mm100 = 0;

        	Map<String, Object> dest = new HashMap<String, Object>();
        	Map<Short, Map<String ,?>> destMap = new HashMap<Short, Map<String ,?>>();
        	//rvCanBean.addResult(dest, mm100);
        	for(Map.Entry<Integer,CanUnitBean> pre1000 : telEntry.getValue().entrySet()) {
        		Short canId = pre1000.getValue().getCanId();
        		//不同100毫秒片断，新建Map<String, Object>存放
        		if(((pre1000.getKey() & 0xffff)>>16) == mm100) {
        			mm100 = (pre1000.getKey() & 0xffff) >>16;
                	dest = new HashMap<String, Object>();
                	destMap.put(canId, dest);
                	rvCanBean.addCanIdConvertedDataMapMap(destMap, mm100);
        		}

        		//同100毫秒内，不同CanId合并
        		Map<String, Object> src = (Map<String, Object>)pre1000.getValue().getConvertedDataMap();//source
        		for(Map.Entry<String, Object> dataEntry  :src.entrySet()){
        			dest.put(dataEntry.getKey(), dataEntry.getValue());
        		}
        	}
        	tuble2s.add(new Tuple2<TelegramHash, CanUnitBean>(tel,rvCanBean));
        }
		return tuble2s;
	}

}

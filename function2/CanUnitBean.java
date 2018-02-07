package function2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class CanUnitBean implements Serializable , KryoSerializable{

	private static final long serialVersionUID = - 2022345678L;

	private short canId;
	private long canTime;
	private Map<String ,?> convertedDataMap;
	private Map<Short, Map<String ,?>> canIdConvertedDataMapMap = new HashMap<Short, Map<String ,?>>();

	private List<Integer> timeIntervalList =   new ArrayList<Integer>();

	public boolean compareToTimeInterval(CanUnitBean other, int timeInterval) {
		long tmStampThis = ( long)(this.getCanTime()%100000);
		tmStampThis = (long) (tmStampThis - tmStampThis%timeInterval);
		long tmStampOther = ( long)(other.getCanTime()%100000);
		tmStampOther = (long) (tmStampOther - tmStampOther%timeInterval);

		return tmStampThis == tmStampOther;
	}
	public void addCanIdConvertedDataMapMap(Map<Short, Map<String ,?>> canIdConvertedDataMapMap,
			Integer preIntervalCanUnitBean) {
		this.canIdConvertedDataMapMap = canIdConvertedDataMapMap;
		this.timeIntervalList.add(preIntervalCanUnitBean);
	}

	public CanUnitBean() {

	}

	public  CanUnitBean(CanUnitBean clone) {
		this.canId = clone.canId;
		this.canTime = clone.canTime;
		this.convertedDataMap = new HashMap<>(clone.convertedDataMap);
	}
	@Override
	public void read(Kryo arg0, Input arg1) {
		// TODO 自動生成されたメソッド・スタブ

	}

	@Override
	public void write(Kryo arg0, Output arg1) {
		// TODO 自動生成されたメソッド・スタブ

	}

	public short getCanId() {
		return canId;
	}

	public void setCanId(short canId) {
		this.canId = canId;
	}

	public long getCanTime() {
		return canTime;
	}

	public void setCanTime(long canTime) {
		this.canTime = canTime;
	}

	public Map<String ,?> getConvertedDataMap() {
		return convertedDataMap;
	}

	public void setConvertedDataMap(Map<String ,?> convertedDataMap) {
		this.convertedDataMap = convertedDataMap;
	}

	public List<Integer> getTimeIntervalList() {
		return timeIntervalList;
	}

	public Map<Short, Map<String ,?>> getCanIdConvertedDataMapMap() {
		return canIdConvertedDataMapMap;
	}

}

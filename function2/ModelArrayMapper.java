package function2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

public class ModelArrayMapper implements Serializable{

	public Map<Short,List<Pair<Integer, String>>> getFields() {
		Map<Short,List<Pair<Integer, String>>> fields = new HashMap<Short,List<Pair<Integer, String>>>();
		List<Pair<Integer, String>> list_0x01 = new ArrayList<Pair<Integer, String>>();
		list_0x01.add(new ImmutablePair<Integer,String>(2,"c001_dummy15"));
		list_0x01.add(new ImmutablePair<Integer,String>(3,"c001_dummy16"));
		list_0x01.add(new ImmutablePair<Integer,String>(4,"c001_dummy17"));
		fields.put((short)0x01, list_0x01);

		List<Pair<Integer, String>> list_0x22 = new ArrayList<Pair<Integer, String>>();
		list_0x22.add(new ImmutablePair<Integer,String>(5,"c022_dummy15"));
		list_0x22.add(new ImmutablePair<Integer,String>(6,"c022_dummy16"));
		list_0x22.add(new ImmutablePair<Integer,String>(7,"c022_dummy17"));
		list_0x22.add(new ImmutablePair<Integer,String>(8,"c022_dummy18"));
		fields.put((short)0x22, list_0x22);

		List<Pair<Integer, String>> list_0x122 = new ArrayList<Pair<Integer, String>>();
		list_0x122.add(new ImmutablePair<Integer,String>(9,"c122_dummy15"));
		list_0x122.add(new ImmutablePair<Integer,String>(10,"c122_dummy16"));
		list_0x122.add(new ImmutablePair<Integer,String>(11,"c122_dummy17"));
		list_0x122.add(new ImmutablePair<Integer,String>(12,"c122_dummy18"));
		list_0x122.add(new ImmutablePair<Integer,String>(13,"c122_dummy19"));
		fields.put((short)0x122, list_0x122);

		List<Pair<Integer, String>> list_0x201 = new ArrayList<Pair<Integer, String>>();
		list_0x201.add(new ImmutablePair<Integer,String>(14,"c201_dummy15"));
		list_0x201.add(new ImmutablePair<Integer,String>(15,"c201_dummy16"));
		list_0x201.add(new ImmutablePair<Integer,String>(16,"c201_dummy17"));
		list_0x201.add(new ImmutablePair<Integer,String>(17,"c201_dummy18"));
		list_0x201.add(new ImmutablePair<Integer,String>(18,"c201_dummy19"));
		list_0x201.add(new ImmutablePair<Integer,String>(19,"c201_dummy20"));
		fields.put((short)0x201, list_0x201);
		return fields;

	}
	public Object[] map(Tuple2<TelegramHash, CanUnitBean> tuple) {
		Object[] array = new Object[20];
		array[0] = tuple._1.deviceId;
		array[1] = tuple._1.timestamp;
		for (Map.Entry<Short, Map<String, ?>> telEntry : tuple._2.getCanIdConvertedDataMapMap().entrySet()) {
			Short canid = telEntry.getKey();
			Map<String, ?> map = telEntry.getValue();
			final List<Pair<Integer, String>> canFields = getFields().get(canid);
			setConvertedData(array, canFields,map);
		}
		return array;
	}
	private void setConvertedData(Object[] array, List<Pair<Integer, String>> canFields, Map<String, ?> map) {
		for (Pair<Integer, String> canField : canFields) {
			array[canField.getKey()] = map.get(canField.getValue());
		}
	}

	public StructType schema() {
		List<Pair<String,Class<?>>> canLabels = new ArrayList<Pair<String,Class<?>>>();
		canLabels.add(new ImmutablePair<String,Class<?>>("c001_dummy15",long.class));
		canLabels.add(new ImmutablePair<String,Class<?>>("c001_dummy16",long.class));
		canLabels.add(new ImmutablePair<String,Class<?>>("c001_dummy17",long.class));


		canLabels.add(new ImmutablePair<String,Class<?>>("c022_dummy15",long.class));
		canLabels.add(new ImmutablePair<String,Class<?>>("c022_dummy16",long.class));
		canLabels.add(new ImmutablePair<String,Class<?>>("c022_dummy17",long.class));
		canLabels.add(new ImmutablePair<String,Class<?>>("c022_dummy18",long.class));


		canLabels.add(new ImmutablePair<String,Class<?>>("c122_dummy15",long.class));
		canLabels.add(new ImmutablePair<String,Class<?>>("c122_dummy16",long.class));
		canLabels.add(new ImmutablePair<String,Class<?>>("c122_dummy17",long.class));
		canLabels.add(new ImmutablePair<String,Class<?>>("c122_dummy18",long.class));
		canLabels.add(new ImmutablePair<String,Class<?>>("c122_dummy19",long.class));

		canLabels.add(new ImmutablePair<String,Class<?>>("c201_dummy15",long.class));
		canLabels.add(new ImmutablePair<String,Class<?>>("c201_dummy16",long.class));
		canLabels.add(new ImmutablePair<String,Class<?>>("c201_dummy17",long.class));
		canLabels.add(new ImmutablePair<String,Class<?>>("c201_dummy18",long.class));
		canLabels.add(new ImmutablePair<String,Class<?>>("c201_dummy19",long.class));
		canLabels.add(new ImmutablePair<String,Class<?>>("c201_dummy20",long.class));
		return createSchema(canLabels);
	}

	private static final StructField[] SCHEMA_COMMON_FLIEDS;
	static {
		SCHEMA_COMMON_FLIEDS = new StructField[] {
				DataTypes.createStructField("vin", DataTypes.StringType, false),
				DataTypes.createStructField("timestamp", DataTypes.LongType, false),
		};
	}

	private StructType createSchema(List<Pair<String,Class<?>>> canLabels) {
		List<StructField> fields = new ArrayList<>(Arrays.asList(SCHEMA_COMMON_FLIEDS));
		for(Pair<String,Class<?>> canLabel : canLabels) {
			DataType dataType = null;
			if (canLabel.getValue() == long.class) {
				dataType = DataTypes.LongType;
			} else if(canLabel.getValue() == short.class) {
				dataType = DataTypes.ShortType;
			}else if(canLabel.getValue() == int.class) {
				dataType = DataTypes.IntegerType;
			}
			else if(canLabel.getValue() == String.class) {
				dataType = DataTypes.StringType;
			} else {
				throw new IllegalStateException("no type");
			}
			fields.add(DataTypes.createStructField(canLabel.getKey(), dataType, true));
		}
		return DataTypes.createStructType(fields);
	}



}

package function2;

import java.util.HashMap;
import java.util.Map;

public class Unit {
	public static CanUnitBean createCanUnitBean(short id ,String canTime) {
		CanUnitBean bean = new CanUnitBean();
		bean.setCanId(id);
		bean.setCanTime(Long.parseLong(canTime));
		Map<String,Object> values = new HashMap<>();

		switch(id) {
			case 0x01:
			  values.put("c001_dummy15", 1L);
			  values.put("c001_dummy16", 2L);
			  values.put("c001_dummy17", 3L);
			  break;
			case 0x22:
			  values.put("c022_dummy15", 1L);
			  values.put("c022_dummy16", 2L);
			  values.put("c022_dummy17", 3L);
			  values.put("c022_dummy18", 4L);
			  break;
			case 0x122:
			  values.put("c122_dummy15", 1L);
			  values.put("c122_dummy16", 2L);
			  values.put("c122_dummy17", 3L);
			  values.put("c122_dummy18", 4L);
			  values.put("c122_dummy19", 5L);
			  break;
			case 0x201:
			  values.put("c201_dummy15", 1L);
			  values.put("c201_dummy16", 2L);
			  values.put("c201_dummy17", 3L);
			  values.put("c201_dummy18", 4L);
			  values.put("c201_dummy19", 5L);
			  values.put("c201_dummy20", 6L);
			  break;
		}
		bean.setConvertedDataMap(values);
		return bean;
	}
	public static CanUnitBean createCanUnitBeanNull(short id ,String canTime) {
		CanUnitBean bean = new CanUnitBean();
		bean.setCanId(id);
		bean.setCanTime(Long.parseLong(canTime));
		Map<String,Object> values = new HashMap<>();

		switch(id) {
		case 0x01:
			  values.put("c001_dummy15", 1L);
			  values.put("c001_dummy16", null);
			  values.put("c001_dummy17", 3L);
			  break;
			case 0x22:
			  values.put("c022_dummy15", 1L);
			  values.put("c022_dummy16", null);
			  values.put("c022_dummy17", null);
			  values.put("c022_dummy18", 4L);
			  break;
			case 0x122:
			  values.put("c122_dummy15", 1L);
			  values.put("c122_dummy16", null);
			  values.put("c122_dummy17", null);
			  values.put("c122_dummy18", null);
			  values.put("c122_dummy19", 5L);
			  break;
			case 0x201:
			  values.put("c201_dummy15", 1L);
			  values.put("c201_dummy16", 2L);
			  values.put("c201_dummy17", null);
			  values.put("c201_dummy18", null);
			  values.put("c201_dummy19", null);
			  values.put("c201_dummy20", null);
			  break;
		}
		bean.setConvertedDataMap(values);
		return bean;
	}

	public static CanUnitBean createCanUnitBean2(short id ,String canTime) {
		CanUnitBean bean = new CanUnitBean();
		bean.setCanId(id);
		bean.setCanTime(Long.parseLong(canTime));
		Map<String,Object> values = new HashMap<>();

		switch(id) {
		case 0x01:
			  values.put("c001_dummy15", 11L);
			  values.put("c001_dummy16", 22L);
			  values.put("c001_dummy17", 33L);
			  break;
			case 0x22:
			  values.put("c022_dummy15", 11L);
			  values.put("c022_dummy16", 22L);
			  values.put("c022_dummy17", 33L);
			  values.put("c022_dummy18", 44L);
			  break;
			case 0x122:
			  values.put("c122_dummy15", 11L);
			  values.put("c122_dummy16", 22L);
			  values.put("c122_dummy17", 33L);
			  values.put("c122_dummy18", 44L);
			  values.put("c122_dummy19", 55L);
			  break;
			case 0x201:
			  values.put("c201_dummy15", 11L);
			  values.put("c201_dummy16", 22L);
			  values.put("c201_dummy17", 33L);
			  values.put("c201_dummy18", 44L);
			  values.put("c201_dummy19", 55L);
			  values.put("c201_dummy20", 66L);
			  break;
		}
		bean.setConvertedDataMap(values);
		return bean;
	}
}

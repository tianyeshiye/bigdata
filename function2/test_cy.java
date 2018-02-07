package function2;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class test_cy {

	public static void main(String[] args) {
		SparkConf conf=new SparkConf().setAppName("CanTimeReducer").setMaster("local[2]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		//1506787111000 0x01 0x22 0x122 0x201
		TelegramHash telegramHash1 = new TelegramHash("1111",1506787111000L);
		CanUnitBean	canUnitBean11 = Unit.createCanUnitBean((short)0x01,"1506787111010");
		CanUnitBean	canUnitBean12 = Unit.createCanUnitBean((short)0x22,"1506787111020");
		CanUnitBean	canUnitBean13 = Unit.createCanUnitBean((short)0x122,"1506787111030");
		CanUnitBean	canUnitBean14 = Unit.createCanUnitBean((short)0x201,"1506787111050");

		CanUnitBean	canUnitBean11_1 = Unit.createCanUnitBean2((short)0x01,"1506787111110");
		CanUnitBean	canUnitBean12_1 = Unit.createCanUnitBean2((short)0x22,"1506787111120");
		CanUnitBean	canUnitBean13_1 = Unit.createCanUnitBean2((short)0x122,"1506787111130");
		CanUnitBean	canUnitBean14_1 = Unit.createCanUnitBean2((short)0x201,"1506787111150");

		CanUnitBean	canUnitBean11_null = Unit.createCanUnitBeanNull((short)0x01,"1506787111110");
		CanUnitBean	canUnitBean12_null  = Unit.createCanUnitBeanNull((short)0x22,"1506787111120");
		CanUnitBean	canUnitBean13_null = Unit.createCanUnitBeanNull((short)0x122,"1506787111130");
		CanUnitBean	canUnitBean14_null = Unit.createCanUnitBeanNull((short)0x201,"1506787111150");

		//1506787222000 0x01 0x22 0x122 0x201
		TelegramHash telegramHash2 = new TelegramHash("2222",1506787222000L);
		CanUnitBean	canUnitBean21 = Unit.createCanUnitBean((short)0x01,"1506787222010");
		CanUnitBean	canUnitBean22 = Unit.createCanUnitBean((short)0x22,"1506787222020");
		CanUnitBean	canUnitBean23 = Unit.createCanUnitBean((short)0x122,"1506787222030");
		CanUnitBean	canUnitBean24 = Unit.createCanUnitBean((short)0x201,"1506787222040");

		CanUnitBean	canUnitBean21_1 = Unit.createCanUnitBean2((short)0x01,"1506787222110");
		CanUnitBean	canUnitBean22_1 = Unit.createCanUnitBean2((short)0x22,"1506787222120");
		CanUnitBean	canUnitBean23_1 = Unit.createCanUnitBean2((short)0x122,"1506787222130");
		CanUnitBean	canUnitBean24_1 = Unit.createCanUnitBean2((short)0x201,"1506787222140");
		CanUnitBean	canUnitBean21_null = Unit.createCanUnitBeanNull((short)0x01,"1506787222210");
		CanUnitBean	canUnitBean22_null = Unit.createCanUnitBeanNull((short)0x22,"1506787222220");
		CanUnitBean	canUnitBean23_null = Unit.createCanUnitBeanNull((short)0x122,"1506787222230");
		CanUnitBean	canUnitBean24_null = Unit.createCanUnitBeanNull((short)0x201,"1506787222240");

		JavaRDD<Tuple2<TelegramHash, CanUnitBean>> rdd = sc.parallelize(Arrays.asList(
				new Tuple2<TelegramHash, CanUnitBean>(telegramHash1,canUnitBean11),
				new Tuple2<TelegramHash, CanUnitBean>(telegramHash1,canUnitBean12),
				new Tuple2<TelegramHash, CanUnitBean>(telegramHash1,canUnitBean13),
				new Tuple2<TelegramHash, CanUnitBean>(telegramHash1,canUnitBean14),

				new Tuple2<TelegramHash, CanUnitBean>(telegramHash1,canUnitBean11_1),
				new Tuple2<TelegramHash, CanUnitBean>(telegramHash1,canUnitBean12_1),
				new Tuple2<TelegramHash, CanUnitBean>(telegramHash1,canUnitBean13_1),
				new Tuple2<TelegramHash, CanUnitBean>(telegramHash1,canUnitBean14_1),
				new Tuple2<TelegramHash, CanUnitBean>(telegramHash1,canUnitBean11_null),
				new Tuple2<TelegramHash, CanUnitBean>(telegramHash1,canUnitBean12_null),
				new Tuple2<TelegramHash, CanUnitBean>(telegramHash1,canUnitBean13_null),
				new Tuple2<TelegramHash, CanUnitBean>(telegramHash1,canUnitBean14_null),

				new Tuple2<TelegramHash, CanUnitBean>(telegramHash2,canUnitBean21),
				new Tuple2<TelegramHash, CanUnitBean>(telegramHash2,canUnitBean22),
				new Tuple2<TelegramHash, CanUnitBean>(telegramHash2,canUnitBean23),
				new Tuple2<TelegramHash, CanUnitBean>(telegramHash2,canUnitBean24),

				new Tuple2<TelegramHash, CanUnitBean>(telegramHash2,canUnitBean21_1),
				new Tuple2<TelegramHash, CanUnitBean>(telegramHash2,canUnitBean22_1),
				new Tuple2<TelegramHash, CanUnitBean>(telegramHash2,canUnitBean23_1),
				new Tuple2<TelegramHash, CanUnitBean>(telegramHash2,canUnitBean24_1),

				new Tuple2<TelegramHash, CanUnitBean>(telegramHash2,canUnitBean21_null),
				new Tuple2<TelegramHash, CanUnitBean>(telegramHash2,canUnitBean22_null),
				new Tuple2<TelegramHash, CanUnitBean>(telegramHash2,canUnitBean23_null),
				new Tuple2<TelegramHash, CanUnitBean>(telegramHash2,canUnitBean24_null)
				),1);

		JavaPairRDD<TelegramHash, CanUnitBean> pairRDD = JavaPairRDD.fromJavaRDD(rdd);
		int timeInterval = 100;
		JavaPairRDD<TelegramHash, CanUnitBean> pairRDDReducer =
				pairRDD.mapPartitionsToPair(new PairTimeIntervalAggregationFunction(timeInterval)
				);


		pairRDDReducer.foreach(new VoidFunction<Tuple2<TelegramHash, CanUnitBean>>() {
            @Override
            public void call(Tuple2<TelegramHash, CanUnitBean> tp2) throws Exception {
                System.out.println(tp2._1.deviceId + "->" + tp2._1.timestamp + "" + tp2._2.getCanIdConvertedDataMapMap().toString());

            }
        });
	}

}

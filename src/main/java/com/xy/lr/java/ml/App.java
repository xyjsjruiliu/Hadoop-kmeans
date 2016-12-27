package com.xy.lr.java.ml;

import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.hadoop.io.Text;

import java.util.*;

/**
 * Hello world!
 *
 */
public class App {
    public static void main( String[] args ) {
        /*String valueBuilder = "一个reduce对应一个输出文件，而不是输出文件夹，如果想要输出到多个文件夹建议使用MultiOutputFormat，如何使用请自己脑补。至于如何获取reducer编号，应该可以在reduce内部使用context.getTaskAttemptID().getTaskID().getId()获得";
        String text = valueBuilder.toString();
        // 使用Ansj工具包分词
        String result = ToAnalysis.parse(text).toString(",");

        for (String word : result.split(",")) {
//            System.out.println(word);
            if(word.split("/").length == 2) {
                String string = word.split("/")[1];
                String string1 = word.split("/")[0];

                System.out.println(string + "\t" + string1);
            }
        }*/

//        System.out.println(Math.round(10000));

        Map<Integer, String> map = new HashMap<Integer, String>();

        map.put(1, "1");
        map.put(2, "2");
        map.put(3, "3");

        //已经选择的编号
        List<Integer> order = new ArrayList<Integer>();

        Integer[] keys = map.keySet().toArray(new Integer[0]);
        //随机数发生器
        Random random = new Random();
        //最终值
        Map<Integer, String> result = new HashMap<Integer, String>();
        while (true) {
            if (order.size() == 2)
                break;
            Integer randomKey = keys[random.nextInt(keys.length)];
            if (order.contains(randomKey))
                continue;
            order.add(randomKey);
            String randomValue = map.get(randomKey);

            System.out.println(randomKey + "\t" + randomValue);

//                result.put(randomKey, randomValue);
//            out.append(new Text(randomKey.toString()), new Text(randomValue));
        }
    }
}

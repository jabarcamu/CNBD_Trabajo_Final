package com.cnbd.aasv.hadoop.job5;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
    /*
     * Metodo Reduce, colecciona el output de el Mapper calculado y agrega el conteo
     * de palabras.
     */
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        HashMap<String, Integer> map = new HashMap<String, Integer>();
        /*
         * Itera a traves de todos los valores disponibles con una clave [word] y los
         * aniade juntos y nos da el resultado final como la clave y la suma estos
         * valores a lo largo con el DocID
         */
        for (Text val : values) {
            if (map.containsKey(val.toString())) {
                map.put(val.toString(), map.get(val.toString()) + 1);
            } else {
                map.put(val.toString(), 1);
            }
        }
        StringBuilder docValueList = new StringBuilder();
        for (String docID : map.keySet()) {
            docValueList.append(docID + ":" + map.get(docID) + " ");
        }
        context.write(key, new Text(docValueList.toString()));
    }
}


package com.cnbd.aasv.hadoop.job5;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InvertedIndexMapper extends Mapper<Object, Text, Text, Text> {

    /*
     * Hadoop soporta tipos de datod. Son un tipo de dato especifico de Hadoop que
     * es usado para manejar numeros y Strings en un ambiente Hadoop. IntWritable y
     * Text son usados en ves de Integer de Java y tipo de dato String. Aqui 'one'
     * es el numero de ocurrencias de 'word' y es ingresar valor 1 durante el
     * proceso de Mapeo.
     */
    // private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        // Dividir DocID y el texto actual
        String DocId = value.toString().substring(0, value.toString().indexOf("\t"));
        String value_raw = value.toString().substring(value.toString().indexOf("\t") + 1);

        // Leyendo la entra de una linea al mismo tiempo y toquenizar usando espacio ,
        // "'", y "-" como caracteres tokenizer
        StringTokenizer itr = new StringTokenizer(value_raw, " '-");

        // Iterando a traves de todas las palabras disponibles en cada linea y formando
        // el par clave/valor.
        while (itr.hasMoreTokens()) {
            // Remove special characters
            word.set(itr.nextToken().replaceAll("[^a-zA-Z]", "").toLowerCase());
            if (word.toString() != "" && !word.toString().isEmpty()) {
                /*
                 * Enviando a ouput collector(Context) el cual por turnos pasa al output del
                 * Reducer El output sigue como: 'word1' 5722018411 'word1' 6722018415 'word2'
                 * 6722018415
                 */
                context.write(word, new Text(DocId));
            }
        }
    }
}

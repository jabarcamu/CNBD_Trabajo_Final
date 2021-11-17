package com.cnbd.aasv.hadoop.job5;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.wikiclean.WikiClean;

public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

    /*
     * Hadoop soporta tipos de datod. Son un tipo de dato especifico de Hadoop que
     * es usado para manejar numeros y Strings en un ambiente Hadoop. IntWritable y
     * Text son usados en ves de Integer de Java y tipo de dato String. Aqui 'one'
     * es el numero de ocurrencias de 'word' y es ingresar valor 1 durante el
     * proceso de Mapeo.
     */
    // private final static IntWritable one = new IntWritable(1);    

    private Text word = new Text();
    private WikiClean cleaner = new WikiClean.Builder()
            .withFooter(false)
            .withTitle(false)
            .build();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] titleAndText;

        try {
            titleAndText = parseTitleAndText(value);
        } catch (IllegalArgumentException e) {
            //throw new IOException("Article not parsed.");
            return;
        }


        String pageString = titleAndText[0];
        String text = titleAndText[1].toLowerCase();

        if(notValidPage(pageString))
            return;

        pageString = pageString.trim();
        pageString = pageString.replace(' ', '_');

        Text page = new Text(pageString);
        StringTokenizer tokenizer = new StringTokenizer(text, " \t\n\r\f\",.:;?!#[](){}*");

        while (tokenizer.hasMoreTokens()) {
            String wordText = tokenizer.nextToken();
            word.set(wordText);
            context.write(word, page);
        }
    }

    private boolean notValidPage(String pageString) {
        return pageString.contains(":");
    }

    private String[] parseTitleAndText(Text value) {
        String[] titleAndText = new String[2];

        String valueStr = value.toString();

        titleAndText[0] = cleaner.getTitle(valueStr);

        try {
            titleAndText[1] = cleaner.clean(valueStr);
        } catch (IllegalArgumentException e) {            
            throw e;
        }

        return titleAndText;
    }
}

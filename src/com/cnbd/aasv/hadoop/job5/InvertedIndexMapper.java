package com.cnbd.aasv.hadoop.job5;

import java.util.regex.*;
import java.io.IOException;
import java.util.StringTokenizer;
import java.nio.charset.CharacterCodingException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
// import org.wikiclean.WikiClean;

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
    // private WikiClean cleaner = new WikiClean.Builder()
    //         .withFooter(false)
    //         .withTitle(false)
    //         .build();

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
            String filterKeyword = wordText.replaceAll("^[\"']+|[\"']+$", "");
            
            if(Pattern.matches("^[a-zA-Z]+$",filterKeyword) == false){
              continue;
            }
            word.set(filterKeyword);
            context.write(word, page);
        }
    }

    private boolean notValidPage(String pageString) {
        return pageString.contains(":");
    }

    private String[] parseTitleAndText(Text value) throws CharacterCodingException{
        String[] titleAndText = new String[2];
        
        int start = value.find("<title>");
        int end = value.find("</title>", start);
        start += 7; //aniadir tamanho <title> .
        
        titleAndText[0] = Text.decode(value.getBytes(), start, end-start);

        start = value.find("<text");
        start = value.find(">", start);
        end = value.find("</text>", start);
        start += 1;
        
        if(start == -1 || end == -1) {
            return new String[]{"",""};
        }
        
        titleAndText[1] = Text.decode(value.getBytes(), start, end-start);
        
        return titleAndText;
    }

}

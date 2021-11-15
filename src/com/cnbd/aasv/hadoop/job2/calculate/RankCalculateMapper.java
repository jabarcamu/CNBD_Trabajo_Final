package com.cnbd.aasv.hadoop.job2.calculate;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RankCalculateMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        int pageTabIndex = value.find("\t");
        int rankTabIndex = value.find("\t", pageTabIndex+1);

        String page = Text.decode(value.getBytes(), 0, pageTabIndex);
        String pageWithRank = Text.decode(value.getBytes(), 0, rankTabIndex+1);
        
        // Marcar pagina como una pagina existente (ignorar wiki-links rojos)        
        context.write(new Text(page), new Text("!"));

        // Saltar paginas sin links        
        if(rankTabIndex == -1) return;
        
        String links = Text.decode(value.getBytes(), rankTabIndex+1, value.getLength()-(rankTabIndex+1));
        String[] allOtherPages = links.split(",");
        int totalLinks = allOtherPages.length;
        
        for (String otherPage : allOtherPages){
            Text pageRankTotalLinks = new Text(pageWithRank + totalLinks);
            context.write(new Text(otherPage), pageRankTotalLinks);
        }
        
        // Poner los links originales en la pagina para la salida de Reduce        
        context.write(new Text(page), new Text("|" + links));
    }
}

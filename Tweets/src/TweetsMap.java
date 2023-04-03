
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
public class TweetsMap extends MapReduceBase implements
Mapper<LongWritable, Text, Text, IntWritable>
{
//hadoop supported data types
private final static IntWritable one = new IntWritable(1);

//map method that performs the tokenizer job and framing the initial key value pairs
// after all lines are converted into key-value pairs, reducer is called.
public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable>
output, Reporter reporter) throws IOException
{
	List<String>  Good = (List<String>) Arrays.asList("Parfait","Bien","Satisfait","merci");
	List<String>  Bad = (List<String>) Arrays.asList("nul","insatisfait","cassé","mauvais");
	int GoodWord = 0;
	int BadWord = 0;
	String word = null;
	Text Sentiment = new Text();
	//taking one line at a time from input file and tokenizing the same
	String line = value.toString();
	StringTokenizer tokenizer = new StringTokenizer(line);
	//iterating through all the words available in that line and forming the key value pair
	while (tokenizer.hasMoreTokens())
	{
	word = tokenizer.nextToken();
	if(Good.contains(word))
		GoodWord = 1;
	if(Bad.contains(word))
		BadWord = 1;
	
	}
	//sending to output collector which inturn passes the same to reducer
	if(GoodWord == 1 & BadWord == 0)
	{
		Sentiment.set("Satisfait");
	}
		else if(GoodWord == 0 & BadWord == 1)
		{
			Sentiment.set("Insatisfait");
		}
	else
	{
		Sentiment.set("Inconcluant");
	}
	output.collect(Sentiment, one);
	}
	}
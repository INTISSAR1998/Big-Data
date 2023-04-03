import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Iterator;

// Notre classe REDUCE.
public class AnagrammeReduce extends Reducer<Text, Text, Text, Text> 
{
    // La fonction REDUCE elle-même. Les arguments: la clef key, un Iterable de toutes les valeurs
    // qui sont associées à la clef en question, et le contexte Hadoop.
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
    	// La fonction REDUCE elle-même. Les arguments: la clef key, un Iterable de toutes les valeurs
	    // qui sont associées à la clef en question, et le contexte Hadoop.
        String result = "";
        Boolean First = true;
        Iterator<Text> Val = values.iterator(); // Pour parcourir toutes les valeurs associées à la clef fournie.

        // Pour chaque valeur...
        while(Val.hasNext()) 
        {
            // Premier mot, donc on n'inclut pas le symbole "|".
            if(First) 
            {
                // Notre chaîne de résultat contient initiallement le premier mot.
                result = Val.next().toString(); 
                First = false;			
            }
            else
                // On concatene le mot à la chaîne de resultat.
                result += " | " + Val.next().toString();

        }
        
        // On renvoie le couple (clef;valeur) constitué de notre clef key et de la chaîne concaténée.
        context.write(key, new Text(result));
    }
	
}

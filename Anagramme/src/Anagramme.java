import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;

// Note classe Driver (contient le main du programme Hadoop).

public class Anagramme {

    // Le main du programme.
	public static void main(String[] args) throws Exception
	{
    // Créé un object de configuration Hadoop.
	Configuration Conf = new Configuration();	
	// Permet à Hadoop de lire ses arguments génériques, récupère les arguments restants dans ourArgs.
    String [] HadArgs =  new GenericOptionsParser(Conf, args).getRemainingArgs();
	// Obtient un nouvel objet Job: une tâche Hadoop. On fourni la configuration Hadoop ainsi qu'une description
    // textuelle de la tâche.
    Job job = Job.getInstance(Conf, "Projet Anagramme");
	
    // Défini les classes driver, map et reduce.
	job.setJarByClass(Anagramme.class);
	job.setMapperClass(AnagrammeMap.class);
	job.setReducerClass(AnagrammeReduce.class);
	
    // Défini types clefs/valeurs de notre programme Hadoop.	
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	
    // Défini les fichiers d'entrée du programme et le répertoire des résultats.
    // On se sert du premier et du deuxième argument restants pour permettre à l'utilisateur de les spécifier
    // lors de l'exécution.
	FileInputFormat.addInputPath(job, new Path(HadArgs[0]));
	FileOutputFormat.setOutputPath(job, new Path(HadArgs[1]));
	
    // On lance la tâche Hadoop. Si elle s'est effectuée correctement, on renvoie 0. Sinon, on renvoie -1.    
	if(job.waitForCompletion(true))
		System.exit(0);
	System.exit(-1);
	}
}

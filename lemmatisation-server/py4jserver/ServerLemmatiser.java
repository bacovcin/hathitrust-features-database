package py4jserver;

import java.io.*;
import java.net.*;
import java.util.*;

import edu.northwestern.at.morphadorner.*;
import edu.northwestern.at.morphadorner.corpuslinguistics.spellingstandardizer.*;
import edu.northwestern.at.morphadorner.corpuslinguistics.namerecognizer.Names;
import edu.northwestern.at.morphadorner.corpuslinguistics.lemmatizer.*;
import edu.northwestern.at.morphadorner.corpuslinguistics.lexicon.*;

import edu.northwestern.at.utils.*;

public class ServerLemmatiser {
	public Names names = new Names();

	public Lexicon emeLexicon;
	public SpellingStandardizer emeStandardizer;
	public Lemmatizer emeLemmatizer;

	public Lexicon eceLexicon;
	public SpellingStandardizer eceStandardizer;
	public Lemmatizer eceLemmatizer;

	public Lexicon ncfLexicon;
	public SpellingStandardizer ncfStandardizer;
	public Lemmatizer ncfLemmatizer;

	public String outfilename;

	public void initialise()
		throws Exception
	{

		System.out.println("Beginning initialisation...");

		emeLexicon = new DefaultLexicon();

		URL emelexicon = URLUtils.getURLFromFileNameOrURL("morphadorner/data/emelexicon.lex");
		emeLexicon.loadLexicon
			(
			 	emelexicon,
			 	"utf-8"
			);
		System.out.println("EME Lexicon loaded...");

		eceLexicon = new DefaultLexicon();

		URL ecelexicon = URLUtils.getURLFromFileNameOrURL("morphadorner/data/eccolexicon.lex");
		eceLexicon.loadLexicon
			(
			 	ecelexicon,
			 	"utf-8"
			);
		System.out.println("ECE Lexicon loaded...");

		ncfLexicon = new DefaultLexicon();

		URL ncflexicon = URLUtils.getURLFromFileNameOrURL("morphadorner/data/ncflexicon.lex");
		ncfLexicon.loadLexicon
			(
			 	ncflexicon,
			 	"utf-8"
			);
		System.out.println("NCF Lexicon loaded...");

		emeStandardizer = new DefaultSpellingStandardizer();
		//emeStandardizer.setLexicon(emeLexicon);
		URL standardspellings = URLUtils.getURLFromFileNameOrURL("morphadorner/data/standardspellings.txt");
		emeStandardizer.loadStandardSpellings
			(
			 	standardspellings,
			 	"utf-8"
			);

		emeStandardizer.addStandardSpellings(names.getFirstNames() );
		emeStandardizer.addStandardSpellings(names.getSurnames() );
		emeStandardizer.addStandardSpellings(names.getPlaceNames().keySet() );

		URL spellbwc = URLUtils.getURLFromFileNameOrURL("morphadorner/data/spellingsbywordclass.txt");
		emeStandardizer.loadAlternativeSpellingsByWordClass
			(
			 	spellbwc,
				"utf-8"
			);

		System.out.println("EME standardizer initialized...");
	
		eceStandardizer = new DefaultSpellingStandardizer();
		//eceStandardizer.setLexicon(eceLexicon);
		eceStandardizer.loadStandardSpellings
			(
			 	standardspellings,
			 	"utf-8"
			);

		eceStandardizer.addStandardSpellings(names.getFirstNames() );
		eceStandardizer.addStandardSpellings(names.getSurnames() );
		eceStandardizer.addStandardSpellings(names.getPlaceNames().keySet() );

		eceStandardizer.loadAlternativeSpellingsByWordClass
			(
			 	spellbwc,
				"utf-8"
			);

		System.out.println("ECE standardizer initialized...");
	
		ncfStandardizer = new DefaultSpellingStandardizer();
		//ncfStandardizer.setLexicon(ncfLexicon);
		ncfStandardizer.loadStandardSpellings
			(
			 	standardspellings,
			 	"utf-8"
			);

		ncfStandardizer.addStandardSpellings(names.getFirstNames() );
		ncfStandardizer.addStandardSpellings(names.getSurnames() );
		ncfStandardizer.addStandardSpellings(names.getPlaceNames().keySet() );

		ncfStandardizer.loadAlternativeSpellingsByWordClass
			(
			 	spellbwc,
				"utf-8"
			);

		System.out.println("NCF standardizer initialized...");
	
		emeLemmatizer = new DefaultLemmatizer();

		emeLemmatizer.setLexicon(emeLexicon);
		
		emeLemmatizer.setDictionary
		(
			emeStandardizer.getStandardSpellings() 	
		);

		System.out.println("EME Lemmatizer initialised...");
		
		eceLemmatizer = new DefaultLemmatizer();

		eceLemmatizer.setLexicon(eceLexicon);
		
		eceLemmatizer.setDictionary
		(
			eceStandardizer.getStandardSpellings() 	
		);

		System.out.println("ECE Lemmatizer initialised...");


		ncfLemmatizer = new DefaultLemmatizer();

		ncfLemmatizer.setLexicon(ncfLexicon);
		
		ncfLemmatizer.setDictionary
		(
			ncfStandardizer.getStandardSpellings() 	
		);

		System.out.println("NCF Lemmatizer initialised...");


		return;
	}

	public String[] emeLemmatise(String[] args ) 
		throws Exception
	{
		String[] output = new String[5];
		output[0] = args[0];
		output[3] = args[3];
		output[4] = args[4];

		output[1] = 
			emeStandardizer.standardizeSpelling
			(
			 	args[0],
				args[1]
			);
		output[2] = emeLemmatizer.lemmatize( args[0], args[2]);

		if ( output[1].equalsIgnoreCase( args[0] ) )
		{
			output[1] = args[0];
		}
		if (emeLexicon.containsEntry(output[2])) {
			return output;
		} else
		{
			return null;
		}
	}

	public String[] eceLemmatise(String[] args ) 
		throws Exception
	{
		String[] output = new String[5];
		output[0] = args[0];
		output[3] = args[3];
		output[4] = args[4];

		output[1] = 
			eceStandardizer.standardizeSpelling
			(
			 	args[0],
				args[1]
			);
		output[2] = eceLemmatizer.lemmatize( args[0], args[2]);

		if ( output[1].equalsIgnoreCase( args[0] ) )
		{
			output[1] = args[0];
		}
		if (eceLexicon.containsEntry(output[2])) {
			return output;
		} else
		{
			return null;
		}
	}

	public String[] ncfLemmatise(String[] args ) 
		throws Exception
	{
		String[] output = new String[5];
		output[0] = args[0];
		output[3] = args[3];
		output[4] = args[4];

		output[1] = 
			ncfStandardizer.standardizeSpelling
			(
				args[0],
				args[1]
			);
		output[2] = ncfLemmatizer.lemmatize( args[0], args[2]);

		if ( output[1].equalsIgnoreCase( args[0] ) )
		{
			output[1] = args[0];
		}
		if (ncfLexicon.containsEntry(output[2])) {
			return output;
		} else
		{
			return null;
		}
	}

	public String adornEmeFile(String infilename)
		throws Exception
	{
		String[] output = new String[5];
		String[] outnameparts = new String[2];
		outnameparts[0] = infilename.split("-")[0];
		outnameparts[1] = "output.txt";
		outfilename = String.join("-",outnameparts);
		PrintWriter writer = new PrintWriter(outfilename,"UTF-8");
		BufferedReader reader = new BufferedReader(new FileReader(infilename));
		String line;
		while ((line = reader.readLine()) != null)
		{
			try
			{
				output = emeLemmatise(line.split("\t"));
				if (output != null) 
				{
					writer.println(String.join("\t", output));
				}
			} catch(Exception e)
			{
				continue;
			}
		}
		writer.close();
		return outfilename;
	}

	public String adornEceFile(String infilename)
		throws Exception
	{
		String[] output = new String[5];
		String[] outnameparts = new String[2];
		outnameparts[0] = infilename.split("-")[0];
		outnameparts[1] = "output.txt";
		outfilename = String.join("-",outnameparts);
		PrintWriter writer = new PrintWriter(outfilename,"UTF-8");
		BufferedReader reader = new BufferedReader(new FileReader(infilename));
		String line;
		while ((line = reader.readLine()) != null)
		{
			try
			{
				output = eceLemmatise(line.split("\t"));
				if (output != null) 
				{
					writer.println(String.join("\t", output));
				}
			} catch(Exception e)
			{
				continue;
			}
		}
		writer.close();
		return outfilename;
	}

	public String adornNcfFile(String infilename)
		throws Exception
	{
		String[] output = new String[5];
		String[] outnameparts = new String[2];
		outnameparts[0] = infilename.split("-")[0];
		outnameparts[1] = "output.txt";
		outfilename = String.join("-",outnameparts);
		PrintWriter writer = new PrintWriter(outfilename,"UTF-8");
		BufferedReader reader = new BufferedReader(new FileReader(infilename));
		String line;
		while ((line = reader.readLine()) != null)
		{
			try
			{
				output = ncfLemmatise(line.split("\t"));
				if (output != null) 
				{
					writer.println(String.join("\t", output));
				}
			} catch(Exception e)
			{
				continue;
			}
		}
		writer.close();
		return outfilename;
	}
}

package lemmatizer;

// Import useful java libraries
import java.io.*;
import java.net.*;
import java.util.*;

// Get the relevant MorphAdorner classes
import edu.northwestern.at.morphadorner.*;
import edu.northwestern.at.morphadorner.corpuslinguistics.spellingstandardizer.*;
import edu.northwestern.at.morphadorner.corpuslinguistics.namerecognizer.Names;
import edu.northwestern.at.morphadorner.corpuslinguistics.lemmatizer.*;
import edu.northwestern.at.morphadorner.corpuslinguistics.partsofspeech.*;
import edu.northwestern.at.morphadorner.corpuslinguistics.lexicon.*;

public class MyLemmatizer {
	// Get the list of proper names and part of speech tags
	public Names names = new Names();
	public NUPOSPartOfSpeechTags nupos;

	// Declare EME objects
	public Lexicon emeLexicon;
	public SpellingStandardizer emeStandardizer;
	public Lemmatizer emeLemmatizer;

	// Declare ECCO objects
	public Lexicon eceLexicon;
	public SpellingStandardizer eceStandardizer;
	public Lemmatizer eceLemmatizer;

	// Declare NCF objects
	public Lexicon ncfLexicon;
	public SpellingStandardizer ncfStandardizer;
	public Lemmatizer ncfLemmatizer;

	// This function opens all of the lexicons and initializes the lemmatizers
	// and spelling standardizers
	public void initialise()
		throws Exception
	{
		// Load the part of speech tags
		nupos = new NUPOSPartOfSpeechTags();

		// Get ready to find the English data in the resources folder
		ClassLoader classLoader = getClass().getClassLoader();

		System.out.println("Beginning initialisation...");

		// Load the lexicons
		emeLexicon = new DefaultLexicon();

		URL emelexicon = classLoader.getResource("emelexicon.lex");
		emeLexicon.loadLexicon
			(
			 	emelexicon,
			 	"utf-8"
			);
		System.out.println("EME Lexicon loaded...");

		eceLexicon = new DefaultLexicon();

		URL ecelexicon = classLoader.getResource("eccolexicon.lex");
		eceLexicon.loadLexicon
			(
			 	ecelexicon,
			 	"utf-8"
			);
		System.out.println("ECE Lexicon loaded...");

		ncfLexicon = new DefaultLexicon();

		URL ncflexicon = classLoader.getResource("ncflexicon.lex");
		ncfLexicon.loadLexicon
			(
			 	ncflexicon,
			 	"utf-8"
			);
		System.out.println("NCF Lexicon loaded...");

		// Set up the spelling standardizers
		emeStandardizer = new DefaultSpellingStandardizer();
		URL standardspellings = classLoader.getResource("standardspellings.txt");
		emeStandardizer.loadStandardSpellings
			(
			 	standardspellings,
			 	"utf-8"
			);

		emeStandardizer.addStandardSpellings(names.getFirstNames() );
		emeStandardizer.addStandardSpellings(names.getSurnames() );
		emeStandardizer.addStandardSpellings(names.getPlaceNames().keySet() );

		URL spellbwc = classLoader.getResource("spellingsbywordclass.txt");
		emeStandardizer.loadAlternativeSpellingsByWordClass
			(
			 	spellbwc,
				"utf-8"
			);

		System.out.println("EME standardizer initialized...");
	
		eceStandardizer = new DefaultSpellingStandardizer();
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
	
		// Set up the lemmatizers
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

	// Here is the function for EME lemmatization (the other two functions are
	// the same but referencing different corpora. I'm only commenting on this
	// function.
	public String[] emeLemmatise(String[] args ) 
		throws Exception
	{
		// Initialize the output
		String[] output = new String[3];

		// Store the attested form as part of the output
		output[0] = args[0];

		// Get the standard spelling
		output[1] = 
			emeStandardizer.standardizeSpelling
			(
			 	args[0], // the attested form
				args[1] // the major word class
			);

		// Get the lemma
		output[2] = emeLemmatizer.lemmatize( args[0], args[2]);

		// Keep capitalization in standardized spelling
		if ( output[1].equalsIgnoreCase( args[0] ) )
		{
			output[1] = args[0];
		}

		// Make sure the lemma is an appropriate word for
		// the word class (i.e., that it occurs in the lexicon
		// for the given word class, otherwise return null
		LexiconEntry myEntry = emeLexicon.getLexiconEntry(output[2]);
		if (myEntry != null) {
			String[] poses = myEntry.getCategories();
			for (String s: poses)
			{
				String[] wcs = nupos.getMajorWordClass(s).split("/");
				for (String wc: wcs)
				{
					if (wc.equalsIgnoreCase(args[1]))
					{
						return output;
					}
				}
			}
			return null;
		} else
		{
			return null;
		}
	}

	public String[] eceLemmatise(String[] args ) 
		throws Exception
	{
		String[] output = new String[3];
		output[0] = args[0];

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

		LexiconEntry myEntry = eceLexicon.getLexiconEntry(output[2]);
		if (myEntry != null) {
			String[] poses = myEntry.getCategories();
			for (String s: poses)
			{
				String[] wcs = nupos.getMajorWordClass(s).split("/");
				for (String wc: wcs)
				{
					if (wc.equalsIgnoreCase(args[1]))
					{
						return output;
					}
				}
			}
			return null;
		} else
		{
			return null;
		}
	}

	public String[] ncfLemmatise(String[] args ) 
		throws Exception
	{
		String[] output = new String[3];
		output[0] = args[0];


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
		LexiconEntry myEntry = ncfLexicon.getLexiconEntry(output[2]);
		if (myEntry != null) {
			String[] poses = myEntry.getCategories();
			for (String s: poses)
			{
				String[] wcs = nupos.getMajorWordClass(s).split("/");
				for (String wc: wcs)
				{
					if (wc.equalsIgnoreCase(args[1]))
					{
						return output;
					}
				}
			}
			return null;
		} else
		{
			return null;
		}
	}
}

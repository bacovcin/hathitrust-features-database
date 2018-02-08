// Import code to deal with file IO
import java.io._
import org.apache.commons.compress.compressors.bzip2._
import net.liftweb.json._
import scala.io._
import java.util.Calendar
import com.madhukaraphatak.sizeof.SizeEstimator

// Import mutable sets for tracking unique lemmakeys
import scala.collection.mutable.{HashSet => MSet,Map => MMap}

// Import Try code to capture exceptions
import scala.util.{Try, Success, Failure}
import java.util.NoSuchElementException

// Import the JDBC libraries
import java.sql._

// Import the Akka code for multiprocessing
import akka.actor._
import akka.actor.ActorSystem._
import akka.routing._
import scala.concurrent.duration._

// Get code to deal with file removal
import sys.process._

/** Downloads the Hathitrust Extracted Features Files, extracts English data,
 *  inserts data into database*/
object Main extends App
{
  println(Runtime.getRuntime().maxMemory())
  // Database information
  if (args.length == 0) 
  {
        println("You need to provide the location of the filelist for processing!")
  }
  val batchnumber: String = args(0)
  val txtloc = "hathitrust-textfiles/"

  // Number of workers
  val numDataWorkers = args(1).toInt // Workers who read in files and insert data

  // Memory Buffer Size
  val maxMem = args(2).toInt // Maximum memory as back pressure (in GB)

  // Initial wait time
  val waitTime = args(3).toInt // Initial amount of time to wait before checking if memory has decreased

  // Messages for the Akka workers to send to one another
  sealed trait AkkaMessage
  case class StartMain(numDataWorkers: Int) extends AkkaMessage
  case class DBInitialize(filename: String) extends AkkaMessage
  case class Process(filename: String) extends AkkaMessage 
  case class WriteToDB(outputString: String) extends AkkaMessage
  case class LogMessage(processname: String,workerid:String, action: String, timestamp: String) extends AkkaMessage
  case class Printer(mymessage: String)
  case object Initialize extends AkkaMessage
  case object NeedWork extends AkkaMessage
  case object Close extends AkkaMessage
  case object DataDone extends AkkaMessage
  case object Finished extends AkkaMessage
  case object FinishedFile extends AkkaMessage
  case object DBFinished extends AkkaMessage

  /** Main code for the programme */
  def main() : Unit = 
  {
    val system = ActorSystem("AkkaSystem")
    val master = system.actorOf(Props[MainDispatcher],name="MainDispatcher")

    // Start the main Akka actor
    master ! StartMain( numDataWorkers)
  }

  /** Distributes files to data workers  
   *
   *  Gets the files in batches, sends the files individually to data workers,
   *  once all the files have been processed tell the lemma distributer, once
   *  all the lemmatisation is done, normalise the database, apply indexes, and
   *  quit
   *  */
  class MainDispatcher extends Actor {
    import context._
    var databatches: List[List[String]] = List[List[String]]()
    var curbatch: List[String] = List[String]()
    var nextbatch: List[String] = List[String]()
    var dataworkers = context.actorOf(RoundRobinPool(0).props(Props[DataWorker]), "dummyrouter")
    var maxNumWorkers: Int = 0
    var curNumWorkers: Int = 0
    val dbroutermeta = context.system.actorOf(Props[DBRouterWorker],name="DBMetarouter")
	dbroutermeta ! DBInitialize("metadata/"+batchnumber+"-new.txt")
    val dbrouterdata = context.system.actorOf(Props[DBRouterWorker],name="DBDatarouter")
	dbrouterdata ! DBInitialize("tokens/"+batchnumber+"-new.txt")
    var finishedWorkers: Int = 0
    var dbClosed: Int = 0
    var dbfinished = 0
    var firstFinish: Boolean = true
	var gb = 1024*1024*1024
	var filesProcessed: Int = 0
	var remainingFiles: Int = 0
	var currentMem: Long = 0

    /** Recursively populate the list of dataworkers with workers*/
    def createDataWorkers(numWorkers: Int): Unit =
    {
	  this.dataworkers = context.actorOf(RoundRobinPool(numWorkers).props(Props[DataWorker]), "router")
    }

    /** Split the filelist into download batches*/
    def createBatches() : Unit =
    {
      println("Loading file list...")
      val filelistfile = Source.fromFile("filelists/list-"+batchnumber+".txt")
      this.curbatch = filelistfile.getLines.toList
	  this.remainingFiles = this.curbatch.length
      filelistfile.close
	  println("Finished loading file list...")
	  context.system.scheduler.scheduleOnce(waitTime milliseconds)
	  {
		self ! NeedWork
	  }
	  self ! NeedWork
    }

    /** Perform task for each message */
    def receive = 
    {
      // Create the database structure, create the actors, and initialise the
      // batches
      case StartMain(numDataWorkers) =>
      {
        this.maxNumWorkers = numDataWorkers
        createDataWorkers(numDataWorkers)
        createBatches()
	  }

      // Send the next file in the batch to the data actor
      case NeedWork =>
      {
	    val myRuntime = Runtime.getRuntime
		this.currentMem = ((myRuntime.totalMemory - myRuntime.freeMemory) / this.gb)
        if (this.curbatch.length > 0)
        {
		  if (this.currentMem < maxMem)
		  {
            if ((this.curNumWorkers < this.maxNumWorkers) &&
                (this.curbatch.length > 0))
            {
          	  val head::tail = this.curbatch
          	  this.dataworkers ! Process(head)
          	  this.curbatch = tail
			  this.filesProcessed += 1
			  this.curNumWorkers += 1
			  this.currentMem = ((myRuntime.totalMemory - myRuntime.freeMemory) / this.gb)
	          context.system.scheduler.scheduleOnce(10 milliseconds)
	          {
		        self ! NeedWork
	          }
            }
		  } else
		  {
		    if (this.curNumWorkers == 0)
			{
          	  val head::tail = this.curbatch
          	  this.dataworkers ! Process(head)
          	  this.curbatch = tail
			  this.filesProcessed += 1
			  this.curNumWorkers += 1
			} else
			{
		      context.system.scheduler.scheduleOnce(waitTime milliseconds)
		      {
		        self ! NeedWork
		      }
			}
		  }
        } else
        {
		  if (this.firstFinish)
		  {
            this.dataworkers ! Broadcast(DataDone)
			this.firstFinish = false
		  }
        }
      }

	  case FinishedFile =>
	  {
	    this.remainingFiles -= 1
		if (this.remainingFiles % 10 == 0)
		{
	      val myRuntime = Runtime.getRuntime
		  this.currentMem = ((myRuntime.totalMemory - myRuntime.freeMemory) / this.gb)
		  println("Only " + 
		          this.remainingFiles.toString + 
				  " files remaining to process with " +
				  this.curNumWorkers.toString +
				  " workers working...\t"+
				  Calendar.getInstance().getTime().toString+
				  "\t Current memory usage: "+
				  this.currentMem.toString+"GB"
				  )
		}
		this.curNumWorkers -= 1
		self ! NeedWork
	  }

      case DataDone =>
	  {
	  	this.finishedWorkers += 1
		if (this.finishedWorkers == this.maxNumWorkers)
		{
          this.dbroutermeta ! DBFinished
          this.dbrouterdata ! DBFinished
		}
	  }

      // Learn that all the work is done and do finishing touches on database and quit
      case DBFinished =>
      {
        this.dbfinished += 1
        if (this.dbfinished == 2)
        {
          println("Shutting down...")
          context.system.terminate()
        }
      }
    }
  }


  /** Process files, insert metadata and token level data, and send lemmaKeys to 
   *  lemma dispatcher*/
  class DataWorker extends Actor
  {
    // A Map for converting POS tags into Morphadorner classes:
    val posdict: Map[String,List[String]] =
      scala.collection.immutable.Map(
        ("\\u0027",List("none","punctuation","punctuation")),
        ("-",List("none","punctuation","punctuation")),
        ("\\u0022",List("none","punctuation","punctuation")),
        ("#",List("none","punctuation","punctuation")),
        ("$",List("none","punctuation","punctuation")),
        ("(",List("none","punctuation","punctuation")),
        (")",List("none","punctuation","punctuation")),
        (",",List("none","punctuation","punctuation")),
        (",",List("none","punctuation","punctuation")),
        (".",List("none","punctuation","punctuation")),
        ("\\u003A",List("none","punctuation","punctuation")),
        ("\\u0060",List("none","punctuation","punctuation")),
        ("CC",List("conjunction","conjunction","coordinating conjunction")),
        ("CD",List("numeral","numeral","numeral")),
        ("DT",List("determiner","determiner","determiner")),
        ("EX",List("none","existential there","existential there")),
        ("FW",List("none","foreign","foreign")),
        ("IN",List("preposition","preposition","preposition")),
        ("JJ",List("adjective","adjective","adjective")),
        ("JJR",List("adjective","adjective","adjective")),
        ("JJS",List("adjective","adjective","adjective")),
        ("JJSS",List("adjective","adjective","adjective")),
        ("LS",List("none","list item marker","list item marker")),
        ("MD",List("verb","verb","modal verb")),
        ("NN",List("noun","noun","noun")),
        ("NNP",List("noun","noun","noun")),
        ("NNPS",List("noun","noun","noun")),
        ("NNS",List("noun","noun","noun")),
        ("NP",List("noun","noun","noun")),
        ("NPS",List("noun","noun","noun")),
        ("PDT",List("predeterminer","predeterminer","predeterminer")),
        ("POS",List("none","posessive ending","possessive ending")),
        ("PP",List("pronoun","pronoun","pronoun")),
        ("PRP",List("pronoun","pronoun","possessive pronoun")),
        ("PRP$",List("pronoun","pronoun","possessive pronoun")),
        ("PRPR$",List("pronoun","pronoun","possessive pronoun")),
        ("RB",List("adverb","adverb","adverb")),
        ("RBR",List("adverb","adverb","adverb")),
        ("RBS",List("adverb","adverb","adverb")),
        ("RP",List("particle","particle","particle")),
        ("SYM",List("none","symbol","symbol")),
        ("TO",List("none","literal to","literal to")),
        ("UH",List("interjection","interjection","interjection")),
        ("VB",List("verb","verb","verb")),
        ("VBD",List("verb","verb","verb")),
        ("VBG",List("verb","verb","verb")),
        ("VBN",List("verb","verb","verb")),
        ("VBP",List("verb","verb","verb")),
        ("VBZ",List("verb","verb","verb")),
        ("WDT",List("none","wh-determiner","wh-determiner")),
        ("WP",List("none","wh-pronoun","wh-pronoun")),
        ("WP$",List("none","wh-pronoun","wh-pronoun")),
        ("WRB",List("none","wh-adverb","wh-adverb")))

    // Deal with messages
    def receive =
    {
	  case DataDone =>
	  {
        sender ! DataDone
	  }
      // Get a file to process
      case Process(file) =>
      {
        try
		{
          val volstream = Source.fromInputStream(new BZip2CompressorInputStream(new BufferedInputStream(new FileInputStream(file)))).getLines.next
          val volfile = parse(volstream).values.asInstanceOf[Map[Any,Any]]
          val curmeta = volfile("metadata").asInstanceOf[Map[Any,Any]]
          // Only use English data
          if (curmeta("language").asInstanceOf[String] == "eng")
          {
            val volID = curmeta("volumeIdentifier").asInstanceOf[String].replace("'","''")
            val year = curmeta("pubDate").asInstanceOf[String].toInt
            // Deal with metadata
            context.actorSelection("/user/DBMetarouter") ! WriteToDB(List[String](volID,curmeta("title").asInstanceOf[String].replace("'","''"),year.toString,curmeta("pubPlace").asInstanceOf[String],curmeta("imprint").asInstanceOf[String].replace("'","''"),curmeta("genre").asInstanceOf[List[String]].mkString(","),curmeta("names").asInstanceOf[List[String]].mkString(";").replace("'","''")).mkString("\t"))
            var corpus = ""
            if (year < 1700)
            {
              corpus = "eme"
            } else if (year < 1800)
            {
              corpus = "ece" 
            } else
            {
              corpus = "ncf"
            }

            // Get tokens
            val pages = volfile("features").asInstanceOf[Map[Any,Any]]("pages").asInstanceOf[List[Map[Any,Any]]]
	  	    var forms = MMap[scala.Array[String],BigInt]()
            for (page <- pages)
            {
              var tokens = page("body").asInstanceOf[Map[String,Any]]("tokenPosCount").asInstanceOf[Map[String,Any]]
              for (form <- tokens.keys)
              {
                var poses = tokens(form).asInstanceOf[Map[String,BigInt]]
                for (pos <- poses.keys)
                {
	  		    try
	  			{
	  			  forms(scala.Array(form,pos.stripPrefix("$"))) += poses(pos)
	  			} catch
	  			{
	  			  case e: Exception =>
	  			  {
	  			    forms(scala.Array(form,pos.stripPrefix("$"))) = poses(pos)
	  			  }
	  			}
                }
              }
            }
	  	  for (key <- forms.keys)
	  	  {
	  	    try
	  	    {
	  		  var form = key(0)
	  		  var newpos = key(1)
              var wc = posdict(newpos)
              var lemmaKey = form+":::"+wc(1)+":::"+wc(0)+":::"+corpus
	  		  context.actorSelection("/user/DBDatarouter") ! WriteToDB(List[String](form.replace("'","''"),forms(key).toString,newpos.replace("'","''"),volID,lemmaKey).mkString("\t"))
		  	} catch
			{
              case e : Exception => {}
		 	}
		  }
        }
		sender ! FinishedFile
        val rmstring = "rm "+file 
        rmstring.! 
		} catch
		{
		  case e: FileNotFoundException =>
          {
		    sender ! FinishedFile
		  }
		}
      }
      case Close =>
      {
        println(self.path.name + " has been asked to shutdown...")
        context.stop(self)
      }
    }
  }


  class DBRouterWorker extends Actor
  {
    var myfile = new PrintWriter(new File("temp.txt"))
    def receive = 
    {
	  case Printer(mymessage: String) =>
	  {
	    println(mymessage)
	  }
	  case DBInitialize(filename: String) =>
	  {
	  	this.myfile = new PrintWriter(new File(txtloc + filename))
	  }
      case WriteToDB(outputString) =>
      {
        this.myfile.write(outputString+"\n")
      }
      case DBFinished =>
      {
        this.myfile.close()
        context.actorSelection("/user/MainDispatcher") ! DBFinished
      }
    }
  }
  main()
}

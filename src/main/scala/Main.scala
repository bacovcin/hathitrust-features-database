// Import code to deal with file IO
import java.io._
import org.apache.commons.compress.compressors.bzip2._
import net.liftweb.json._
import scala.io._
import com.madhukaraphatak.sizeof.SizeEstimator

// Import code to run shell commands (rsync)
import sys.process._

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

// Get code to deal with lemmatization
import lemmatizer.MyLemmatizer

/** Downloads the Hathitrust Extracted Features Files, extracts English data,
 *  inserts data into database and lemmatizes forms*/
object Main extends App
{
  println(Runtime.getRuntime().maxMemory())
  // Database information
  if (args.length == 0) 
  {
        println("You need to provide the location of the filelist for processing!")
  }
  val batchnumber: String = args(0)
  val txtloc = "/home/hbacovci/GitHub/hathitrust-features-database/outputs/"
  val url = "jdbc:mysql://localhost/hathitrust?rewriteBatchedStatements=true"
  val username = "hathitrust"
  val password = "hathitrust"

  // Batch size for database queries
  val dataBatchSize = args(1).toInt

  // Number of workers
  val numDataWorkers = args(2).toInt // Workers who read in files and insert data
  val numLemmaWorkers = args(3).toInt // Workers who perform lemmatization


  // Messages for the Akka workers to send to one another
  sealed trait AkkaMessage
  case class StartMain(dataBatchSize: Int, numDataWorkers: Int, numLemmaWorkers: Int) extends AkkaMessage
  case class StartLemma(numLemmaWorkers: Int) extends AkkaMessage
  case class DBInitialize(filename: String) extends AkkaMessage
  case class Lemmatise(lemmaKeys: List[String]) extends AkkaMessage 
  case class LemmaKey(lemmaKey: String) extends AkkaMessage
  case class GoodLemmaKey(lemmaKey: String,outputList: String) extends AkkaMessage
  case class Process(filename: String) extends AkkaMessage 
  case class Download(filenames: List[String]) extends AkkaMessage 
  case class WriteToDB(outputString: String) extends AkkaMessage
  case class LogMessage(processname: String,workerid:String, action: String, timestamp: String) extends AkkaMessage
  case object Initialize extends AkkaMessage
  case object NeedWork extends AkkaMessage
  case object Close extends AkkaMessage
  case object LemmaDone extends AkkaMessage
  case object DataDone extends AkkaMessage
  case object Finished extends AkkaMessage
  case object DBFinished extends AkkaMessage

  /** Main code for the programme */
  def main() : Unit = 
  {
    val system = ActorSystem("AkkaSystem")
    val master = system.actorOf(Props[MainDispatcher],name="MainDispatcher")

    // Start the main Akka actor
    master ! StartMain(dataBatchSize,
                   numDataWorkers,
                   numLemmaWorkers)
  }
  
  /** Rsync all of the files in a filelist*/
  def downloadNewBatch(batch: List[String]) : Unit =
  {
    val timestamp = System.currentTimeMillis().toString
    val samplename = "sample"+timestamp+".txt"
    val samplefile = new PrintWriter(new File(samplename))
    for (file <- batch)
    {
      samplefile.write(file+'\n') 
    }
    samplefile.close
    val syncstring = "rsync -av --no-relative --files-from "+samplename+" data.analytics.hathitrust.org::features/ ."
    syncstring.!
        
    val rmstring = "rm "+samplename 
    rmstring.!
  }

  /** Downloads a batch of files from the Hathitrust server */
  class Downloader extends Actor
  {
    /** Perform task for each message */
    def receive = 
    {
      case Download(filenames) =>
      {
//        context.actorSelection("/user/Logger") ! LogMessage(self.path.name,"0","downloadStart",System.currentTimeMillis().toString)
        downloadNewBatch(filenames)
//        context.actorSelection("/user/Logger") ! LogMessage(self.path.name,"0","downloadFinish",System.currentTimeMillis().toString)
      }
    }
  }
  /** Writes out a log code to a log file */
  class Logger extends Actor
  {
    val logfile = new PrintWriter(new File("log.csv"))
    logfile.write("processname,workerid,action,timestamp\n")
    /** Perform task for each message */
    def receive = 
    {
      case LogMessage(processname: String,workerid: String, action: String, timestamp: String) =>
      {
        logfile.write(processname+","+workerid+","+action+","+timestamp+"\n")
      }
      case Finished =>
      {
        logfile.close
      }
    }
  }

  /** Downloads files and distributes them to data workers  
   *
   *  Gets the files in batches, sends the files individually to data workers,
   *  once all the files have been processed tell the lemma distributer, once
   *  all the lemmatisation is done, normalise the database, apply indexes, and
   *  quit
   *  */
  class MainDispatcher extends Actor {
    var databatches: List[List[String]] = List[List[String]]()
    var curbatch: List[String] = List[String]()
    var nextbatch: List[String] = List[String]()
    var dataworkers: List[Object] = List[Object]()
    var numWorkers: Int = 0
    val lemmadispatcher = context.system.actorOf(Props[LemmaDispatcher],name="LemmaDispatcher")
    val downloader = context.system.actorOf(Props[Downloader],name="Downloader")
    val dbroutermeta = context.system.actorOf(Props[DBRouterWorker],name="DBMetarouter")
	dbroutermeta ! DBInitialize("metadata/"+batchnumber+".txt")
    val dbrouterlemma = context.system.actorOf(Props[DBRouterWorker],name="DBLemmarouter")
	dbrouterlemma ! DBInitialize("lemmata/"+batchnumber+".txt")
    val dbrouterdata = context.system.actorOf(Props[DBRouterWorker],name="DBDatarouter")
	dbrouterdata ! DBInitialize("tokens/"+batchnumber+".txt")
//    val logger = context.system.actorOf(Props[Logger],name="Logger")
    var finishedWorkers: Int = 0
    var dbClosed: Int = 0
    var dbfinished = 0
    var notLastBatch: Boolean = true

    /** Open a db connection, run code, and then close the connection */
    def workOnDatabase[A](url:String, username:String, password:String, workCode: Connection => A) : A = 
    {
      val driver = "com.mysql.jdbc.Driver"
      val connection: Connection = DriverManager.getConnection(url, username, password)
      val output = workCode(connection)
      connection.close()
      output
    }

    /** Normalise the database*/
    def normalizeCorpus()(db: Connection): Unit =
    {
//      context.actorSelection("/user/Logger") ! LogMessage("normalizer","0","startingNormalization",System.currentTimeMillis().toString)
	  println("Reading in data from text files...")
      val readTableStmt1: Statement = db.createStatement()
      readTableStmt1.executeUpdate("LOAD DATA LOCAL INFILE '"+txtloc+"tokens.txt' INTO TABLE tokens")
      //context.actorSelection("/user/Logger") ! LogMessage("normalizer","0","loadedtokens",System.currentTimeMillis().toString)
      val readTableStmt2: Statement = db.createStatement()
      readTableStmt2.executeUpdate("LOAD DATA LOCAL INFILE '"+txtloc+"lemmata.txt' INTO TABLE lemmata")
      //context.actorSelection("/user/Logger") ! LogMessage("normalizer","0","loadedlemmata",System.currentTimeMillis().toString)
      val readTableStmt3: Statement = db.createStatement()
      readTableStmt3.executeUpdate("LOAD DATA LOCAL INFILE '"+txtloc+"metadata.txt' INTO TABLE metadata")
      //context.actorSelection("/user/Logger") ! LogMessage("normalizer","0","loadedmetadata",System.currentTimeMillis().toString)
      println("Dropping the forms table...")
      //context.actorSelection("/user/Logger") ! LogMessage("normalizer","0","dropforms",System.currentTimeMillis().toString)
      val dropTableStmt1: Statement = db.createStatement()
      dropTableStmt1.executeUpdate("DROP TABLE IF EXISTS forms")

      println("Adding the LemmaKey index on lemmata...")
      //context.actorSelection("/user/Logger") ! LogMessage("indexing","0","addlemmakeyindex",System.currentTimeMillis().toString)
      val lemmakeyIndexStmt: Statement = db.createStatement()
      lemmakeyIndexStmt.executeUpdate("CREATE INDEX lemmaKey on lemmata(lemmaKey)")

      println("Adding the POS index on forms...")
      //context.actorSelection("/user/Logger") ! LogMessage("indexing","0","addvolidindex",System.currentTimeMillis().toString)
      val volidIndexStmt: Statement = db.createStatement()
      volidIndexStmt.executeUpdate("CREATE INDEX volumeId on metadata(volumeId)")

      println("Recreating the forms table from tokens...")
      //context.actorSelection("/user/Logger") ! LogMessage("normalizer","0","creatforms",System.currentTimeMillis().toString)
      val createTableStmt: Statement = db.createStatement()
      createTableStmt.executeUpdate("CREATE TABLE forms SELECT tokens.form form, tokens.count count, tokens.POS POS, metadata.id volumeID, lemmata.id lemmaID FROM tokens INNER JOIN metadata ON tokens.volumeId = metadata.volumeId INNER JOIN lemmata ON tokens.lemmaKey = lemmata.lemmaKey;")

      println("Dropping now redundant tokens table...")
      //context.actorSelection("/user/Logger") ! LogMessage("normalizer","0","droptokens",System.currentTimeMillis().toString)
      val dropTableStmt2: Statement = db.createStatement()
      dropTableStmt2.executeUpdate("DROP TABLE IF EXISTS tokens")

      println("Dropping the LemmaKey index on lemmata...")
      //context.actorSelection("/user/Logger") ! LogMessage("indexing","0","rmlemmakeyindex",System.currentTimeMillis().toString)
      val lemmakeyDeindexStmt: Statement = db.createStatement()
      lemmakeyDeindexStmt.executeUpdate("ALTER TABLE lemmata DROP INDEX lemmaKey")

      println("Dropping the volumeId index on metadata...")
      //context.actorSelection("/user/Logger") ! LogMessage("indexing","0","rmvolidindex",System.currentTimeMillis().toString)
      val volidDeindexStmt: Statement = db.createStatement()
      volidDeindexStmt.executeUpdate("ALTER TABLE metadata DROP INDEX volumeId")

      //context.actorSelection("/user/Logger") ! LogMessage("normalizer","0","donenormalizing",System.currentTimeMillis().toString)

    }

    /** Index the database*/
    def indexCorpus()(db: Connection): Unit =
    {
      println("Adding the POS index on forms...")
      //context.actorSelection("/user/Logger") ! LogMessage("indexing","0","addposindex",System.currentTimeMillis().toString)
      val posIndexStmt: Statement = db.createStatement()
      posIndexStmt.executeUpdate("CREATE INDEX POS on forms(POS)")

      println("Adding the lemmaId index on forms...")
      //context.actorSelection("/user/Logger") ! LogMessage("indexing","0","addlemmaindex",System.currentTimeMillis().toString)
      val lemmaIndexStmt: Statement = db.createStatement()
      lemmaIndexStmt.executeUpdate("CREATE INDEX lemmaID on forms(lemmaID)")

      println("Adding the volumeID index on forms...")
      val volIndexStmt: Statement = db.createStatement()
      //context.actorSelection("/user/Logger") ! LogMessage("indexing","0","addvolindex",System.currentTimeMillis().toString)
      volIndexStmt.executeUpdate("CREATE INDEX volumeID on forms(volumeID)")
      //context.actorSelection("/user/Logger") ! LogMessage("indexing","0","doneindexing",System.currentTimeMillis().toString)
    }
    /** Initialize database 
    *
     *  Delete tables if they exist and create new tables
    *  */
    def initialiseDatabase()(db: Connection): Unit =
    {
      println("Deleting the tables...")
      val dropTableStmt1: Statement = db.createStatement()
      dropTableStmt1.executeUpdate("DROP TABLE IF EXISTS tokens")
      val dropTableStmt2: Statement = db.createStatement()
      dropTableStmt2.executeUpdate("DROP TABLE IF EXISTS lemmata")
      val dropTableStmt3: Statement = db.createStatement()
      dropTableStmt3.executeUpdate("DROP TABLE IF EXISTS metadata")
      val dropTableStmt4: Statement = db.createStatement()
      dropTableStmt4.executeUpdate("DROP TABLE IF EXISTS forms")

      println("Initialising tables...")
      val createTableStmt1: Statement = db.createStatement()
      createTableStmt1.executeUpdate("""CREATE TABLE tokens (
                     form varchar(100) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,
                      count int NOT NULL,
                      POS varchar(4) NOT NULL,
                      volumeId varchar(35) NOT NULL,
                      lemmaKey varchar(255) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL
                  )""")
      val createTableStmt2: Statement = db.createStatement()
      createTableStmt2.executeUpdate(""" CREATE TABLE lemmata (
                      id int NOT NULL UNIQUE AUTO_INCREMENT,
                      lemmaKey varchar(255) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,
                      lemma varchar(100) CHARACTER SET utf8 COLLATE utf8_bin,
                      standard varchar(100) CHARACTER SET utf8 COLLATE utf8_bin,
                      PRIMARY KEY (id)
                  )""")
      val createTableStmt3: Statement = db.createStatement()
      createTableStmt3.executeUpdate("""CREATE TABLE metadata (
                      id int NOT NULL UNIQUE AUTO_INCREMENT,
                      volumeId varchar(35) NOT NULL,
                      title varchar(1000) NOT NULL,
                      pubDate int NOT NULL,
                      pubPlace varchar(3) NOT NULL,
                      imprint varchar(255) NOT NULL,
                      genre  set('not fiction',
                                'legal case and case notes',
                                'government publication',
                                'novel',
                                'drama',
                                'bibliography',
                                'poetry',
                                'biography',
                                'catalog',
                                'fiction',
                                'statistics',
                                'dictionary',
                                'essay',
                                'index') NOT NULL,
                     names varchar(255) NOT NULL,
                     PRIMARY KEY (id)
                 )""")

      val setStmt1: Statement = db.createStatement()
    }
    /** Recursively populate the list of dataworkers with workers*/
    def createDataWorkers(numWorkers: Int, curWorkerNum: Int): Unit =
    {
      if (curWorkerNum < numWorkers)
      {
        val newWorkerNum = curWorkerNum + 1
        val newWorker = context.system.actorOf(Props[DataWorker],name="DataWorker"+newWorkerNum.toString)
        val head::tail = this.curbatch
        newWorker ! Process(head)
        this.curbatch = tail
        this.dataworkers = this.dataworkers :+ newWorker
        createDataWorkers(numWorkers, newWorkerNum)
      }
    }
    /** Create the lemma dispatcher*/
    def createLemmaDispatcher(numLemmaWorkers: Int): Unit =
    {
      context.actorSelection("/user/LemmaDispatcher") ! StartLemma(numLemmaWorkers)
    }

    /** Split the filelist into download batches*/
    def createBatches(dataBatchSize: Int) : Unit =
    {
      println("Loading file list...")
      val filelistfile = Source.fromFile("filelists/list-"+batchnumber+".txt")
      var newbatch = List[String]()
      val lines = filelistfile.getLines
      for (line <- lines)
      {
        newbatch = newbatch :+ line
        if (newbatch.length == dataBatchSize)
        {
          this.databatches = this.databatches :+ newbatch
          newbatch = List[String]()
        }
      }
      this.databatches = this.databatches :+ newbatch
      filelistfile.close
    }

    // Download a batch (and the first time download the next batch as well)
    def getNextBatch(isFirst:Boolean): Boolean =
    {
      isFirst match
      {
        case true =>
        {
          val head1::tail1 = this.databatches
          // For the first download block until rsync is done
          downloadNewBatch(head1)
          this.curbatch = head1.map(_.split("/").last)
          this.databatches = tail1
          val head2::tail2 = this.databatches
          // Asyncroneously run the rsync now
          context.actorSelection("/user/Downloader") ! Download(head2)
          this.nextbatch = head2.map(_.split("/").last)
          this.databatches = tail1
          this.databatches = tail2
          return true
        }
        case _ =>
        {
          if (this.databatches.length > 0)
          {
            this.curbatch = this.nextbatch
            val head::tail = this.databatches
            // Asyncroneously run the rsync now
            context.actorSelection("/user/Downloader") ! Download(head)
            this.nextbatch = head.map(_.split("/").last)
            this.databatches = tail
            return true
          } else
          {
            this.curbatch = this.nextbatch
            return false
          }
        }
      }
    }

    /** Perform task for each message */
    def receive = 
    {
      // Create the database structure, create the actors, and initialise the
      // batches
      case StartMain(dataBatchSize,
                 numDataWorkers, numLemmaWorkers) =>
      {
        this.numWorkers = numDataWorkers
        createLemmaDispatcher(numLemmaWorkers)
	  }
	  case StartLemma =>
	  {
        //context.actorSelection("/user/Logger") ! LogMessage(self.path.name,"0","startedSystem",System.currentTimeMillis().toString)
        workOnDatabase(url,username,password,initialiseDatabase())
        //context.actorSelection("/user/Logger") ! LogMessage(self.path.name,"0","initialisedDatabase",System.currentTimeMillis().toString)
        createBatches(dataBatchSize)
        //context.actorSelection("/user/Logger") ! LogMessage(self.path.name,"0","createdBatches",System.currentTimeMillis().toString)
        getNextBatch(true)
        //context.actorSelection("/user/Logger") ! LogMessage(self.path.name,"0","gotFirstBatch",System.currentTimeMillis().toString)
        createDataWorkers(numDataWorkers, 0)
      }
      // Learn that all the work has been sent to the database writers, wait for the database writing to be finished before normalizing and indexing
      case LemmaDone =>
      {
        //context.actorSelection("/user/Logger") ! LogMessage(self.path.name,"0","finishedLemmaBatch",System.currentTimeMillis().toString)
        println("Lemmatization batch finished...")
        this.dbroutermeta ! Finished
        this.dbrouterdata ! Finished
        this.dbrouterlemma ! Finished
      }
      // Learn that all the work is done and do finishing touches on database and quit
      case DBFinished =>
      {
        this.dbfinished += 1
        if (this.dbfinished == 3)
        {
          this.dbfinished = 0
          //context.actorSelection("/user/Logger") ! LogMessage(self.path.name,"0","finishedDBBatch",System.currentTimeMillis().toString)
          println("Database IO batch finished...")
          if (this.notLastBatch)
          {
            this.notLastBatch = getNextBatch(false)
            println("Starting a new batch...")
            //context.actorSelection("/user/Logger") ! LogMessage(self.path.name,"0","gotNewBatch",System.currentTimeMillis().toString)
            for (worker <- 1 until this.numWorkers+1)
            {
			  try
			  {
                var head::tail = this.curbatch
                context.actorSelection("/user/DataWorker"+worker.toString) ! Process(head)
                this.curbatch = tail
			  } catch
			  {
			    case e: scala.MatchError =>
				{
                  context.actorSelection("/user/DataWorker"+worker.toString) ! DataDone
				}
			  }
            }
          } else if (dbClosed == 0)
          {
            this.dbfinished = 0
            for (worker <- 1 until this.numWorkers)
            {
              context.actorSelection("/user/DataWorker"+worker.toString) ! Close
            }
	        dbrouterdata ! DBFinished
	        dbrouterlemma ! DBFinished
	        dbroutermeta ! DBFinished
            dbClosed = 1
          } else
          {
            //println("Starting normalizing...")
            //workOnDatabase(url,username,password,normalizeCorpus())
            //println("Starting indexing...")
            //workOnDatabase(url,username,password,indexCorpus())
            println("Shutting down...")
            //context.actorSelection("/user/Logger") ! Finished
            context.system.terminate()
          }
        }
      }
      // Send the next file in the batch to the data actor
      case NeedWork =>
      {
        if (this.curbatch.length > 0)
        {
          val head::tail = this.curbatch
          sender ! Process(head)
          this.curbatch = tail
        } else
        {
          //context.actorSelection("/user/Logger") ! LogMessage(self.path.name,"0","finishedDataBatch",System.currentTimeMillis().toString)
          this.finishedWorkers += 1
          if (finishedWorkers == dataworkers.length)
          {
            println("Finished a batch..."+(this.databatches.length+1).toString+" batches left...")
            context.actorSelection("/user/LemmaDispatcher") ! DataDone
            this.finishedWorkers = 0
          }
        }
      }
    }
  }

  class DBRouterWorker extends Actor
  {
    var myfile = new PrintWriter(new File("temp.txt"))
    def receive = 
    {
	  case DBInitialize(filename: String) =>
	  {
	  	this.myfile = new PrintWriter(new File(txtloc + filename))
	  }
      case WriteToDB(outputString) =>
      {
        this.myfile.write(outputString+"\n")
      }
      case Finished =>
      {
        context.actorSelection("/user/MainDispatcher") ! DBFinished
      }
      case DBFinished =>
      {
        this.myfile.close()
        context.actorSelection("/user/MainDispatcher") ! DBFinished
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
	    sender ! NeedWork
	  }
      // Get a file to process
      case Process(file) =>
      {
        //context.actorSelection("/user/Logger") ! LogMessage("DataWorker",self.path.name,"processStart",System.currentTimeMillis().toString)
        val volstream = Source.fromInputStream(new BZip2CompressorInputStream(new BufferedInputStream(new FileInputStream(file)))).getLines.next
        //context.actorSelection("/user/Logger") ! LogMessage("DataWorker",self.path.name,"openedFile",System.currentTimeMillis().toString)
        val volfile = parse(volstream).values.asInstanceOf[Map[Any,Any]]
        //context.actorSelection("/user/Logger") ! LogMessage("DataWorker",self.path.name,"parsedFile",System.currentTimeMillis().toString)
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
          //context.actorSelection("/user/Logger") ! LogMessage("DataWorker",self.path.name,"getPages",System.currentTimeMillis().toString)
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
              context.actorSelection("/user/LemmaDispatcher") ! LemmaKey(lemmaKey)
			  context.actorSelection("/user/DBDatarouter") ! WriteToDB(List[String](form.replace("'","''"),forms(key).toString,newpos.replace("'","''"),volID,lemmaKey).mkString("\t"))
		  	} catch
			{
              case e : Exception => {}
		 	}
		  }
        }
        //context.actorSelection("/user/Logger") ! LogMessage("DataWorker",self.path.name,"finishedWithFile",System.currentTimeMillis().toString)
        //println(self.path.name + " finished processing "+file+"...")
        sender ! NeedWork
        val rmstring = "rm "+file 
        rmstring.! 
        //context.actorSelection("/user/Logger") ! LogMessage("DataWorker",self.path.name,"removedFile",System.currentTimeMillis().toString)
      }
      case Close =>
      {
        println(self.path.name + " has been asked to shutdown...")
        //context.actorSelection("/user/Logger") ! LogMessage("DataWorker",self.path.name,"closed",System.currentTimeMillis().toString)
        context.stop(self)
      }
    }
  }

  /** Distributes lemma keys to lemma workers for lemmatization  
   *
   *  Receives lemmakeys from data workers, checks if they are unique,
   *  sends unique lemmakeys to lemma workers for lemmatization
   *  */
  class LemmaDispatcher extends Actor 
  {
    var uniqueLemmaKeys: MMap[String,MSet[String]] = MMap[String,MSet[String]]()
    var numWorkers = 0
    var finishedWorkers: Int = 0
    var router = context.actorOf(RoundRobinPool(0).props(Props[LemmaWorker]), "dummyrouter")

    def receive = 
    {
      // Create the lemma workers
      case StartLemma(numWorkers) =>
      {
        this.numWorkers = numWorkers
        this.router = context.actorOf(RoundRobinPool(numWorkers).props(Props[LemmaWorker]), "router")
        this.router ! Broadcast(Initialize)
      }
      // Send lemma keys on to the workers after checking for uniqueness
      case LemmaKey(lemmaKey) =>
      {
        //context.actorSelection("/user/Logger") ! LogMessage(self.path.name,"0","gotLemmaKey",System.currentTimeMillis().toString)
        var myform = lemmaKey.split(":::")(0)
        if (!(uniqueLemmaKeys contains myform))
        {
          this.router ! LemmaKey(lemmaKey)
        } else
        {
          if (!(uniqueLemmaKeys(myform) contains lemmaKey))
          {
            this.router ! LemmaKey(lemmaKey)
          }
        }
        //context.actorSelection("/user/Logger") ! LogMessage(self.path.name,"0","finishedLemmaKey",System.currentTimeMillis().toString)
      }
      // Only add actual lemmas to the map (ignore errata since they are likely to be hapax)
      case GoodLemmaKey(lemmaKey,outputString) =>
      {
        //context.actorSelection("/user/Logger") ! LogMessage(self.path.name,"0","gotGoodLemmaKey",System.currentTimeMillis().toString)
        var myform = lemmaKey.split(":::")(0)
        if (!(uniqueLemmaKeys contains myform))
        {
          context.actorSelection("/user/DBLemmarouter") ! WriteToDB(outputString)
		  uniqueLemmaKeys(myform) = MSet[String](lemmaKey)
        } else
        {
          if (!(uniqueLemmaKeys(myform) contains lemmaKey))
          {
            context.actorSelection("/user/DBLemmarouter") ! WriteToDB(outputString)
			uniqueLemmaKeys(myform) add lemmaKey
          }
        }
        //context.actorSelection("/user/Logger") ! LogMessage(self.path.name,"0","finishedGoodLemmaKey",System.currentTimeMillis().toString)
      }
      // Tell the lemma workers to ignore batch size, since they won't get more data
      case DataDone =>
      {
        println("Set Size: " + (SizeEstimator.estimate(this.uniqueLemmaKeys).toFloat / 1073741824.0).toString + "GB")
        this.router ! Broadcast(Finished) 
      }
	  case Finished =>
	  {
        finishedWorkers += 1
        if (finishedWorkers == this.numWorkers)
        {
          context.actorSelection("/user/MainDispatcher") ! StartLemma
          finishedWorkers = 0
        }
	  
	  }
      // Collect workers finishing to send final message back to the main dispatcher
      case LemmaDone =>
      {
        finishedWorkers += 1
        if (finishedWorkers == this.numWorkers)
        {
          context.actorSelection("/user/MainDispatcher") ! LemmaDone
          finishedWorkers = 0
        }
      }
    }
  }

  /** Receives lemmakeys from LemmaDispatcher, performs lemmatisation,
   *  inserts new lemmadata in database*/
  class LemmaWorker extends Actor {
    var aLemmatizer = new MyLemmatizer()

    def processLemma(lemmaKey: String, aLemmatizer: MyLemmatizer): List[String] = 
    {
      // Break the lemmaKey into the form, major word class,
      // lemma word class, and corpus
      val components = lemmaKey.split(":::")

      // Use corpus appropriate lemmatizer
      val myoutput = components(3) match
      {
        case "eme" => aLemmatizer.emeLemmatise(components)
        case "ece" => aLemmatizer.eceLemmatise(components)
        case "ncf" => aLemmatizer.ncfLemmatise(components)
        case _ => null
      }
      myoutput match
      {
        case null => List[String](null)
        case _ => 
        {
          myoutput.toList :+ lemmaKey
        }
      }
    }

    def receive = 
    {
      case Initialize =>
      {
        this.aLemmatizer.initialise()
		sender ! Finished
      }
      case Finished =>
      {
        println(self.path.name+" was told to finish processing...")
        //context.actorSelection("/user/Logger") ! LogMessage("LemmaWorker",self.path.name,"sentBatch",System.currentTimeMillis().toString)
        sender ! LemmaDone
      }
      case LemmaKey(lemmaKey) =>
      {
        //context.actorSelection("/user/Logger") ! LogMessage("LemmaWorker",self.path.name,"processLemmaStart",System.currentTimeMillis().toString)
        var output = processLemma(lemmaKey,this.aLemmatizer)
        output(0) match
        {
          case null => {}
          case _ =>
          {
          sender ! GoodLemmaKey(lemmaKey,List[String](output(3),output(2),output(1)).mkString("\t"))
          }
        }
        //context.actorSelection("/user/Logger") ! LogMessage("LemmaWorker",self.path.name,"processLemmaFinish",System.currentTimeMillis().toString)
      }
    }
  }
  main()
}

// Import code to deal with file IO
import java.io._
import org.apache.commons.compress.compressors.bzip2._
import net.liftweb.json._
import scala.io._

// Import code to run shell commands (rsync)
import sys.process._

// Import mutable sets for tracking unique lemmakeys
import scala.collection.mutable.{Set => Set,Map => MMap}

// Import Try code to capture exceptions
import scala.util.{Try, Success, Failure}

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
  // Database information
  val url = "jdbc:mysql://localhost/hathitrust?rewriteBatchedStatements=true"
  val username = "hathitrust"
  val password = "hathitrust"

  // Batch size for database queries
  val dataBatchSize = 20
  val lemmaBatchSize = 2500

  // Number of workers
  val numDataWorkers = 2 // Workers who read in files and insert data
  val numLemmaWorkers = 4 // Workers who perform lemmatization


  // Messages for the Akka workers to send to one another
  sealed trait AkkaMessage
  case class StartMain(dataBatchSize: Int, lemmaBatchSize: Int, numDataWorkers: Int, numLemmaWorkers: Int) extends AkkaMessage
  case class StartLemma(lemmaBatchSize: Int, numLemmaWorkers: Int) extends AkkaMessage
  case class Initialize(batchSize: Int) extends AkkaMessage
  case class Lemmatise(lemmaKeys: List[String]) extends AkkaMessage 
  case class LemmaKey(lemmaKey: String) extends AkkaMessage
  case class LemmaKeys(lemmaKeys: Set[String]) extends AkkaMessage
  case class Process(filename: String) extends AkkaMessage 
  case class Download(filenames: List[String]) extends AkkaMessage 
  case class WriteToDB(queryString: String, queries: List[List[Any]], types: List[String]) extends AkkaMessage
  case class LogMessage(processname: String,workerid:String, action: String, timestamp: String) extends AkkaMessage
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
                   lemmaBatchSize,
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
        context.actorSelection("/user/Logger") ! LogMessage(self.path.name,"0","downloadStart",System.currentTimeMillis().toString)
        downloadNewBatch(filenames)
        context.actorSelection("/user/Logger") ! LogMessage(self.path.name,"0","downloadFinish",System.currentTimeMillis().toString)
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
    val dbrouterlemma = context.system.actorOf(Props[DBRouterWorker],name="DBLemmarouter")
    val dbrouterdata = context.system.actorOf(Props[DBRouterWorker],name="DBDatarouter")
    val logger = context.system.actorOf(Props[Logger],name="Logger")
    var finishedWorkers: Int = 0
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
      println("Dropping the forms table...")
      context.actorSelection("/user/Logger") ! LogMessage("normalizer","0","dropforms",System.currentTimeMillis().toString)
      val dropTableStmt1: Statement = db.createStatement()
      dropTableStmt1.executeUpdate("DROP TABLE IF EXISTS forms")

      println("Adding the LemmaKey index on lemmata...")
      context.actorSelection("/user/Logger") ! LogMessage("indexing","0","addlemmakeyindex",System.currentTimeMillis().toString)
      val lemmakeyIndexStmt: Statement = db.createStatement()
      lemmakeyIndexStmt.executeUpdate("CREATE INDEX lemmaKey on lemmata(lemmaKey)")

      println("Adding the POS index on forms...")
      context.actorSelection("/user/Logger") ! LogMessage("indexing","0","addvolidindex",System.currentTimeMillis().toString)
      val volidIndexStmt: Statement = db.createStatement()
      volidIndexStmt.executeUpdate("CREATE INDEX volumeId on metadata(volumeId)")

      println("Recreating the forms table from tokens...")
      context.actorSelection("/user/Logger") ! LogMessage("normalizer","0","creatforms",System.currentTimeMillis().toString)
      val createTableStmt: Statement = db.createStatement()
      createTableStmt.executeUpdate("CREATE TABLE forms SELECT tokens.form form, tokens.count count, tokens.POS POS, tokens.pageNum pageNum, metadata.id volumeID, lemmata.id lemmaID FROM tokens INNER JOIN metadata ON tokens.volumeId = metadata.volumeId INNER JOIN lemmata ON tokens.lemmaKey = lemmata.lemmaKey;")

      println("Dropping now redundant tokens table...")
      context.actorSelection("/user/Logger") ! LogMessage("normalizer","0","droptokens",System.currentTimeMillis().toString)
      val dropTableStmt2: Statement = db.createStatement()
      dropTableStmt2.executeUpdate("DROP TABLE IF EXISTS tokens")

      println("Dropping the LemmaKey index on lemmata...")
      context.actorSelection("/user/Logger") ! LogMessage("indexing","0","rmlemmakeyindex",System.currentTimeMillis().toString)
      val lemmakeyDeindexStmt: Statement = db.createStatement()
      lemmakeyDeindexStmt.executeUpdate("ALTER TABLE lemmata DROP INDEX lemmaKey")

      println("Dropping the volumeId index on metadata...")
      context.actorSelection("/user/Logger") ! LogMessage("indexing","0","rmvolidindex",System.currentTimeMillis().toString)
      val volidDeindexStmt: Statement = db.createStatement()
      volidDeindexStmt.executeUpdate("ALTER TABLE metadata DROP INDEX volumeId")

      context.actorSelection("/user/Logger") ! LogMessage("normalizer","0","donenormalizing",System.currentTimeMillis().toString)

    }

    /** Index the database*/
    def indexCorpus()(db: Connection): Unit =
    {
      println("Adding the POS index on forms...")
      context.actorSelection("/user/Logger") ! LogMessage("indexing","0","addposindex",System.currentTimeMillis().toString)
      val posIndexStmt: Statement = db.createStatement()
      posIndexStmt.executeUpdate("CREATE INDEX POS on forms(POS)")

      println("Adding the lemmaId index on forms...")
      context.actorSelection("/user/Logger") ! LogMessage("indexing","0","addlemmaindex",System.currentTimeMillis().toString)
      val lemmaIndexStmt: Statement = db.createStatement()
      lemmaIndexStmt.executeUpdate("CREATE INDEX lemmaID on forms(lemmaID)")

      println("Adding the volumeID index on forms...")
      val volIndexStmt: Statement = db.createStatement()
      context.actorSelection("/user/Logger") ! LogMessage("indexing","0","addvolindex",System.currentTimeMillis().toString)
      volIndexStmt.executeUpdate("CREATE INDEX volumeID on forms(volumeID)")
      context.actorSelection("/user/Logger") ! LogMessage("indexing","0","doneindexing",System.currentTimeMillis().toString)
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
                      pageNum int NOT NULL,
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
      setStmt1.executeUpdate("SET GLOBAL max_allowed_packet=1073741824")

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
    def createLemmaDispatcher(numLemmaWorkers: Int, lemmaBatchSize: Int): Unit =
    {
      context.actorSelection("/user/LemmaDispatcher") ! StartLemma(lemmaBatchSize, numLemmaWorkers)
    }

    /** Split the filelist into download batches*/
    def createBatches(dataBatchSize: Int) : Unit =
    {
      println("Loading file list...")
      val filelist = Source.fromFile("htrc-ef-all-files.txt")
      var newbatch = List[String]()
      val lines = filelist.getLines
      for (i <- 1 until 100)
      {
        var line = lines.next
        newbatch = newbatch :+ line
        if (newbatch.length == dataBatchSize)
        {
          this.databatches = this.databatches :+ newbatch
          newbatch = List[String]()
        }
      }
      filelist.close
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
      case StartMain(dataBatchSize, lemmaBatchSize,
                 numDataWorkers, numLemmaWorkers) =>
      {
        this.numWorkers = numDataWorkers
        createLemmaDispatcher(numLemmaWorkers, lemmaBatchSize)
        context.actorSelection("/user/Logger") ! LogMessage(self.path.name,"0","startedSystem",System.currentTimeMillis().toString)
        workOnDatabase(url,username,password,initialiseDatabase())
        context.actorSelection("/user/Logger") ! LogMessage(self.path.name,"0","initialisedDatabase",System.currentTimeMillis().toString)
        //"rsync -azv data.analytics.hathitrust.org::features/listing/htrc-ef-all-files.txt .".!
        createBatches(dataBatchSize)
        context.actorSelection("/user/Logger") ! LogMessage(self.path.name,"0","createdBatches",System.currentTimeMillis().toString)
        getNextBatch(true)
        context.actorSelection("/user/Logger") ! LogMessage(self.path.name,"0","gotFirstBatch",System.currentTimeMillis().toString)
        createDataWorkers(numDataWorkers, 0)
      }
      // Learn that all the work has been sent to the database writers, wait for the database writing to be finished before normalizing and indexing
      case LemmaDone =>
      {
        context.actorSelection("/user/Logger") ! LogMessage(self.path.name,"0","finishedLemmaBatch",System.currentTimeMillis().toString)
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
          context.actorSelection("/user/Logger") ! LogMessage(self.path.name,"0","finishedDBBatch",System.currentTimeMillis().toString)
          println("Database IO batch finished...")
          if (this.notLastBatch)
          {
            this.notLastBatch = getNextBatch(false)
            println("Starting a new batch...")
            context.actorSelection("/user/Logger") ! LogMessage(self.path.name,"0","gotNewBatch",System.currentTimeMillis().toString)
            for (worker <- 1 until this.numWorkers+1)
            {
              var head::tail = this.curbatch
              println(worker)
              context.actorSelection("/user/DataWorker"+worker.toString) ! Process(head)
              this.curbatch = tail
            }
          } else
          {
            for (worker <- 1 until this.numWorkers)
            {
              context.actorSelection("/user/DataWorker"+worker.toString) ! Close
            }
            println("Starting normalizing...")
            workOnDatabase(url,username,password,normalizeCorpus())
            println("Starting indexing...")
            workOnDatabase(url,username,password,indexCorpus())
            println("Shutting down...")
            context.actorSelection("/user/Logger") ! Finished
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
          context.actorSelection("/user/Logger") ! LogMessage(self.path.name,"0","finishedDataBatch",System.currentTimeMillis().toString)
          for (worker <- 1 until this.numWorkers)
          this.finishedWorkers += 1
          if (finishedWorkers == dataworkers.length)
          {
            println("Finished a batch...")
            context.actorSelection("/user/LemmaDispatcher") ! DataDone
            this.finishedWorkers = 0
          }
        }
      }
    }
  }

  class DBRouterWorker extends Actor
  {
    val writeLength = 5000
    val numDBWriters = 2
    val myrouter = context.actorOf(RoundRobinPool(numDBWriters).props(Props[DBWriter]), self.path.name+"Router")
    var finishedWorkers = 0
    var outputList = List[List[Any]]()
    var outputString = ""
    var outputTypes = List[String]()
    var outputCount = 0
    def receive = 
    {
      case WriteToDB(queryString: String, queries: List[List[Any]],types: List[String]) =>
      {
        outputString = queryString
        outputTypes = types
        context.actorSelection("/user/Logger") ! LogMessage(self.path.name,"0","queryRecieved",System.currentTimeMillis().toString)
        outputList = outputList :+ queries(0)
        outputCount += queries.length
        if (outputCount >= writeLength)
        {
          myrouter ! WriteToDB(queryString,outputList,types)
          outputList = List[List[Any]]()
          outputCount = 0
        }
        context.actorSelection("/user/Logger") ! LogMessage(self.path.name,"0","queryProcessed",System.currentTimeMillis().toString)
      }
      case Finished =>
      {
        myrouter ! WriteToDB(outputString,outputList,outputTypes)
        myrouter ! Broadcast(Finished)
      }
      case DBFinished =>
      {
        this.finishedWorkers += 1
        if (this.finishedWorkers == this.numDBWriters)
        {
          context.actorSelection("/user/MainDispatcher") ! DBFinished
          this.finishedWorkers = 0
        }
      }
    }
  }

  /** Writes out a log code to a log file */
  class DBWriter extends Actor
  {
    val driver = "com.mysql.jdbc.Driver"
    val db: Connection = DriverManager.getConnection(url, username, password)
    def receive = 
    {
      case WriteToDB(queryString: String, queries: List[List[Any]],types: List[String]) =>
      {
        context.actorSelection("/user/Logger") ! LogMessage("DBWriter",self.path.name,"queryRecieved",System.currentTimeMillis().toString)
        val queryStmt: PreparedStatement = db.prepareStatement(queryString)
        var isFirst = true
        for (query <- queries)
        {
          if (isFirst)
          {
            isFirst = false
          } else
          {
            queryStmt.addBatch()
          }
          for (i <- 0 until types.length)
          {
            if (types(i) == "string")
            {
              queryStmt.setString(i+1,query(i).asInstanceOf[String])
            } else if (types(i) == "int")
            {
              queryStmt.setInt(i+1,query(i).asInstanceOf[Int])
            } else
            {
              println("You passed an illegal type: " + types(i))
              context.system.terminate()
            }
          }
        }
        if (queries.length == 1)
        {
          queryStmt.executeUpdate()
        } else
        {
          queryStmt.executeBatch()
        }
        queryStmt.close()
        context.actorSelection("/user/Logger") ! LogMessage("DBWriter",self.path.name,"queryCompleted",System.currentTimeMillis().toString)
      }
      case Finished =>
      {
        context.actorSelection("/user/Logger") ! LogMessage("DBWriter",self.path.name,"shutdown",System.currentTimeMillis().toString)
        sender ! DBFinished
      }
    }
  }

  /** Process files, insert metadata and token level data, and send lemmaKeys to 
   *  lemma dispatcher*/
  class DataWorker extends Actor
  {
    var uniqueCheckBatchSize = 100
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
      // Get a file to process
      case Process(file) =>
      {
        context.actorSelection("/user/Logger") ! LogMessage("DataWorker",self.path.name,"processStart",System.currentTimeMillis().toString)
        val volstream = Source.fromInputStream(new BZip2CompressorInputStream(new BufferedInputStream(new FileInputStream(file)))).getLines.next
        context.actorSelection("/user/Logger") ! LogMessage("DataWorker",self.path.name,"openedFile",System.currentTimeMillis().toString)
        val volfile = parse(volstream).values.asInstanceOf[Map[Any,Any]]
        context.actorSelection("/user/Logger") ! LogMessage("DataWorker",self.path.name,"parsedFile",System.currentTimeMillis().toString)
        val curmeta = volfile("metadata").asInstanceOf[Map[Any,Any]]
        // Only use English data
        if (curmeta("language").asInstanceOf[String] == "eng")
        {
          val volID = curmeta("volumeIdentifier").asInstanceOf[String].replace("'","''")
          val year = curmeta("pubDate").asInstanceOf[String].toInt
          // Deal with metadata
          val insertMetaString: String = "INSERT INTO metadata (volumeId,title,pubDate,pubPlace,imprint,genre,names) VALUES (?,?,?,?,?,(?),?)"
          context.actorSelection("/user/DBMetarouter") ! WriteToDB(insertMetaString,List[List[Any]](List[Any](volID,curmeta("title").asInstanceOf[String].replace("'","''"),year,curmeta("pubPlace").asInstanceOf[String],curmeta("imprint").asInstanceOf[String].replace("'","''"),curmeta("genre").asInstanceOf[List[String]].mkString(","),curmeta("names").asInstanceOf[List[String]].mkString(";").replace("'","''"))),List[String]("string","string","int","string","string","string","string"))
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
          val insertTokenString: String = "INSERT INTO tokens (form,count,POS,volumeId,pageNum,lemmaKey) VALUES (?,?,?,?,?,?)"
          var batchCount: Int = 0
          val pages = volfile("features").asInstanceOf[Map[Any,Any]]("pages").asInstanceOf[List[Map[Any,Any]]]
          context.actorSelection("/user/Logger") ! LogMessage("DataWorker",self.path.name,"getPages",System.currentTimeMillis().toString)
          var curUniqueLemmaKeys: Set[String] = Set[String]()
          var oldUniqueLemmaKeys: Set[String] = Set[String]()
          for (page <- pages)
          {
            var tokens = page("body").asInstanceOf[Map[Any,Any]]("tokenPosCount").asInstanceOf[Map[Any,Any]]
            for (form <- tokens.keys)
            {
              var poses = tokens(form).asInstanceOf[Map[String,BigInt]]
              for (pos <- poses.keys)
              {
                var newpos = pos.stripPrefix("$")
                try
                {
                  var wc = posdict(newpos)
                  var lemmaKey = form.asInstanceOf[String].replace("'","''")+":::"+wc(1)+":::"+wc(0)+":::"+corpus
                  if (!(oldUniqueLemmaKeys contains lemmaKey))
                  {
                    curUniqueLemmaKeys = curUniqueLemmaKeys + lemmaKey
                  }
                  var newToken = List[Any](form.asInstanceOf[String].replace("'","''"),poses(pos).toInt,newpos.replace("'","''"),volID,page("seq").asInstanceOf[String].toInt,lemmaKey)
                  context.actorSelection("/user/DBDatarouter") ! WriteToDB(insertTokenString,List[List[Any]](newToken),List[String]("string","int","string","string","int","string"))
                  batchCount = batchCount + 1
                  if (batchCount == uniqueCheckBatchSize)
                  {
                    context.actorSelection("/user/LemmaDispatcher") ! LemmaKeys(curUniqueLemmaKeys)
                    oldUniqueLemmaKeys = oldUniqueLemmaKeys union curUniqueLemmaKeys
                    curUniqueLemmaKeys = Set[String]()
                    batchCount = 0
                  }
                } catch
                {
                  case e : Exception => {}
                }
              }
            }
          }
          context.actorSelection("/user/LemmaDispatcher") ! LemmaKeys(curUniqueLemmaKeys)
        }
        context.actorSelection("/user/Logger") ! LogMessage("DataWorker",self.path.name,"finishedWithFile",System.currentTimeMillis().toString)
        println(self.path.name + " finished processing "+file+"...")
        sender ! NeedWork
        val rmstring = "rm "+file 
        rmstring.! 
        context.actorSelection("/user/Logger") ! LogMessage("DataWorker",self.path.name,"removedFile",System.currentTimeMillis().toString)
      }
      case Close =>
      {
        println(self.path.name + " has been asked to shutdown...")
        context.actorSelection("/user/Logger") ! LogMessage("DataWorker",self.path.name,"closed",System.currentTimeMillis().toString)
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
    var uniqueLemmaKeys: Set[String] = Set[String]()
    var keysNeedingLemmas: List[String] = List[String]()
    var numWorkers = 0
    var batches: List[List[String]] = List[List[String]](List[String]())
    var finishedWorkers: Int = 0
    var router = context.actorOf(RoundRobinPool(0).props(Props[LemmaWorker]), "dummyrouter")

    def receive = 
    {
      // Create the lemma workers
      case StartLemma(batchSize, numWorkers) =>
      {
        this.numWorkers = numWorkers
        this.router = context.actorOf(RoundRobinPool(numWorkers).props(Props[LemmaWorker]), "router")
        this.router ! Broadcast(Initialize(batchSize))
      }
      // Send lemma keys on to the workers after checking for uniqueness
      case LemmaKeys(lemmaKeys) =>
      {
        context.actorSelection("/user/Logger") ! LogMessage(self.path.name,"0","gotLemmaKeys",System.currentTimeMillis().toString)
        val newLemmaKeys = lemmaKeys &~ uniqueLemmaKeys 
        for (lemmaKey <- newLemmaKeys)
        {
          this.router ! LemmaKey(lemmaKey)
          uniqueLemmaKeys = uniqueLemmaKeys + lemmaKey
        }
        context.actorSelection("/user/Logger") ! LogMessage(self.path.name,"0","finishedLemmaKeys",System.currentTimeMillis().toString)
      }
      // Tell the lemma workers to ignore batch size, since they won't get more data
      case DataDone =>
      {
        this.router ! Broadcast(Finished) 
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
    val insertString: String = "INSERT lemmata (lemmaKey, lemma, standard) VALUES (?, ?, ?)"

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
      case Initialize(batchSize: Int) =>
      {
        this.aLemmatizer.initialise()
      }
      case Finished =>
      {
        println(self.path.name+" was told to finish processing...")
        context.actorSelection("/user/Logger") ! LogMessage("LemmaWorker",self.path.name,"sentBatch",System.currentTimeMillis().toString)
        sender ! LemmaDone
      }
      case LemmaKey(lemmaKey) =>
      {
        context.actorSelection("/user/Logger") ! LogMessage("LemmaWorker",self.path.name,"processLemmaStart",System.currentTimeMillis().toString)
        var output = processLemma(lemmaKey,this.aLemmatizer)
        output(0) match
        {
          case null => {}
          case _ =>
          {
            context.actorSelection("/user/DBLemmarouter") ! WriteToDB(this.insertString,List[List[Any]](List[String](output(3),output(2),output(1))),List[String]("string","string","string"))
          }
        }
        context.actorSelection("/user/Logger") ! LogMessage("LemmaWorker",self.path.name,"processLemmaFinish",System.currentTimeMillis().toString)
      }
    }
  }
  main()
}

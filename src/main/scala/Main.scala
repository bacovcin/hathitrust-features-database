// Import code to deal with file IO
import java.io._
import org.apache.commons.compress.compressors.bzip2._
import net.liftweb.json._
import scala.io._

// Import code to run shell commands (rsync)
import sys.process._

// Import mutable sets for tracking unique lemmakeys
import scala.collection.mutable.Set

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
  val url = "jdbc:mysql://localhost/hathitrust"
  val username = "hathitrust"
  val password = "hathitrust"

  // Batch size for database queries
  val dataBatchSize = 2
  val lemmaBatchSize = 1000

  // Number of workers
  val numDataWorkers = 2 // Workers who read in files and insert data
  val numLemmaWorkers = 2 // Workers who perform lemmatization


  // Messages for the Akka workers to send to one another
  sealed trait AkkaMessage
  case class StartMain(dataBatchSize: Int, lemmaBatchSize: Int, numDataWorkers: Int, numLemmaWorkers: Int) extends AkkaMessage
  case class StartLemma(lemmaBatchSize: Int, numLemmaWorkers: Int) extends AkkaMessage
  case class Initialize(batchSize: Int) extends AkkaMessage
  case class Lemmatise(lemmaKeys: List[String]) extends AkkaMessage 
  case class LemmaKey(lemmaKey: String) extends AkkaMessage
  case class Process(filename: String) extends AkkaMessage 
  case object NeedWork extends AkkaMessage
  case object Close extends AkkaMessage
  case object LemmaDone extends AkkaMessage
  case object DataDone extends AkkaMessage
  case object Finished extends AkkaMessage

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
    val lemmadispatcher = context.system.actorOf(Props[LemmaDispatcher],name="LemmaDispatcher")
    var finishedWorkers: Int = 0
    var notLastBatch: Boolean = true
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
      for (line <- lines)
      {
        newbatch = newbatch :+ line
        if (newbatch.length == dataBatchSize)
        {
          this.databatches = this.databatches :+ newbatch
          newbatch = List[String]()
        }
      }
      filelist.close
    }
    /** Download the next batch and get the list of downloaded files */
    def getListOfFiles(dir: File, extensions: List[String]): List[String] = {
      val files = dir.listFiles.filter(_.isFile).toList.filter { file =>
        extensions.exists(file.getName.endsWith(_))
      }
      for (x <- files) yield x.getName
    }

    // Download a batch (and the first time download the next batch as well)
    def getNextBatch(isFirst:Boolean): Boolean =
    {
      isFirst match
      {
        case true =>
        {
          val head1::tail1 = this.databatches
          downloadNewBatch(head1)
          this.curbatch = head1.map(_.split("/").last)
          this.databatches = tail1
          val head2::tail2 = this.databatches
          downloadNewBatch(head2)
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
            downloadNewBatch(head)
            this.nextbatch = head.map(_.split("/").last)
            this.databatches = tail
            return true
          } else
          {
            this.curbatch = getListOfFiles(new File("."),List("bz2"))
            return false
          }
        }
      }
    }

    /** Rsync all of the files in a filelist*/
    def downloadNewBatch(batch: List[String]) : Unit =
    {
      val samplefile = new PrintWriter(new File("sample.txt"))
      for (file <- batch)
      {
        println(file)
        samplefile.write(file+'\n') 
      }
      samplefile.close
      "rsync -av --no-relative --files-from sample.txt data.analytics.hathitrust.org::features/ .".!
    }

    /** Perform task for each message */
    def receive = 
    {
      // Create the database structure, create the actors, and initialise the
      // batches
      case StartMain(dataBatchSize, lemmaBatchSize,
                 numDataWorkers, numLemmaWorkers) =>
      {
        workOnDatabase(url,username,password,initialiseDatabase())
        //"rsync -azv data.analytics.hathitrust.org::features/listing/htrc-ef-all-files.txt .".!
        createBatches(dataBatchSize)
        getNextBatch(true)
        createLemmaDispatcher(numLemmaWorkers, lemmaBatchSize)
        createDataWorkers(numDataWorkers, 0)
      }
      // Learn that all the work is done and do finishing touches on database and quit
      case LemmaDone =>
      {
        println("Starting normalizing...")
        workOnDatabase(url,username,password,normalizeCorpus())
        println("Starting indexing...")
        workOnDatabase(url,username,password,indexCorpus())
        println("Shutting down...")
        context.system.terminate()
      }
      // Send the next file in the batch to the data actor
      case NeedWork =>
      {
        if (this.curbatch.length > 0)
        {
          val head::tail = this.curbatch
          sender ! Process(head)
          this.curbatch = tail
        } else if (this.notLastBatch == false)
        {
          sender ! Close
          finishedWorkers += 1
          if (finishedWorkers == dataworkers.length)
          {
            context.actorSelection("/user/LemmaDispatcher") ! DataDone
          }
        } else
        {
          this.notLastBatch = getNextBatch(false)
          val head::tail = this.curbatch
          sender ! Process(head)
          this.curbatch = tail
        }
      }
    }
  }

  /** Process files, insert metadata and token level data, and send lemmaKeys to 
   *  lemma dispatcher*/
  class DataWorker extends Actor
  {
    val driver = "com.mysql.jdbc.Driver"
    val db: Connection = DriverManager.getConnection(url, username, password)
    var dataWriteSize = 1000
    // A Map for converting POS tags into Morphadorner classes:
    val posdict: Map[String,List[String]] =
      Map(
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
        val volstream = Source.fromInputStream(new BZip2CompressorInputStream(new BufferedInputStream(new FileInputStream(file)))).getLines.next
        val volfile = parse(volstream).values.asInstanceOf[Map[Any,Any]]
        val curmeta = volfile("metadata").asInstanceOf[Map[Any,Any]]
        // Only use English data
        if (curmeta("language").asInstanceOf[String] == "eng")
        {
          val volID = curmeta("volumeIdentifier").asInstanceOf[String].replace("'","''")
          val year = curmeta("pubDate").asInstanceOf[String].toInt
          // Deal with metadata
          val insertMetaString: String = "INSERT INTO metadata (volumeId,title,pubDate,pubPlace,imprint,genre,names) VALUES (?,?,?,?,?,(?),?)"
          val insertMetaStmt: PreparedStatement = db.prepareStatement(insertMetaString)
          insertMetaStmt.setString(1,volID)
          insertMetaStmt.setString(2,curmeta("title").asInstanceOf[String].replace("'","''"))
          insertMetaStmt.setInt(3,year)
          insertMetaStmt.setString(4,curmeta("pubPlace").asInstanceOf[String])
          insertMetaStmt.setString(5,curmeta("imprint").asInstanceOf[String].replace("'","''"))
          insertMetaStmt.setString(6,curmeta("genre").asInstanceOf[List[String]].mkString(","))
          insertMetaStmt.setString(7,curmeta("names").asInstanceOf[List[String]].mkString(";").replace("'","''"))
          insertMetaStmt.executeUpdate()
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
          var insertTokenStmt: PreparedStatement = db.prepareStatement(insertTokenString)
          var batchCount: Int = 0
          val pages = volfile("features").asInstanceOf[Map[Any,Any]]("pages").asInstanceOf[List[Map[Any,Any]]]
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
                  insertTokenStmt.setString(1,form.asInstanceOf[String].replace("'","''"))
                  insertTokenStmt.setInt(2,poses(pos).toInt)
                  insertTokenStmt.setString(3,newpos.replace("'","''"))
                  insertTokenStmt.setString(4,volID)
                  insertTokenStmt.setInt(5,page("seq").asInstanceOf[String].toInt)
                  insertTokenStmt.setString(6,lemmaKey)
                  context.actorSelection("/user/LemmaDispatcher") ! LemmaKey(lemmaKey)
                  batchCount = batchCount + 1
                  if (batchCount == dataWriteSize)
                  {
                    insertTokenStmt.executeBatch()
                    println(self.path.name + " has executed a batch...")
                    insertTokenStmt = db.prepareStatement(insertTokenString)
                    batchCount = 0
                  } else
                  {
                    insertTokenStmt.addBatch()
                  }
                } catch
                {
                  case e : Exception => {}
                }
              }
            }
          }
          insertTokenStmt.executeBatch()
          println(self.path.name + " has executed a batch...")
        }
        sender ! NeedWork
        val rmstring = "rm "+file
        rmstring.!
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
    var router = context.actorOf(SmallestMailboxPool(0).props(Props[LemmaWorker]), "dummyrouter")

    def receive = 
    {
      // Create the lemma workers
      case StartLemma(batchSize, numWorkers) =>
      {
        this.numWorkers = numWorkers
        this.router = context.actorOf(SmallestMailboxPool(numWorkers).props(Props[LemmaWorker]), "router")
        this.router ! Broadcast(Initialize(batchSize))
      }
      // Send lemma keys on to the workers after checking for uniqueness
      case LemmaKey(lemmaKey) =>
      {
        if (!(uniqueLemmaKeys contains lemmaKey))
        {
          uniqueLemmaKeys = uniqueLemmaKeys + lemmaKey
          this.router ! LemmaKey(lemmaKey)
        }
      }
      // Tell the lemma workers to ignore batch size, since they won't get more data
      case DataDone =>
      {
        this.router ! Broadcast(Close) 
      }
      // Collect workers finishing to send final message back to the main dispatcher
      case LemmaDone =>
      {
        finishedWorkers += 1
        if (finishedWorkers == this.numWorkers)
        {
          context.actorSelection("/user/MainDispatcher") ! LemmaDone
        }
      }
    }
  }

  /** Receives lemmakeys from LemmaDispatcher, performs lemmatisation,
   *  inserts new lemmadata in database*/
  class LemmaWorker extends Actor {
    val driver = "com.mysql.jdbc.Driver"
    val db: Connection = DriverManager.getConnection(url, username, password)
    var lemmaCount: Int = 0
    var lemmata: List[List[String]] = List[List[String]]()
    var batchSize = 0
    var aLemmatizer = new MyLemmatizer()

    def writeToDatabase(outputs: List[List[String]]): Unit =
    {
      val insertString: String = "INSERT lemmata (lemmaKey, lemma, standard) VALUES (?, ?, ?)"
      val insertStmt: PreparedStatement = db.prepareStatement(insertString)

      for (output <- outputs)
      {
        // If the lemmatizer gives a null result,
        // that means that the form is either (a)
        // not English or (b) an OCR error or
        // (c) a really rare word (so no update)
        //
        // Otherwise we want to insert the lemmata data into the lemmata table
        output(0) match
        {
          case null => {}
          case _ =>
          {
            insertStmt.setString(1,output(3))
            insertStmt.setString(2,output(2))
            insertStmt.setString(3,output(1))
            insertStmt.addBatch()
          }
        }
      }
      // Commit the batches to the database
      val insertResults = insertStmt.executeBatch()
      println(self.path.name + " has executed a batch...")
    }
      
    def processLemma(lemmaKey: String, aLemmatizer: MyLemmatizer): List[String] = 
    {
      // Break the lemmaKey into the form, major word class,
      // lemma word class, and corpus
      val components = lemmaKey.split(":::")

      // Use corpus appropriate lemmatizer
      val output = components(3) match
      {
        case "eme" => aLemmatizer.emeLemmatise(components)
        case "ece" => aLemmatizer.eceLemmatise(components)
        case "ncf" => aLemmatizer.ncfLemmatise(components)
        case _ => null
      }
      output match
      {
        case null => List[String](null)
        case _ => 
        {
          output.toList :+ lemmaKey
        }
      }
    }

    def receive = 
    {
      case Initialize(batchSize: Int) =>
      {
        this.aLemmatizer.initialise()
        this.batchSize = batchSize
      }
      case Close =>
      {
        println(self.path.name+" has " + lemmata.length.toString + " number of lemmata...")
        println(self.path.name+" was told to close...")
        writeToDatabase(lemmata)
        sender ! LemmaDone
        db.close()
        context.stop(self)
      }
      case LemmaKey(lemmaKey) =>
      {
        lemmata = lemmata :+ processLemma(lemmaKey,this.aLemmatizer)
        if (lemmata.length == this.batchSize)
        {
          writeToDatabase(lemmata)
          lemmata = List[List[String]]()
        }
      }
    }
  }

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
    val dropTableStmt1: Statement = db.createStatement()
    dropTableStmt1.executeUpdate("DROP TABLE IF EXISTS forms")

    println("Recreating the forms table from tokens...")
    val createTableStmt: Statement = db.createStatement()
    createTableStmt.executeUpdate("CREATE TABLE forms SELECT tokens.form form, tokens.count count, tokens.POS POS, tokens.pageNum pageNum, metadata.id volumeID, lemmata.id lemmaID FROM tokens INNER JOIN metadata ON tokens.volumeId = metadata.volumeId INNER JOIN lemmata ON tokens.lemmaKey = lemmata.lemmaKey;")

    println("Dropping now redundant tokens table...")
    val dropTableStmt2: Statement = db.createStatement()
    dropTableStmt2.executeUpdate("DROP TABLE IF EXISTS tokens")

  }

  /** Index the database*/
  def indexCorpus()(db: Connection): Unit =
  {
    println("Adding the POS index on forms...")
    val posIndexStmt: Statement = db.createStatement()
    posIndexStmt.executeUpdate("CREATE INDEX POS on forms(POS)")

    println("Adding the lemmaId index on forms...")
    val lemmaIndexStmt: Statement = db.createStatement()
    lemmaIndexStmt.executeUpdate("CREATE INDEX lemmaID on forms(lemmaID)")

    println("Adding the volumeID index on forms...")
    val volIndexStmt: Statement = db.createStatement()
    volIndexStmt.executeUpdate("CREATE INDEX volumeID on forms(volumeID)")
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
                    form varchar(255) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,
                    count int NOT NULL,
                    POS varchar(4) NOT NULL,
                    volumeId varchar(255) NOT NULL,
                    pageNum int NOT NULL,
                    lemmaKey varchar(255) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL
                )""")
    val createTableStmt2: Statement = db.createStatement()
    createTableStmt2.executeUpdate(""" CREATE TABLE lemmata (
                    id int NOT NULL UNIQUE AUTO_INCREMENT,
                    lemmaKey varchar(100) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,
                    lemma varchar(255) CHARACTER SET utf8 COLLATE utf8_bin,
                    standard varchar(255) CHARACTER SET utf8 COLLATE utf8_bin,
                    PRIMARY KEY (id)
                )""")
    val createTableStmt3: Statement = db.createStatement()
    createTableStmt3.executeUpdate("""CREATE TABLE metadata (
                    id int NOT NULL UNIQUE AUTO_INCREMENT,
                    volumeId varchar(100) NOT NULL,
                    title varchar(400) NOT NULL,
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
  main()
}

// Import Try code to capture exceptions
import scala.util.{Try, Success, Failure}

// Import the JDBC libraries
import java.sql._

// Import the Akka code for multiprocessing
import akka.actor._
import akka.actor.ActorSystem._

// Get code to deal with lemmatization
import lemmatizer.MyLemmatizer

// Here is the main object
object Main extends App
{
  // Database information
  val url = "jdbc:mysql://localhost/hathitrust"
  val username = "hathitrust"
  val password = "plotkin"

  // Batch size for database queries
  val batchSize = 1000

  // Number of workers
  val numWorkers = 6

  sealed trait LemmaMessage
  case class Start(numLemmata: Int, batchSize: Int, numWorkers: Int) extends LemmaMessage
  case class Lemmatise(min:Int, max:Int) extends LemmaMessage
  case object NeedWork extends LemmaMessage
  case object Initialize extends LemmaMessage
  case object Close extends LemmaMessage

  class LemmaDispatcher extends Actor {
    var batches: List[List[Int]] = List[List[Int]](List[Int]())
    var workers: List[Object] = List[Object]()
    var finishedWorkers: Int = 0
    def getBatches(numLemmata: Int, batchSize: Int): List[List[Int]] =
    {
      def getBatchesRecursive(numLemmata: Int, batchSize: Int, curBatches: List[List[Int]]): List[List[Int]] =
      {
        if (curBatches.last.last > numLemmata + 1)
        {
          curBatches
        } else
        {
          val lastBatchNum = curBatches.last.last
          getBatchesRecursive(numLemmata, batchSize, curBatches :+ List[Int](lastBatchNum,lastBatchNum+batchSize))
        }
      }
      getBatchesRecursive(numLemmata,batchSize,List[List[Int]](List[Int](0,batchSize)))
    }
    def createWorkers(numWorkers: Int, curWorkerNum: Int): Unit =
    {
      if (curWorkerNum < numWorkers)
      {
        val newWorkerNum = curWorkerNum + 1
        val newWorker = context.system.actorOf(Props[LemmaWorker],name="Worker"+newWorkerNum.toString)
        val head::tail = this.batches
        newWorker ! Initialize
        newWorker ! Lemmatise(head(0),head(1))
        this.batches = tail
        workers = workers :+ newWorker
        createWorkers(numWorkers, newWorkerNum)
      }
    }
    def receive = 
    {
      case Start(numLemmata, batchSize, numWorkers) =>
      {
        this.batches = getBatches(numLemmata, batchSize)
        createWorkers(numWorkers, 0)
      }
      case NeedWork =>
      {
        if (this.batches.length > 0)
        {
          val head::tail = this.batches
          sender ! Lemmatise(head(0),head(1))
          this.batches = tail
        } else
        {
          sender ! Close
          finishedWorkers += 1
          if (finishedWorkers == numWorkers)
          {
            println("Starting indexing...")
            workOnDatabase(url,username,password,indexCorpus())
            println("Starting normalizing...")
            workOnDatabase(url,username,password,normalizeCorpus())
            println("Shutting down...")
            context.system.terminate()
          }
        }
      }
    }
  }

  class LemmaWorker extends Actor {
 
    val driver = "com.mysql.jdbc.Driver"
    val db: Connection = DriverManager.getConnection(url, username, password)
    val aLemmatizer: MyLemmatizer = new MyLemmatizer()

    def parseLemmata(lemmata: ResultSet): Unit = 
    {
      def parseLemmataRecursive(lemmata: ResultSet, lemmaOutputs: List[List[List[String]]], aLemmatizer: MyLemmatizer): Unit =
      {
        def processLemma(lemmaKey: String, id: String, aLemmatizer: MyLemmatizer): List[List[String]] = 
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
          }
          output match
          {
            case null => List[List[String]](List[String](id),
                                          List[String](null, lemmaKey))
            case _ => List[List[String]](List[String](id),output.toList) 
          }
        }
        def writeToDatabase(outputs: List[List[List[String]]]): Unit =
        {
          println(self.path.name+" is starting a new batch...")
          val updateString: String = "UPDATE lemmata SET lemma = ?, standard = ? WHERE id = ?"
          val deleteTokensString: String = "DELETE FROM tokens where lemmaKey = ?"
          val deleteLemmataString: String = "DELETE FROM lemmata where id = ?"
        
          val updateStmt: PreparedStatement = db.prepareStatement(updateString)
          val deleteTokensStmt: PreparedStatement = db.prepareStatement(deleteTokensString)
          val deleteLemmataStmt: PreparedStatement = db.prepareStatement(deleteLemmataString)

          for (output <- outputs)
          {
            // If the lemmatizer gives a null result,
            // that means that the form is either (a)
            // not English or (b) an OCR error or
            // (c) a really rare word
            //
            // In any case we want to delete all instances
            // of that form from the tokens
            //
            // Otherwise we want to update the lemmata data into the lemmata table
            output(1)(0) match
            {
              case null => 
              {
                deleteTokensStmt.setString(1,output(1)(1))
                deleteTokensStmt.addBatch()
                deleteLemmataStmt.setInt(1,output(0)(0).toInt)
                deleteLemmataStmt.addBatch()
              }
              case _ =>
              {
                updateStmt.setString(1,output(1)(2))
                updateStmt.setString(2,output(1)(1))
                updateStmt.setInt(3,output(0)(0).toInt)
                updateStmt.addBatch()
              }
            }
          }

          // Commit the batches to the database
          val deleteTokensResults = deleteTokensStmt.executeBatch()
          val deleteLemmataResults = deleteLemmataStmt.executeBatch()
          val updateResults = updateStmt.executeBatch()
        }
      
        // Get the next lemma
        val nextLemma = lemmata.next()
        // Only write out to the database after parsing the entire batch
        if (!nextLemma)
        {
          writeToDatabase(lemmaOutputs)
        }
        else
        {
          val newOutput: List[List[String]] = processLemma(lemmata.getString("lemmaKey"), lemmata.getInt("id").toString, aLemmatizer)
          val newOutputs: List[List[List[String]]] = lemmaOutputs :+ newOutput
          parseLemmataRecursive(lemmata, newOutputs, aLemmatizer)
        }
      }
      // Parse the lemmata
      parseLemmataRecursive(lemmata, List[List[List[String]]](), aLemmatizer)
    }

    def getLemmata(min: Int, max: Int): ResultSet =
    {
      val requestString: String = "SELECT lemmaKey, id FROM lemmata WHERE id >= ? AND id < ?"
      val requestStmt: PreparedStatement = db.prepareStatement(requestString)
      requestStmt.setInt(1,min)
      requestStmt.setInt(2,max)
      requestStmt.executeQuery()
    }
 
    def receive = 
    {
      case Initialize =>
      {
        aLemmatizer.initialise()
      }
      case Close =>
      {
        db.close()
        context.stop(self)
      }
      case Lemmatise(min: Int, max: Int) =>
      {
        val lemmata = getLemmata(min, max)
        parseLemmata(lemmata)
        sender ! NeedWork
      }
    }
  }

  // Open a db connection, run code, and then close the connection
  def workOnDatabase[A](url:String, username:String, password:String, workCode: Connection => A) : A = 
  {
    val driver = "com.mysql.jdbc.Driver"
    val connection: Connection = DriverManager.getConnection(url, username, password)
    val output = workCode(connection)
    connection.close()
    output
  }
  // Use a database connection to get the lemmata
  def getMaxLemmata()(db: Connection): Int = 
  {
    // Query string (get all the distinct lemmaKeys)
    val query: String = "SELECT max(id) FROM lemmata"

    // create the java statement
    val st: Statement = db.createStatement();
      
    // execute the query, and get a java resultset
    val results: ResultSet = st.executeQuery(query);

    results.next()
    results.getInt("Max(id)")
  }

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

    println("Dropping lemma and metadata indexes...")
    val dropIndexStmt1: Statement = db.createStatement()
    dropIndexStmt1.executeUpdate("DROP INDEX IF EXISTS lemmaKey ON lemmata")
    val dropIndexStmt2: Statement = db.createStatement()
    dropIndexStmt2.executeUpdate("DROP INDEX IF EXISTS volumeId ON metadata")

    println("Adding the POS index on forms...")
    val posIndexStmt: Statement = db.createStatement()
    posIndexStmt.executeUpdate("CREATE INDEX POS on forms(POS)")

  }
  def indexCorpus()(db: Connection): Unit =
  {
    println("Marking the lemmaKey unique on lemmata...")
    val uniqLemmaStmt: Statement = db.createStatement()
    uniqLemmaStmt.executeUpdate("ALTER TABLE lemmata ADD UNIQUE lemmata(lemmaKey)")

    println("Adding the lemmaKey index on lemmata...")
    val lemmaIndexStmt: Statement = db.createStatement()
    lemmaIndexStmt.executeUpdate("CREATE UNIQUE INDEX lemmaKey on lemmata(lemmaKey)")

  }

  // This function says what we actually do
  def main() : Unit = 
  {
    val system = ActorSystem("LemmaSystem")
    val master = system.actorOf(Props[LemmaDispatcher],name="Dispatcher")

    master ! Start(workOnDatabase(url,username,password,getMaxLemmata()),
                   batchSize,
                   numWorkers)
  }
  main()
}

// Import the JDBC libraries
import java.sql._

// Import the Akka code for multiprocessing
import akka.actor._

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
  val batchSize = 100

  sealed trait LemmaMessage
  case class Start(numLemmata: Int) extends LemmaMessage
  case class Lemmatise(min:Int, max:Int) extends LemmaMessage
  case object NeedWork extends LemmaMessage
  case object Initialize extends LemmaMessage

  class LemmaWorker extends Actor {
 
    def doLemmatisation(min: Int, max: Int): Unit =
    {
  
    }

    def initialise(): Unit =
    {
        val driver = "com.mysql.jdbc.Driver"
        val connection: Connection = DriverManager.getConnection(url, username, password)
        workCode(connection)
    }
 
    def receive = 
    {
      case Initialize =>
      {
      }
      case Lemmatise(min: Int, max: Int) =>
      {
        doLemmatisation(min, max)
        sender ! Result(calculatePiFor(start, nrOfElements)) // perform the work
      }
    }
  }

  // Open a db connection, run code, and then close the connection
  def workOnDatabase(url:String, username:String, password:String, workCode: Connection => Unit) : Unit = 
  {
    val driver = "com.mysql.jdbc.Driver"
    val connection: Connection = DriverManager.getConnection(url, username, password)
    workCode(connection)
    connection.close()
  }
  // Use a database connection to get the lemmata
  def getUniqueLemmata(implicit db: Connection): ResultSet = 
  {
    // Query string (get all the distinct lemmaKeys)
    val query: String = "SELECT id, lemmaKey FROM lemmata"

    // create the java statement
    val st: Statement = db.createStatement();
      
    // execute the query, and get a java resultset
    val results: ResultSet = st.executeQuery(query);

    results
  }

  def parseLemmata(lemmata: ResultSet)(implicit db: Connection): Unit = 
  {
    def parseLemmataRecursive(lemmata: ResultSet, lemmaOutputs: List[List[List[String]]], aLemmatizer: MyLemmatizer, batchCount: Int)(implicit db: Connection): Unit =
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
      def writeToDatabase(outputs: List[List[List[String]]])(implicit db: Connection): Unit =
      {
        println("A new batch is being processed")
        val updateString: String = "UPDATE lemmata SET lemma = ?, standard = ? WHERE id = ?"
        val deleteString: String = "DELETE FROM tokens where lemmaKey = ?"
        
        val updateStmt: PreparedStatement = db.prepareStatement(updateString)
        val deleteStmt: PreparedStatement = db.prepareStatement(deleteString)

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
              deleteStmt.setString(1,output(1)(1))
              deleteStmt.addBatch()
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
        val deleteResults = deleteStmt.executeBatch()
        val updateResults = updateStmt.executeBatch()
      }
      
      var newBatchCount = batchCount
      var curOutputs = lemmaOutputs
      // Only write out to the database every batchSize number of lemmata
      if (batchCount == batchSize)
      {
        writeToDatabase(lemmaOutputs)
        curOutputs = List[List[List[String]]]()
        newBatchCount = 0
      } 
      // Get the next lemma
      val nextLemma = lemmata.next()
      if (!nextLemma) Unit
      else
      {
        val newOutput: List[List[String]] = processLemma(lemmata.getString("lemmaKey"), lemmata.getInt("id").toString, aLemmatizer)
        val newOutputs: List[List[List[String]]] = curOutputs :+ newOutput
        parseLemmataRecursive(lemmata, newOutputs, aLemmatizer, newBatchCount + 1)
      }
    }
    // Initialize the lemmatizer
    val aLemmatizer: MyLemmatizer = new MyLemmatizer()
    aLemmatizer.initialise()

    // Parse the lemmata
    parseLemmataRecursive(lemmata, List[List[List[String]]](), aLemmatizer, 0)
  }

  def normalizeCorpus()(implicit db: Connection): Unit =
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
  def indexCorpus()(implicit db: Connection): Unit =
  {
    println("Marking the lemmaKey unique on lemmata...")
    val uniqLemmaStmt: Statement = db.createStatement()
    uniqLemmaStmt.executeUpdate("ALTER TABLE lemmata ADD UNIQUE lemmata(lemmaKey)")

    println("Adding the lemmaKey index on lemmata...")
    val lemmaIndexStmt: Statement = db.createStatement()
    lemmaIndexStmt.executeUpdate("CREATE UNIQUE INDEX lemmaKey on lemmata(lemmaKey)")

  }
  def dbWork(conn: Connection) : Unit =
  {
    implicit val db: Connection = conn
    println("Getting unique lemmata...")
    val lemmata: ResultSet = getUniqueLemmata
    println("Starting lemmata processing...")
    parseLemmata(lemmata)
    println("Starting indexing...")
    indexCorpus()
    println("Normalizing corpus...")
    normalizeCorpus()

  }

  // This function says what we actually do
  def main() : Unit = 
  {
    workOnDatabase(url, username, password, dbWork)
  }
  main()
}

// Import the mongodb libraries
import org.mongodb.scala._
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Aggregates._
import org.mongodb.scala.model.Sorts._
import org.mongodb.scala.model.Updates._
import org.mongodb.scala.model.Indexes
import org.mongodb.scala.model._
import org.mongodb.scala.result._

// Get code to deal with concurrency
import scala.concurrent._
import scala.concurrent.duration._

// Get code to deal with mutable lists
import scala.collection.mutable.ListBuffer

// Get code to deal with lemmatization
import lemmatizer.MyLemmatizer

// Here is the main object
object Main
{
  // It's really just a function
  def main(args: Array[String]) : Unit = 
  {

    // Initialize the lemmatizer
    val aLemmatizer: MyLemmatizer = new MyLemmatizer()
    aLemmatizer.initialise()

    // Open a connection to the MongoDatabase
    val mongoClient: MongoClient = MongoClient()
    val database: MongoDatabase = mongoClient.getDatabase("hathitrust")

    // Save connections to the two relevant collections
    val tokens: MongoCollection[Document] = database.getCollection("tokens")
    val lemmata: MongoCollection[Document] = database.getCollection("lemmata")

    // Create a promise for later concurrency blocking
    val promise = Promise[Boolean]

    // This is the observer that will actually process the stream of data from
    // the lemmata collection
    val myObserver: Observer[Document] = new Observer[Document]()
    {
      var batchCount: Long = 0
      var batchSize: Long = 50 // How many lemmata to process in a batch
      var seen: Long = 0 // Track batch size
      var subscription: Option[Subscription] = None // Here is where we store the stream

      // Initialize lists of MongoDB operations on the two collections
      var lemmataRequests: ListBuffer[WriteModel[_ <: Document]] = ListBuffer.empty[WriteModel[_ <: Document]]
      var tokensRequests: ListBuffer[WriteModel[_ <: Document]] = ListBuffer.empty[WriteModel[_ <: Document]]
      var lemmataPromise = Promise[Boolean]
      var tokensPromise = Promise[Boolean]
      var indexPromise = Promise[Boolean]
  
      // Here is what happens when the stream is first encountered
      override def onSubscribe(subscription: Subscription): Unit = 
      {
        // Save the stream and get the first batch
        this.subscription = Some(subscription)
        subscription.request(batchSize)
        batchCount += 1
      }
  
      // Here is what happens to each document from each batch
      override def onNext(result: Document): Unit = 
      {
        // Break the lemmaKey into the form, major word class,
        // lemma word class, and corpus
        val components = result("_id").asString().getValue().split(":::")

        // Initialize results from lemmatizer
        var output = Array.ofDim[String](3)

        // Use corpus appropriate lemmatizer
        if (components(3) == "eme")
        {
          output = aLemmatizer.emeLemmatise(components)
        } else if (components(3) == "ece")
        {
          output = aLemmatizer.eceLemmatise(components)
        } else if (components(3) == "ncf")
        {
          output = aLemmatizer.ncfLemmatise(components)
        }

        // If the lemmatizer gives a null result,
        // that means that the form is either (a)
        // not English or (b) an OCR error or
        // (c) a really rare word
        //
        // In any case we want to delete all instances
        // of that form from the tokens collection and
        // delete the entry from lemmata
        if (output == null)
        {
          tokensRequests += DeleteManyModel(equal("lemmaKey",result("_id")))
          lemmataRequests += DeleteOneModel(equal("_id",result("_id")))
        } else
        // We got a real lemma result, plan to update the lemmata collection
        {
          lemmataRequests += UpdateOneModel(Document("_id" -> result("_id")),
                                            combine(set("lemma",output(2)),
                                                    set("standardSpelling",output(1)))
                                           )
        }

        // Update the counter for determining batch size
        seen += 1

        // We've seen the whole batch, pull down another set of lemmata
        if (seen == batchSize) 
        {
          // Reinitialize the counter
          seen = 0
          
          // Actually perform the deletion and update operations on the database
          tokens.bulkWrite(tokensRequests.toList).subscribe(new Observer[BulkWriteResult] {

            override def onNext(result: BulkWriteResult): Unit = ()

            override def onError(e: Throwable): Unit = tokensPromise.success(false)

            override def onComplete(): Unit = tokensPromise.success(true)
          })
          lemmata.bulkWrite(lemmataRequests.toList).subscribe(new Observer[BulkWriteResult] {

            override def onNext(result: BulkWriteResult): Unit = ()

            override def onError(e: Throwable): Unit = lemmataPromise.success(false)

            override def onComplete(): Unit = lemmataPromise.success(true)
          })

          // Make sure we don't pull another batch until the operations on this
          // batch are complete
          val lemmataFuture = lemmataPromise.future
          val tokensFuture = tokensPromise.future
          println("Batch Count: " + batchCount.toString)
          println("Processing lemmata requests...")
          Await.result(lemmataFuture, Duration(1, java.util.concurrent.TimeUnit.HOURS))

          println("Processing token requests...")
          Await.result(tokensFuture, Duration(1, java.util.concurrent.TimeUnit.HOURS))

          // Reinitialize elements for next batch
          tokensPromise = Promise[Boolean]
          tokensPromise = Promise[Boolean]
          lemmataRequests = ListBuffer.empty[WriteModel[_ <: Document]]
          tokensRequests = ListBuffer.empty[WriteModel[_ <: Document]]

          // Get the next batch
          println("Getting a new batch...")
          subscription.get.request(batchSize)
          batchCount += 1
        }
      }

      // Make sure to update the promise with failure if there is an error
      override def onError(e: Throwable): Unit = 
      {
        println(s"Error: $e")
        promise.success(false)
      }

      // Make sure to complete the promise after everything has been processed
      override def onComplete(): Unit = 
      {
        println("Creating index...")
        tokens.createIndex(Indexes.ascending("POS")).subscribe(new Observer[String] 
          {

            override def onNext(result: String): Unit = ()

            override def onError(e: Throwable): Unit = indexPromise.success(false)

            override def onComplete(): Unit = indexPromise.success(true)
          })

        val indexFuture = indexPromise.future
        Await.result(indexFuture, Duration(10, java.util.concurrent.TimeUnit.DAYS))
        println("Completed")
        promise.success(true)
      }
    }
    // Run the observer code on all of the unique lemmata
    val completed = lemmata.find(not(exists("lemma"))).subscribe(myObserver)

    // Make sure that the execution only ends after 10 hours or finishing
    // processing all of the lemmata
    val future = promise.future
    Await.result(future, Duration(10, java.util.concurrent.TimeUnit.HOURS))
  }
}

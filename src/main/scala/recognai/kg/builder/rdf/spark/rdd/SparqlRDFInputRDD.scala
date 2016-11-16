package recognai.kg.builder.rdf.spark.rdd

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}
import recognai.kg.builder.BuilderConstants._
import recognai.kg.builder.rdf.model._
import recognai.kg.builder.rdf.operations.SparqlOperationsWithJena

import scala.util.Try

/**
  * Created by @frascuchon on 09/11/2016.
  */
class SparqlRDFInputRDD(private val partitionSize: Option[Int] = None,
                        private val countLimit: Option[Int] = None,
                        private val sparqlEndpoint: String,
                        private val sparqlQuery: String,
                        private val sparqlQueryPrefix: String)(@transient sc: SparkContext)
  extends RDD[Triple](sc, Seq.empty) {

  import SparqlRDFInputRDD.SparqlInputPartition

  @transient private lazy val sparql = SparqlOperationsWithJena

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[Triple] =

    split match {
      case SparqlInputPartition(_, p, offset, limit) =>
        sparql.query(sparqlEndpoint, sparqlQueryPrefix, sparqlQuery, offset, limit)
      case _ => Iterator.empty
    }

  override protected def getPartitions: Array[Partition] = {
    /**
      * @return The configured number of partitions if defined, default parallelism otherwise
      */
    def getRecordsPerPartition(all: Long): Int = partitionSize match {
      case Some(p) => p
      case None =>
        logWarning("Setting partitions to default parallelism")
        all.toInt
    }

    val recordsCount =
      if (countLimit.isDefined) countLimit.map(_.toLong)
      else sparql.queryCount(sparqlEndpoint, sparqlQuery, sparqlQueryPrefix)

    recordsCount match {
      case Some(count) =>
        logDebug(s"Found $count records for query $sparqlQuery")

        val recordsPerPartition: Int = getRecordsPerPartition(count)
        val partitions: Int = Try(Math.ceil(count * 1.0 / recordsPerPartition).toInt).getOrElse(0)

        logDebug(s"Creating $partitions partitions for incoming sparql query {${sparqlQuery.replace("\n", "")}}")
        (0 until partitions map {
          p => SparqlInputPartition(id, index = p, offset = p * recordsPerPartition, limit = recordsPerPartition)
        }) toArray
      case None => Array.empty
    }
  }
}

object SparqlRDFInputRDD {

  def apply()(implicit sc: SparkContext): SparqlRDFInputRDD = {

    val configuration = sc.getConf

    val partitionSize = configuration.getOption(PARTITION_SIZE_PROPERTY).map(_.toInt)
    val sparqlQuery = configuration.get(SPARQL_QUERY_PROPERTY)
    val sparqlQueryPrefixes = configuration.get(SPARQL_QUERY_PREFIX_PROPERTY)
    val sparqlEndpoint = configuration.get(SPARQL_ENDPOINT_PROPERTY)

    new SparqlRDFInputRDD(partitionSize, countLimit = None, sparqlEndpoint, sparqlQuery, sparqlQueryPrefixes)(sc)
  }

  def apply(partitionSize: Option[Int] = None
            , countLimit: Option[Int] = None
            , endpoint: String
            , query: String
            , prefixes: Map[String, String])(implicit sc: SparkContext): SparqlRDFInputRDD =

    newSparqlRDD(partitionSize, countLimit, endpoint, query, prefixes)

  private def newSparqlRDD(partitionSize: Option[Int]
                           , countLimit: Option[Int]
                           , endpoint: String
                           , query: String
                           , prefixes: Map[String, String])(implicit sc: SparkContext) =

    new SparqlRDFInputRDD(partitionSize
      , countLimit
      , endpoint, query
      , prefixes.map { case (k, v) => s"PREFIX $k: <$v>" }.mkString(" "))(sc)


  /**
    * Sparql Input partition definition
    *
    * @param rddID  the RDD id
    * @param index  the partition id
    * @param offset the sparql query offset
    * @param limit  the sparql query limit
    */
  private[rdd] case class SparqlInputPartition(rddID: Int, override val index: Int, offset: Long, limit: Long) extends Partition


}




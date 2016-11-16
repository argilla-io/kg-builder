package recognai.kg.builder.rdf.spark.enrichment

import org.apache.spark.sql.{Dataset, SparkSession}
import recognai.kg.builder.rdf.model

/**
  * Created by @frascuchon on 13/11/2016.
  */
trait RDFEnrichment {

  import model._
  /**
    * Merge input rdf triples dataset with another source
    *
    * @param input
    * @param sparkSession the spark session
    * @return a new triples dataset with enhanced information from external source
    */
  def enrichRDF(input: Dataset[Triple])(implicit sparkSession: SparkSession): Dataset[Triple]
}

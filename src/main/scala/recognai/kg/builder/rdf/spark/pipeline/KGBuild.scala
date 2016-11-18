package recognai.kg.builder.rdf.spark.pipeline

import java.net.URL

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import recognai.kg.builder.rdf.model._
import recognai.kg.builder.rdf.spark.enrichment.{ApplicationConfig, EntityConf, Query}
import recognai.kg.builder.rdf.spark.rdd.SparqlRDFInputRDD
import recognai.kg.builder.rdf.store.ESStore

import scala.util.Try

/**
  * Created by @frascuchon on 10/11/2016.
  */
object KGBuild extends LazyLogging {


  def apply(config: ApplicationConfig)(implicit session: SparkSession): Dataset[Subject] = {

    def readDataSources(datasourcesConfig: List[EntityConf])(implicit sc: SparkContext): RDD[Triple] = {
      val triplesInputs = for {
        EntityConf(endpoint, prefixes, queries, partitionSize, excludedPredicates) <- datasourcesConfig
        Query(query, limit) <- queries
      } yield SparqlRDFInputRDD(partitionSize, limit, endpoint, query, prefixes)
        .filter {
          case Triple(_, p, _) => !excludedPredicates.forall(_.exists(StringUtils.contains(p, _)))
        }

      sc.union(triplesInputs) persist()
    }

    def createPropertiesMap(group: Iterator[Triple]): Map[String, Seq[ObjectProperty]] = {

      def mergeProperties(map: Map[String, Seq[ObjectProperty]], triple: Triple)
      : Map[String, Seq[ObjectProperty]] = {

        val key = triple.Predicate
        val value = map.getOrElse(key, Seq.empty);
        map + (key -> value.+:(triple.Object))
      }

      group.foldLeft(Map.empty[String, Seq[ObjectProperty]])(mergeProperties)
    }

    import session.implicits._
    implicit val sc = session.sparkContext

    val triples = readDataSources(config.entities)
    val enrichments = readDataSources(config.enrichments)

    val enrichedTriples = sc.union(triples, enrichments) toDS()

    enrichedTriples.groupByKey(_.Subject)
      .mapGroups { (subject, groupData) => Subject(subject, createPropertiesMap(groupData)) }

  }
}

object Main extends App with LazyLogging {

  Option(Try(args(0)).getOrElse(null)) match {
    case Some(configFile) =>
      runBuild(configFile)
    case None =>
      logger.error("Missing config url  command line argument. Please, provide URL configuration")
      sys.exit(1)
  }

  private def runBuild(path: String): Unit = {
    val objectMapper = new ObjectMapper(new YAMLFactory())
    objectMapper.registerModule(DefaultScalaModule)

    Try(objectMapper.readValue(new URL(path), classOf[ApplicationConfig]))
      .map(config => {

        // Create spark session
        val sessionBuilder = SparkSession.builder()
          .appName(config.spark.appName)
        config.spark.master.foreach(sessionBuilder.master)

        implicit val sparkSession = sessionBuilder.getOrCreate()

        // Create subjects dataset
        val subjects = KGBuild(config)

        // Persist dataset
        ESStore(subjects, config.store)

      }) recover {
      case e: Exception =>
        logger.error(s"Cannot load application properties from $path", e)
        sys.exit(1)
    }

  }

}


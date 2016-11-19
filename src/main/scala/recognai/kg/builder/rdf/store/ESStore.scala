package recognai.kg.builder.rdf.store

import org.apache.spark.sql.{Dataset, SparkSession}
import org.elasticsearch.hadoop.cfg.ConfigurationOptions._
import recognai.kg.builder.BuilderConstants._
import recognai.kg.builder.rdf.model.{LiteralProperty, ObjectProperty, Subject}
import recognai.kg.builder.rdf.spark.enrichment.StoreConfig

/**
  * Created by @frascuchon on 14/11/2016.
  */
object ESStore {

  private def cleanKey(key: String): String = key.split("://")(1).replace(".", "_").replace("/", ":")

  private def cleanProperties(properties: Map[String, Seq[ObjectProperty]], excludes: List[String])
  : Map[String, Seq[String]] = {

    properties
      .filterKeys(!excludes.contains(_))
      .flatMap {
        case (key, values) =>
          val filteredProperties = values
            .filter(_.isLiteral())
            .map { case ObjectProperty(_, Some(LiteralProperty(v, _, _))) => v }

          if (filteredProperties.isEmpty) None
          else Some(cleanKey(key) -> filteredProperties)
      }
  }

  private case class ESSubject(id: String, `type`: String, features: Map[String, Seq[String]])

  def apply(subjects: Dataset[Subject], config: StoreConfig)(implicit sparkSession: SparkSession): Unit = {

    def persistSchema(subjects: Dataset[Subject], config:StoreConfig): Unit = {

      import org.elasticsearch.spark.sql._
      import recognai.kg.builder.rdf.SchemaExtractionModule._
      import sparkSession.implicits._

      val schema = subjects.infersSchema(config.`exclude.features`)

      val resource = config.`elasticsearch.hadoop`.getOrElse(ES_RESOURCE, DEFAULT_RESOURCE)
      schema
        .map(t => {
          t.copy(propertiesDefinition = t.propertiesDefinition.map { case (k, v) => cleanKey(k) -> v })
        })
        .saveToEs(
          config.`elasticsearch.hadoop` ++
            Map(ES_RESOURCE -> s"schemas/${resource.replace("/", ":")}", ES_MAPPING_ID -> SCHEMA_ID_FIELD)
        )
    }

    import org.elasticsearch.spark.sql._
    import sparkSession.implicits._

    persistSchema(subjects, config)

    val excludedProperties = config.`exclude.features`

    subjects
      .map(s =>
        ESSubject(id = s.Subject
          , `type` = s.getType.getOrElse("none")
          , features = cleanProperties(s.properties, excludedProperties)))
      .saveToEs(config.`elasticsearch.hadoop`)
  }
}

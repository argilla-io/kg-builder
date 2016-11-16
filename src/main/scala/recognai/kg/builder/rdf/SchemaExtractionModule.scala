package recognai.kg.builder.rdf

import org.apache.spark.sql.{Dataset, SparkSession}
import recognai.kg.builder.rdf.model.Subject

import scala.util.Try

object SchemaExtractionModule {

  case class Type(name: Option[String], propertiesDefinition: Map[String, String])

  implicit def schemaOperations(subjects: Dataset[Subject]) = new SchemaExtractor(subjects)

  /**
    * Build the schema from a subjects dataset
    *
    * @param subjects the subjects dataset
    * @return A Dataset with inferred schema types
    */
  class SchemaExtractor(@transient subjects: Dataset[Subject]) extends Serializable {

    def infersSchema(excluded: List[String])(implicit sparkSession: SparkSession): Dataset[Type] = {

      import sparkSession.implicits._

      subjects
        .map(s => (s.getType, propertiesDefinitions(s, excluded)))
        .groupByKey(_._1)
        .mapGroups { case (t, properties) => Type(t, properties.map(_._2).reduce(_ ++ _)) }

    }

    private def propertiesDefinitions(s: Subject, excluded: List[String]): Map[String, String] =
      s.properties.flatMap {
        // TODO Several types per property
        case (name, values) =>
          if (excluded.contains(name)) None
          else Try(name, values.flatMap(_.getType()).head) toOption
      }
  }

}

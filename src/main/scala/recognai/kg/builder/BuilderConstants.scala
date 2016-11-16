package recognai.kg.builder

/**
  * Created by @frascuchon on 11/11/2016.
  */
object BuilderConstants {

  // Input params
  val PARTITION_SIZE_PROPERTY = "kg.builder.partitionSize"
  val SPARQL_QUERY_PROPERTY = "kg.builder.query"
  val SPARQL_QUERY_PREFIX_PROPERTY = "kg.builder.queryPrefix"
  val SPARQL_ENDPOINT_PROPERTY = "kg.builder.endpoint"

  // Read query triples
  val SUBJECT_VARIABLE_NAME = "subject"
  val PREDICATE_VARIABLE_NAME = "predicate"
  val OBJECT_VARIABLE_NAME = "object"

  // ES Write
  val DEFAULT_RESOURCE = "kg/subjects"
  val SCHEMA_ID_FIELD = "name"

}

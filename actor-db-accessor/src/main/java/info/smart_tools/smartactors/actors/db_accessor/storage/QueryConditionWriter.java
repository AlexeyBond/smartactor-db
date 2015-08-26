package info.smart_tools.smartactors.actors.db_accessor.storage;

/**
 *  Writes a part of SQL statement search conditions.
 */
@FunctionalInterface
public interface QueryConditionWriter {
    void write(QueryStatement query,QueryConditionWriterResolver resolver,
               FieldPath contextFieldPath,Object queryParameter) throws QueryBuildException;
}

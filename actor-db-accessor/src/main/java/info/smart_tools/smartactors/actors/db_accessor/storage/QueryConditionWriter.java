package info.smart_tools.smartactors.actors.db_accessor.storage;

public interface QueryConditionWriter {
    void write(QueryStatement query,QueryConditionWriterResolver resolver,
               FieldPath contextFieldPath,Object queryParameter) throws QueryBuildException;
}

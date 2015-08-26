package info.smart_tools.smartactors.actors.db_accessor.storage;

public interface QueryConditionWriterResolver {
    QueryConditionWriter resolve(String conditionKey) throws QueryBuildException;
}

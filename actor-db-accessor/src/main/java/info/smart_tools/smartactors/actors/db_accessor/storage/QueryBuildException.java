package info.smart_tools.smartactors.actors.db_accessor.storage;

public class QueryBuildException extends Exception {
    public QueryBuildException(String message) {
        super(message);
    }

    public QueryBuildException(String message, Exception cause) {
        super(message,cause);
    }
}

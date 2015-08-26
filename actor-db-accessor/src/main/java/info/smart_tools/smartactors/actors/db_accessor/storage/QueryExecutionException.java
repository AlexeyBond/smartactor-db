package info.smart_tools.smartactors.actors.db_accessor.storage;

public class QueryExecutionException extends Exception {
    public QueryExecutionException(String message) {
        super(message);
    }

    public QueryExecutionException(String message, Exception cause) {
        super(message,cause);
    }
}

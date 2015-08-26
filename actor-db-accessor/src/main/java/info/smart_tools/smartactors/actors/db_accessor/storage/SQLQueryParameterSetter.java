package info.smart_tools.smartactors.actors.db_accessor.storage;

import java.sql.PreparedStatement;
import java.sql.SQLException;

@FunctionalInterface
public interface SQLQueryParameterSetter {
    int setParameters(PreparedStatement statement, int firstIndex) throws SQLException;
}

package info.smart_tools.smartactors.actors.db_accessor.storage;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 *  Sets some of {@link PreparedStatement}'s parameters starting from {@code firstIndex}.
 *  Returns index of next parameter to be set.
 */
@FunctionalInterface
public interface SQLQueryParameterSetter {
    int setParameters(PreparedStatement statement, int firstIndex) throws SQLException;
}

package info.smart_tools.smartactors.actors.db_accessor.storage;

import java.io.StringWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

public class QueryStatement {
    StringWriter bodyWriter;
    List<SQLQueryParameterSetter> parameterSetters;

    public QueryStatement() {
        this.bodyWriter = new StringWriter();
        this.parameterSetters = new LinkedList<>();
    }

    public Writer getBodyWriter() {
        return bodyWriter;
    }

    public void pushParameterSetter(SQLQueryParameterSetter setter) {
        parameterSetters.add(setter);
    }

    public PreparedStatement compile(Connection connection) throws SQLException {
        PreparedStatement stmt = connection.prepareStatement(this.bodyWriter.toString());

        int index = 1;

        for (SQLQueryParameterSetter setter : this.parameterSetters) {
            index = setter.setParameters(stmt,index);
        }

        return stmt;
    }
}
package info.smart_tools.smartactors.actors.db_accessor.storage.postgresql_jsonb;

import info.smart_tools.smartactors.actors.db_accessor.messages.CreateCollectionQueryMessage;
import info.smart_tools.smartactors.actors.db_accessor.messages.DeletionQueryMessage;
import info.smart_tools.smartactors.actors.db_accessor.messages.SearchQueryMessage;
import info.smart_tools.smartactors.actors.db_accessor.messages.UpsertQueryMessage;
import info.smart_tools.smartactors.actors.db_accessor.storage.QueryExecutionException;
import info.smart_tools.smartactors.actors.db_accessor.storage.QueryExecutor;
import info.smart_tools.smartactors.core.ChangeValueException;
import info.smart_tools.smartactors.core.FieldName;
import info.smart_tools.smartactors.core.IObject;
import info.smart_tools.smartactors.core.IObjectCreator;
import info.smart_tools.smartactors.core.impl.SMObjectCreator;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

class QueryExecutorImpl implements QueryExecutor {
    IObjectCreator objectCreator = new SMObjectCreator();

    public void executeSearchQuery(PreparedStatement statement,SearchQueryMessage message)
            throws QueryExecutionException {
        try {
            ResultSet resultSet = statement.executeQuery();
            List<IObject> objects = new LinkedList<>();

            while (resultSet.next()) {
                String jsonValue = resultSet.getString(Schema.DOCUMENT_COLUMN_NAME);
                IObject object = objectCreator.create(jsonValue);

                try {
                    object.setValue(new FieldName("id"), resultSet.getLong(Schema.ID_COLUMN_NAME));
                } catch (ChangeValueException e) {
                    throw new QueryExecutionException("Could not set document's id field.",e);
                }

                objects.add(object);
            }

            /*TODO: How to set message field?*/
            //message.set(message.getTargetField(),objects);
        } catch (SQLException e) {
            throw new QueryExecutionException("Search query execution failed because of SQL exception.",e);
        }
    }

    private void justExecuteQuery(PreparedStatement statement)
            throws SQLException {
        statement.execute();
    }

    public void executeDeleteQuery(PreparedStatement statement,DeletionQueryMessage message)
            throws QueryExecutionException {
        try {
            justExecuteQuery(statement);
        } catch (SQLException e) {
            throw new QueryExecutionException("Deletion query execution failed because of SQL exception.",e);
        }
    }

    public void executeCollectionCreationQuery(PreparedStatement statement,CreateCollectionQueryMessage message)
            throws QueryExecutionException {
        try {
            justExecuteQuery(statement);
        } catch (SQLException e) {
            throw new QueryExecutionException("Collection creation query execution failed because of SQL exception.",e);
        }
    }

    public void executeUpdateQuery(PreparedStatement statement,UpsertQueryMessage message)
            throws QueryExecutionException {
        try {
            justExecuteQuery(statement);
        } catch (SQLException e) {
            throw new QueryExecutionException("Update query execution failed because of SQL exception.",e);
        }
    }

    public void executeInsertionQuery(PreparedStatement statement,UpsertQueryMessage message)
            throws QueryExecutionException {
        try {
            ResultSet resultSet = statement.executeQuery();
            Iterator<IObject> documentsIterator = message.getDocuments().iterator();

            while(resultSet.next()) {
                if (!documentsIterator.hasNext()) {
                    throw new QueryExecutionException("Database returned too much of generated ids.");
                }

                IObject document = documentsIterator.next();

                try {
                    document.setValue(new FieldName("id"), resultSet.getLong("id"));
                } catch (ChangeValueException e) {
                    throw new QueryExecutionException("Could not set new id on inserted document.");
                }
            }

            if(documentsIterator.hasNext()) {
                throw new QueryExecutionException("Database returned not enough of generated ids.");
            }
        } catch (SQLException e) {
            throw new QueryExecutionException("Insertion query execution failed because of SQL exception.",e);
        }
    }

}

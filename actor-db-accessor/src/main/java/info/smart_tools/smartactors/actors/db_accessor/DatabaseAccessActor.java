package info.smart_tools.smartactors.actors.db_accessor;

import info.smart_tools.smartactors.actors.db_accessor.messages.*;
import info.smart_tools.smartactors.actors.db_accessor.storage.QueryStatement;
import info.smart_tools.smartactors.actors.db_accessor.storage.postgresql_jsonb.Driver;
import info.smart_tools.smartactors.core.IMessage;
import info.smart_tools.smartactors.core.actors.Actor;
import info.smart_tools.smartactors.core.actors.annotations.FromState;
import info.smart_tools.smartactors.core.actors.annotations.Handler;
import info.smart_tools.smartactors.core.actors.annotations.InitialState;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

@InitialState("disconnected")
public class DatabaseAccessActor extends Actor {
    private Connection connection;
    private Driver storageDriver;

    @Handler("connect")
    @FromState("disconnected")
    public void connect(ConnectMessage message) throws Exception {
        this.connectToDatabase(
            message.getUrl(),
            message.getUsername(),
            message.getPassword(),
            message.getDriver()
        );

        storageDriver = new Driver();

        connection.setAutoCommit(false);

        become(new State("connected"));
    }

    @Handler("disconnect")
    @FromState("connected")
    public void disconnectHandler(IMessage message) throws Exception {
        this.disconnectFromDatabase();
    }

    @Handler("create-collection")
    @FromState("connected")
    public void createCollectionHandler(CreateCollectionQueryMessage message) {
        /*TODO: Create collection here.*/
        try {
            QueryStatement query = storageDriver.getQueryBuilder().buildCollectionCreationQuery(message);
            storageDriver.getQueryExecutor().executeCollectionCreationQuery(query.compile(connection), message);
        } catch (Exception e) {

        }
    }

    @Handler("find-documents")
    @FromState("connected")
    public void findDocumentsHandler(SearchQueryMessage message) {
        /*TODO: Perform search.*/
        try {
            QueryStatement query = storageDriver.getQueryBuilder().buildSearchQuery(message);
            storageDriver.getQueryExecutor().executeSearchQuery(query.compile(connection),message);
        } catch (Exception e) {

        }
    }

    @Handler("update-documents")
    @FromState("connected")
    public void updateDocuments(UpsertQueryMessage message) {
        /*TODO: Update document(s).*/
        try {
            QueryStatement query = storageDriver.getQueryBuilder().buildUpdateQuery(message);
            storageDriver.getQueryExecutor().executeUpdateQuery(query.compile(connection), message);

            connection.commit();
        } catch (Exception e) {
            try {
                connection.rollback();
            } catch (SQLException ee) {
                /*TODO: Handle.*/
            }
        }
    }

    @Handler("insert-documents")
    @FromState("connected")
    public void insertDocuments(UpsertQueryMessage message) {
        try {
            QueryStatement query = storageDriver.getQueryBuilder().buildInsertionQuery(message);
            storageDriver.getQueryExecutor().executeInsertionQuery(query.compile(connection),message);

            connection.commit();
        } catch (Exception e) {
            e.printStackTrace();
            try {
                connection.rollback();
            } catch (SQLException ee) {
                /**/
            }
        }
    }

    @Handler("delete-documents")
    @FromState("connected")
    public void deleteDocumentsHandler(DeletionQueryMessage message) {
        /*TODO: Delete documents.*/
        try {
            QueryStatement query = storageDriver.getQueryBuilder().buildDeleteQuery(message);
            storageDriver.getQueryExecutor().executeDeleteQuery(query.compile(connection), message);
        } catch (Exception e) {

        }
    }

    @Handler("create-collection")
    @Handler("find-documents")
    @Handler("upsert-documents")
    @Handler("delete-documents")
    @FromState("disconnected")
    public void queryHandler(IMessage message) {
        /*TODO: Handle queries received when there is no connection to database.*/
    }

    private void connectToDatabase(String url,String username,String password,String driver) throws Exception {
        System.out.printf("Connecting to %s using %s as %s:%s\n",url,driver,username,password);
        Class<?> driverClass = Class.forName(driver);

        connection = DriverManager.getConnection(url,username,password);
    }

    private void disconnectFromDatabase() throws Exception {
        connection.close();
    }
}

package info.smart_tools.smartactors.actors.db_accessor;

import info.smart_tools.smartactors.actors.db_accessor.messages.*;
import info.smart_tools.smartactors.core.IMessage;
import info.smart_tools.smartactors.core.actors.Actor;
import info.smart_tools.smartactors.core.actors.annotations.FromState;
import info.smart_tools.smartactors.core.actors.annotations.Handler;
import info.smart_tools.smartactors.core.actors.annotations.InitialState;

import java.sql.Connection;
import java.sql.DriverManager;

@InitialState("disconnected")
public class DatabaseAccessActor extends Actor {
    private Connection connection;

    @Handler("connect")
    @FromState("disconnected")
    public void connect(ConnectMessage message) throws Exception {
        try {
            this.connectToDatabase(
                message.getUrl(),
                message.getUsername(),
                message.getPassword(),
                message.getDriver()
            );
        } catch (Exception e) {
            System.out.println("Exception: "+e.toString());
        }

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
    }

    @Handler("find-documents")
    @FromState("connected")
    public void findDocumentsHandler(SearchQueryMessage message) {
        /*TODO: Perform search.*/
    }

    @Handler("upsert-documents")
    @FromState("connected")
    public void upsertDocuments(UpsertQueryMessage message) {
        /*TODO: Upsert (update or insert) document(s).*/
    }

    @Handler("delete-documents")
    @FromState("connected")
    public void deleteDocumentsHandler(DeletionQueryMessage message) {
        /*TODO: Delete documents.*/
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

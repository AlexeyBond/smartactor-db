package info.smart_tools.smartactors.actors.db_accessor.messages;

public interface ConnectMessage {
    String getUrl();
    String getDriver();
    String getUsername();
    String getPassword();
}

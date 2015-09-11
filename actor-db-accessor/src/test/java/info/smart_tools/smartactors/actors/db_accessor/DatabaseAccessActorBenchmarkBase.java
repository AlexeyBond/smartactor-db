package info.smart_tools.smartactors.actors.db_accessor;

import info.smart_tools.smartactors.actors.db_accessor.messages.ConnectMessage;
import info.smart_tools.smartactors.actors.db_accessor.messages.UpsertQueryMessage;
import info.smart_tools.smartactors.core.FieldName;
import info.smart_tools.smartactors.core.IObject;
import info.smart_tools.smartactors.core.impl.SMObject;
import org.openjdk.jmh.Main;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.RunnerException;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.*;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Measurement(iterations = 10)
@Fork(1)
@Threads(1)
public abstract class DatabaseAccessActorBenchmarkBase {
    protected DatabaseAccessActor databaseAccessActor;

    @Setup
    public void setUp() throws Exception {
        databaseAccessActor = new DatabaseAccessActor();

        ConnectMessage connectMessage = mock(ConnectMessage.class);

        when(connectMessage.getDriver()).thenReturn("org.postgresql.Driver");
        when(connectMessage.getUrl()).thenReturn("jdbc:postgresql://localhost:5433/postgres");
        when(connectMessage.getUsername()).thenReturn("test_user");
        when(connectMessage.getPassword()).thenReturn("password");

        databaseAccessActor.connect(connectMessage);
    }

    public static void runBenchmarks(Class<?> clazz) throws RunnerException, IOException {
        Main.main(new String[]{clazz.getSimpleName()});
    }
}

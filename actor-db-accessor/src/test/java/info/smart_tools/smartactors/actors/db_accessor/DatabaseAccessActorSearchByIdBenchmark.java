package info.smart_tools.smartactors.actors.db_accessor;

import info.smart_tools.smartactors.actors.db_accessor.messages.SearchQueryMessage;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.runner.RunnerException;

import java.io.IOException;
import java.util.HashMap;
import java.util.PrimitiveIterator;
import java.util.Random;
import java.util.stream.LongStream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DatabaseAccessActorSearchByIdBenchmark extends DatabaseAccessActorBenchmarkBase {
    private PrimitiveIterator.OfLong random = new Random().longs(11611956l,11611956l+1000000l).iterator();

    @Benchmark
    @Measurement(iterations = 1000)
    public void searchById() {
        databaseAccessActor.findDocumentsHandler(produceSearchByIdMessage(random.next()));
    }

    SearchQueryMessage produceSearchByIdMessage(Long id) {
        SearchQueryMessage searchQueryMessage = mock(SearchQueryMessage.class);

        when(searchQueryMessage.getPageNumber()).thenReturn(1);
        when(searchQueryMessage.getCollectionName()).thenReturn("testCollection");
        when(searchQueryMessage.getPageSize()).thenReturn(1);
        when(searchQueryMessage.getTargetField()).thenReturn("found");
        when(searchQueryMessage.getQuery()).thenReturn(
                new HashMap<String,Object>(){{
                    put("id",new HashMap<String,Object>(){{
                        put("$eq",id);
                    }});
                }});

        return searchQueryMessage;
    }

    public static void main(String[] args) throws RunnerException, IOException {
        runBenchmarks(DatabaseAccessActorSearchByIdBenchmark.class);
    }
}

package info.smart_tools.smartactors.actors.db_accessor;

import info.smart_tools.smartactors.actors.db_accessor.messages.SearchQueryMessage;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;

import java.io.IOException;
import java.util.*;
import java.util.stream.LongStream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DatabaseAccessActorSearchByIdBenchmark extends DatabaseAccessActorBenchmarkBase {
    private PrimitiveIterator.OfLong random = new Random().longs(11611956l,11611956l+1000000l).iterator();

    @Benchmark
    @Warmup(iterations = 1)
    @Measurement(iterations = 100)
    public void searchOneById() {
        databaseAccessActor.findDocumentsHandler(produceSearchByIdMessage(random.next()));
    }

    public void searchFewById(int count) {
        Long[] ids = new Long[count];
        while(count-- != 0) {
            ids[count] = random.next();
        }
        databaseAccessActor.findDocumentsHandler(produceSearchByIdMessage(ids));
    }

    @Benchmark
    @Warmup(iterations = 1)
    @Measurement(iterations = 100)
    public void search10ById() {
        searchFewById(10);
    }

    @Benchmark
    @Warmup(iterations = 1)
    @Measurement(iterations = 100)
    public void search100ById() {
        searchFewById(100);
    }

    @Benchmark
    @Warmup(iterations = 1)
    @Measurement(iterations = 10)
    public void search1000ById() {
        searchFewById(1000);
    }

    SearchQueryMessage produceSearchByIdMessage(Long... id) {
        SearchQueryMessage searchQueryMessage = mock(SearchQueryMessage.class);

        when(searchQueryMessage.getPageNumber()).thenReturn(1);
        when(searchQueryMessage.getCollectionName()).thenReturn("testCollection");
        when(searchQueryMessage.getPageSize()).thenReturn(1);
        when(searchQueryMessage.getTargetField()).thenReturn("found");
        when(searchQueryMessage.getQuery()).thenReturn(
                new HashMap<String,Object>(){{
                    put("id",new HashMap<String,Object>(){{
                        if(id.length == 1) {
                            put("$eq",id[0]);
                        } else {
                            put("$in",Arrays.asList(id));
                        }
                    }});
                }});

        return searchQueryMessage;
    }

    public static void main(String[] args) throws RunnerException, IOException {
        runBenchmarks(DatabaseAccessActorSearchByIdBenchmark.class);
    }
}

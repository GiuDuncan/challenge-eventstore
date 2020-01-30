package net.intelie.challenges;
import java.util.Collections;
import java.util.Comparator;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;


/**
 * The implementation should be correct, fast, memory-efficient, and thread-safe.
 * You may consider that insertions, deletions, queries, and iterations will happen frequently and concurrently. This will be a system hotspot. Optimize at will.
 *
 * We expect you to:
 *
 * Write tests;
 * Provide some evidence of thread-safety;
 * Justify design choices, arguing about costs and benefits involved. You may write those as comments inline or, if you wish, provide a separate document summarizing those choices;
 * Write all code and documentation in english.
 */



public class EventStoreClass implements EventStore {

    /**
     * Hashmap is a great choice to store key, value items since it provides expected constant-time performance O(1)
     * for most operations like add(), remove() and contains() and usually fast for other.
     * @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/HashMap.html">HashMap</a>
     *
     * In this case, the hashmap would be build as: key=event type, value=all events from type.
     * It makes sense to use the specified hashmap since we need to be able to remove all events of a certain type
     * and the iterator also access events using type.
     *
     * But, as it needs to be concurrent, we can use the ConcurrentHashMap which is an tread-safe structure.
     * @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/ConcurrentHashMap.html">ConcurrentHashMap</a>
     *
     * All the events also must be stored in a concurrent structure, in this case I chose the ConcurrentSkipListSet
     * as it has a constructor that specifies a comparator to order its elements.
     *
     * As the elements are ordered by the timestamp, the query function needs to perform a binary search to
     * get the set that has a timestamp between start and end time.
     * @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/ConcurrentSkipListSet.html">ConcurrentSkipListSet</a>
     */
    ConcurrentHashMap<String, ConcurrentSkipListSet<Event>> events = new ConcurrentHashMap<>();
    Comparator<Event> comparator = Comparator.comparing(Event::timestamp);

    @Override
    public void insert(Event event) {
        ConcurrentSkipListSet<Event> current;
        boolean exists = events.containsKey(event.type());
        if (exists) { // list of events of type already exists
            current = events.get(event.type());
            current.add(event);
        }
        else { // new list of events of type
            current = new ConcurrentSkipListSet<Event>(comparator);
            current.add(event);
            events.put(event.type(), current);
        }
        //current.forEach(s -> System.out.println(s.timestamp())); // todo: remove
    }

    @Override
    public void removeAll(String type) {
        if (!events.containsKey(type)) return;
        events.get(type).clear();
        events.remove(type);
    }

    @Override
    public EventIterator query(String type, long startTime, long endTime) {

        ConcurrentSkipListSet<Event> query_events = (ConcurrentSkipListSet<Event>)
                events.get(type).subSet(new Event(type, startTime), true, new Event(type, endTime), false);

        query_events.forEach(s -> System.out.println(s.timestamp())); // todo: remove
        return new EventIteratorClass(query_events);
    }
}

package net.intelie.challenges;

import org.junit.Test;

import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListSet;

import static org.junit.Assert.assertEquals;

public class EventStoreTest {
    @Test
    public void isOrdered() throws Exception {
        EventStoreClass store = defaultStore();
        ConcurrentSkipListSet<Event> events = store.events.get("typeA");
        Comparator<Event> comparator = Comparator.comparing(Event::timestamp);
        boolean sorted = isSorted(events, comparator);

        assertEquals(sorted, true);
    }

    @Test
    public void removed() throws Exception {
        EventStoreClass store = defaultStore();
        store.removeAll("typeB");
        ConcurrentSkipListSet<Event> events = store.events.get("typeB");
        assertEquals(events, null);
    }

    @Test
    public void queryTest1() throws Exception {
        EventStoreClass store = defaultStore();
        store.removeAll("typeB");
        EventIterator it = store.query("typeA", 12L, 35L);
        
    }

    @Test
    public void queryTestNoResultInTimePeriod() throws Exception {
        EventStoreClass store = defaultStore();
        store.removeAll("typeB");
        EventIterator it = store.query("typeA", 0L, 5L);

        assertEquals(it.moveNext(), false);
    }

    private EventStoreClass defaultStore(){
        EventStoreClass store = new EventStoreClass();
        store.insert(new Event("typeA", 30L));
        store.insert(new Event("typeA", 32L));
        store.insert(new Event("typeA", 50L));
        store.insert(new Event("typeA", 10L));
        store.insert(new Event("typeA", 20L));
        store.insert(new Event("typeB", 20L));
        return store;
    }

    private static boolean isSorted(ConcurrentSkipListSet<Event> events,  Comparator<Event> comparator) {
        if (events == null || events.size() <= 1) {
            return true;
        }

        Iterator<Event> it = events.iterator();
        Event current, previous = it.next();
        while (it.hasNext()) {
            current = it.next();
            if (comparator.compare(previous, current) > 0) {
                return false;
            }
            previous = current;
        }
        return true;
    }
}

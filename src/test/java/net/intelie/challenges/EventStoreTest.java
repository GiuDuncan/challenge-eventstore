package net.intelie.challenges;

import org.junit.Test;

import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListSet;

import static org.junit.Assert.assertEquals;
import org.junit.Assert;

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
    public void queryReturnsExpectedEventsInIterator() throws Exception {
        EventStoreClass store = defaultStore();
        EventIterator it = store.query("typeA", 12L, 35L);

        it.moveNext();
        assertEquals(isEventEqual(it.current(), new Event("typeA", 20L)), true);
        it.moveNext();
        assertEquals(isEventEqual(it.current(), new Event("typeA", 30L)), true);
        it.moveNext();
        assertEquals(isEventEqual(it.current(), new Event("typeA", 32L)), true);
    }

    @Test
    public void queryIteratorMoveNextReturnsFalseWhenOver() throws Exception {
        EventStoreClass store = defaultStore();
        EventIterator it = store.query("typeA", 12L, 22L);

        boolean start = it.moveNext();
        assertEquals(start, true);
        boolean end = it.moveNext();
        assertEquals(end, false);
    }

    @Test
    public void iteratorCurrentIllegalStateException() {
        EventStoreClass store = defaultStore();
        EventIterator it = store.query("typeA", 12L, 22L);

        try {
            it.current();
            Assert.fail();
        } catch (IllegalStateException e) {
        }
    }

    @Test
    public void iteratorRemoveIllegalStateException() {
        EventStoreClass store = defaultStore();
        EventIterator it = store.query("typeA", 12L, 22L);

        try {
            it.remove();
            Assert.fail();
        } catch (IllegalStateException e) {
        }
    }

    @Test
    public void queryIteratorRemovesValue() throws Exception {
        EventStoreClass store = defaultStore();
        EventIterator it = store.query("typeA", 12L, 35L);

        it.moveNext();
        it.remove();
        ConcurrentSkipListSet<Event> events = store.events.get("typeA");
        Iterator<Event> it2 = events.iterator();

        assertEquals(isEventEqual(it2.next(), new Event("typeA", 10L)), true);
        assertEquals(isEventEqual(it2.next(), new Event("typeA", 30L)), true);
        assertEquals(isEventEqual(it2.next(), new Event("typeA", 32L)), true);
        assertEquals(isEventEqual(it2.next(), new Event("typeA", 50L)), true);
    }

    @Test
    public void queryNoResultInTimePeriod() throws Exception {
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

    private boolean isEventEqual(Event a, Event b){
        if(a.timestamp() == b.timestamp() && a.type() == b.type()){
            return true;
        }
            return false;
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

package net.intelie.challenges;

import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListSet;

public class EventIteratorClass implements EventIterator{

    private final ConcurrentSkipListSet<Event> query_events;
    private Iterator<Event> iterator;
    private Event current = null;

    public EventIteratorClass(ConcurrentSkipListSet<Event> query_events) {
        this.query_events = query_events;
        this.iterator = query_events.iterator();
    }

    @Override
    public boolean moveNext() {
        if (iterator == null)
            return false;
        current = iterator.hasNext() ? iterator.next() : null;
        return current != null; // returns true if current is not null (there is a next)
    }

    @Override
    public Event current() {
        if (isIlegal())
            throw new IllegalStateException();
        return current;
    }

    @Override
    public void remove() {
        if (isIlegal())
            throw new IllegalStateException();
        query_events.remove(current);
    }

    @Override
    public void close() throws Exception {
        iterator = null;
    }

    private boolean isIlegal(){
        return current == null || !iterator.hasNext() || iterator == null;
    }
}

package dev.rtbot.sql;

import org.junit.Test;
import static org.junit.Assert.*;

import java.util.Map;

public class StringDictionaryTest {

    @Test
    public void encodeAssignsSequentialIds() {
        StringDictionary dict = new StringDictionary();
        assertEquals(1.0, dict.encode("Bay A"), 1e-9);
        assertEquals(2.0, dict.encode("Bay B"), 1e-9);
        assertEquals(3.0, dict.encode("Bay C"), 1e-9);
    }

    @Test
    public void encodeReturnsSameIdForSameString() {
        StringDictionary dict = new StringDictionary();
        double id1 = dict.encode("Bay A");
        double id2 = dict.encode("Bay A");
        assertEquals(id1, id2, 1e-9);
    }

    @Test
    public void decodeReturnsOriginalString() {
        StringDictionary dict = new StringDictionary();
        double id = dict.encode("Bay A");
        assertEquals("Bay A", dict.decode(id));
    }

    @Test
    public void decodeUnknownIdReturnsNull() {
        StringDictionary dict = new StringDictionary();
        assertNull(dict.decode(999.0));
    }

    @Test
    public void lookupReturnsIdWithoutCreating() {
        StringDictionary dict = new StringDictionary();
        assertNull(dict.lookup("Bay A"));
        dict.encode("Bay A");
        Double id = dict.lookup("Bay A");
        assertNotNull(id);
        assertEquals(1.0, id, 1e-9);
    }

    @Test
    public void sizeReflectsUniqueEntries() {
        StringDictionary dict = new StringDictionary();
        assertEquals(0, dict.size());
        dict.encode("a");
        dict.encode("b");
        dict.encode("a");
        assertEquals(2, dict.size());
    }

    @Test
    public void allEntriesReturnsFullMapping() {
        StringDictionary dict = new StringDictionary();
        dict.encode("x");
        dict.encode("y");
        Map<Double, String> entries = dict.allEntries();
        assertEquals(2, entries.size());
        assertEquals("x", entries.get(1.0));
        assertEquals("y", entries.get(2.0));
    }

    @Test
    public void restoreFromEntries() {
        StringDictionary dict = new StringDictionary();
        dict.restore(Map.of(1.0, "Bay A", 2.0, "Bay B"));
        assertEquals("Bay A", dict.decode(1.0));
        assertEquals("Bay B", dict.decode(2.0));
        // Next encode should use ID 3.0
        assertEquals(3.0, dict.encode("Bay C"), 1e-9);
    }

    @Test
    public void encodeEmptyStringWorks() {
        StringDictionary dict = new StringDictionary();
        double id = dict.encode("");
        assertEquals(1.0, id, 1e-9);
        assertEquals("", dict.decode(id));
    }

    @Test
    public void encodeManyStringsDoNotCollide() {
        StringDictionary dict = new StringDictionary();
        for (int i = 0; i < 10000; i++) {
            double id = dict.encode("str_" + i);
            assertEquals((double)(i + 1), id, 1e-9);
        }
        assertEquals(10000, dict.size());
        for (int i = 0; i < 10000; i++) {
            assertEquals("str_" + i, dict.decode((double)(i + 1)));
        }
    }

    @Test
    public void toJsonAndFromJsonRoundTrip() {
        StringDictionary dict = new StringDictionary();
        dict.encode("Bay A");
        dict.encode("Bay B");
        String json = dict.toJson();
        StringDictionary restored = StringDictionary.fromJson(json);
        assertEquals(2, restored.size());
        assertEquals("Bay A", restored.decode(1.0));
        assertEquals("Bay B", restored.decode(2.0));
        assertEquals(3.0, restored.encode("Bay C"), 1e-9);
    }

    @Test
    public void putMappingAddsNewEntry() {
        StringDictionary dict = new StringDictionary();
        dict.putMapping(5.0, "hello");
        assertEquals("hello", dict.decode(5.0));
        assertEquals(Double.valueOf(5.0), dict.lookup("hello"));
        assertEquals(1, dict.size());
    }

    @Test
    public void putMappingIsIdempotentForExistingString() {
        StringDictionary dict = new StringDictionary();
        dict.encode("hello");  // gets ID 1.0
        dict.putMapping(99.0, "hello");  // should be no-op
        assertEquals(Double.valueOf(1.0), dict.lookup("hello"));
        assertEquals("hello", dict.decode(1.0));
        assertNull(dict.decode(99.0));
        assertEquals(1, dict.size());
    }

    @Test
    public void putMappingIsIdempotentForExistingId() {
        StringDictionary dict = new StringDictionary();
        dict.putMapping(1.0, "hello");
        dict.putMapping(1.0, "world");  // should be no-op — ID 1.0 already taken
        assertEquals("hello", dict.decode(1.0));
        assertNull(dict.lookup("world"));
        assertEquals(1, dict.size());
    }

    @Test
    public void putMappingAdvancesNextId() {
        StringDictionary dict = new StringDictionary();
        dict.putMapping(5.0, "hello");
        // Next encode should get ID 6.0, not 1.0
        assertEquals(6.0, dict.encode("world"), 1e-9);
    }

    @Test
    public void putMappingAfterEncodeDoesNotConflict() {
        StringDictionary dict = new StringDictionary();
        dict.encode("alpha");   // ID 1.0
        dict.encode("beta");    // ID 2.0
        dict.putMapping(3.0, "gamma");  // manual insert
        dict.encode("delta");   // should get ID 4.0
        assertEquals(Double.valueOf(4.0), dict.lookup("delta"));
    }
}

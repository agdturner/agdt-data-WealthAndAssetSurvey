/*
 * Copyright 2019 Centre for Computational Geography, University of Leeds.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.ac.leeds.ccg.andyt.generic.data.waas.data;

import java.io.Serializable;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.TreeSet;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_W2HRecord;

/**
 *
 * @author geoagdt
 */
public class WaAS_W2Data implements Serializable {

    /**
     * Keys are CASEW2 and values are WaAS_Wave2_HHOLD_Records
     */
    public TreeMap<WaAS_W2ID, WaAS_W2HRecord> lookup;
    /**
     * CASEW1 values in Wave 2 records.
     */
    public final TreeSet<WaAS_W1ID> W1InW2;
    /**
     * All CASEW2 values.
     */
    public final TreeSet<WaAS_W2ID> AllW2;
    /**
     * CASEW2 values for records that have CASEW1 values.
     */
    public final TreeSet<WaAS_W2ID> W2InW1;
    /**
     * CASEW2 values for records in waves 3, 4 and 5 and that have CASEW1
     * values.
     */
    public final TreeSet<WaAS_W2ID> W2InW1W3W4W5;

    /**
     * Keys are CASEW2 and values are CASEW1.
     */
    public final TreeMap<WaAS_W2ID, WaAS_W1ID> W2ToW1;

    /**
     * Keys are CASEW1 and values are sets of CASEW2 (which is normally expected
     * to contain just one CASEW2).
     */
    public final TreeMap<WaAS_W1ID, HashSet<WaAS_W2ID>> W1ToW2;

    public WaAS_W2Data() {
        lookup = new TreeMap<>();
        W1InW2 = new TreeSet<>();
        AllW2 = new TreeSet<>();
        W2InW1 = new TreeSet<>();
        W2InW1W3W4W5 = new TreeSet<>();
        W2ToW1 = new TreeMap<>();
        W1ToW2 = new TreeMap<>();
    }
}

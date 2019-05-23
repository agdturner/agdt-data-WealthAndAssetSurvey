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
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_W4HRecord;

/**
 *
 * @author geoagdt
 */
public class WaAS_W4Data implements Serializable {

    /**
     * Keys are CASEW4 and values are WaAS_Wave4_HHOLD_Records
     */
    public TreeMap<WaAS_W4ID, WaAS_W4HRecord> lookup;
    /**
     * CASEW1 values in Wave 4 records.
     */
    public final TreeSet<WaAS_W1ID> W1InW4;
    /**
     * CASEW2 values in Wave 4 records.
     */
    public final TreeSet<WaAS_W2ID> W2InW4;
    /**
     * w3ID values in Wave 4 records.
     */
    public final TreeSet<WaAS_W3ID> W3InW4;
    /**
     * All CASEW4 values.
     */
    public final TreeSet<WaAS_W4ID> AllW4;
    /**
     * CASEW4 values for records that have w3ID, CASEW2 and CASEW1 values.
     */
    public final TreeSet<WaAS_W4ID> W4InW1W2W3;
    /**
     * Keys are CASEW4 and values are w3ID.
     */
    public final TreeMap<WaAS_W4ID, WaAS_W3ID> W4ToW3;
    /**
     * Keys are w3ID and values are sets of CASEW4 (which is normally expected
     * to contain just one CASEW4).
     */
    public final TreeMap<WaAS_W3ID, HashSet<WaAS_W4ID>> W3ToW4;

    public WaAS_W4Data() {
        lookup = new TreeMap<>();
        W1InW4 = new TreeSet<>();
        W2InW4 = new TreeSet<>();
        W3InW4 = new TreeSet<>();
        AllW4 = new TreeSet<>();
        W4InW1W2W3 = new TreeSet<>();
        W4ToW3 = new TreeMap<>();
        W3ToW4 = new TreeMap<>();
    }
}

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
package uk.ac.leeds.ccg.data.waas.data;

import uk.ac.leeds.ccg.data.waas.data.records.WaAS_W2Record;
import uk.ac.leeds.ccg.data.waas.data.id.WaAS_W1ID;
import uk.ac.leeds.ccg.data.waas.data.id.WaAS_W2ID;
import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * WaAS_W2Data
 *
 * @author Andy Turner
 * @version 1.0.0
 */
public class WaAS_W2Data implements Serializable {

    /**
     * Keys are CASEW2 IDs and values are WaAS_W2Record
     */
    public final Map<WaAS_W2ID, WaAS_W2Record> lookup;
    /**
     * CASEW1 in Wave 2 records.
     */
    public final Set<WaAS_W1ID> w1_In_w2;
    /**
     * All CASEW2 values.
     */
    public final Set<WaAS_W2ID> all;
    /**
     * CASEW2 IDs for records that have CASEW1.
     */
    public final Set<WaAS_W2ID> w2_In_w1;

    /**
     * Keys are CASEW2 and values are CASEW1.
     */
    public final Map<WaAS_W2ID, WaAS_W1ID> w2_To_w1;

    /**
     * Keys are CASEW1 and values are sets of CASEW2 (normally size one).
     */
    public final Map<WaAS_W1ID, Set<WaAS_W2ID>> w1_To_w2;

    public WaAS_W2Data() {
        lookup = new TreeMap<>();
        w1_In_w2 = new TreeSet<>();
        all = new TreeSet<>();
        w2_In_w1 = new TreeSet<>();
        w2_To_w1 = new TreeMap<>();
        w1_To_w2 = new TreeMap<>();
    }
}

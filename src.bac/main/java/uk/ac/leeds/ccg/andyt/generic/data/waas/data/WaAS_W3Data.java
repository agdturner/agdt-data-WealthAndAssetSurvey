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

import uk.ac.leeds.ccg.andyt.generic.data.waas.data.records.WaAS_W3Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.id.WaAS_W1ID;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.id.WaAS_W2ID;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.id.WaAS_W3ID;
import java.io.Serializable;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 *
 * @author geoagdt
 */
public class WaAS_W3Data  implements Serializable {

        /**
         * Keys are w3ID and values are WaAS_W3Record
         */
        public final TreeMap<WaAS_W3ID, WaAS_W3Record> lookup;
        /**
         * CASEW1 in Wave 3 records.
         */
        public final TreeSet<WaAS_W1ID> w1_In_w3;
        /**
         * CASEW2 in Wave 3 records.
         */
        public final TreeSet<WaAS_W2ID> w2_In_w3;
        /**
         * All CASEW3 IDs.
         */
        public final TreeSet<WaAS_W3ID> all;
        /**
         * CASEW3 IDs for records that have CASEW1 and CASEW2.
         */
        public final TreeSet<WaAS_W3ID> w3_In_w1w2;
        /**
         * Keys are W3 and values are W2.
         */
        public final TreeMap<WaAS_W3ID, WaAS_W2ID> w3_To_w2;
        /**
         * Keys are W2 and values are sets of W3 (normally size one).
         */
        public final TreeMap<WaAS_W2ID, HashSet<WaAS_W3ID>> w2_To_w3;

        public WaAS_W3Data() {
            lookup = new TreeMap<>();
            w1_In_w3 = new TreeSet<>();
            w2_In_w3 = new TreeSet<>();
            all = new TreeSet<>();
            w3_In_w1w2 = new TreeSet<>();
            w3_To_w2 = new TreeMap<>();
            w2_To_w3 = new TreeMap<>();
        }
    }


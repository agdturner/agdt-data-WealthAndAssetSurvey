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
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold.WaAS_W3HRecord;

/**
 *
 * @author geoagdt
 */
public class WaAS_W3Data  implements Serializable {

        /**
         * Keys are w3ID and values are WaAS_Wave3_HHOLD_Records
         */
        public TreeMap<WaAS_W3ID, WaAS_W3HRecord> lookup;
        /**
         * CASEW1 values in Wave 3 records.
         */
        public final TreeSet<WaAS_W1ID> W1InW3;
        /**
         * CASEW2 values in Wave 3 records.
         */
        public final TreeSet<WaAS_W2ID> W2InW3;
        /**
         * All w3ID values.
         */
        public final TreeSet<WaAS_W3ID> AllW3;
        /**
         * w3ID values for records that have CASEW1 and CASEW2 values.
         */
        public final TreeSet<WaAS_W3ID> W3InW1W2;
        /**
         * w3ID values for records in waves 4 and 5 and that have CASEW2 values.
         */
        public final TreeSet<WaAS_W3ID> W3InW2W4W5;
        /**
         * Keys are w3ID and values are CASEW2.
         */
        public final TreeMap<WaAS_W3ID, WaAS_W2ID> W3ToW2;
        /**
         * Keys are CASEW2 and values are sets of w3ID (which is normally
         * expected to contain just one w3ID).
         */
        public final TreeMap<WaAS_W2ID, HashSet<WaAS_W3ID>> W2ToW3;

        public WaAS_W3Data() {
            lookup = new TreeMap<>();
            W1InW3 = new TreeSet<>();
            W2InW3 = new TreeSet<>();
            AllW3 = new TreeSet<>();
            W3InW1W2 = new TreeSet<>();
            W3InW2W4W5 = new TreeSet<>();
            W3ToW2 = new TreeMap<>();
            W2ToW3 = new TreeMap<>();
        }
    }


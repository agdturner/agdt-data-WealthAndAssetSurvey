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

import uk.ac.leeds.ccg.andyt.generic.data.waas.data.records.WaAS_W1Record;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.id.WaAS_W1ID;
import java.io.Serializable;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 *
 * @author geoagdt
 */
public class WaAS_W1Data implements Serializable {

    /**
     * Keys are CASEW1 IDs and values are WaAS_W1Record
     */
    public final TreeMap<WaAS_W1ID, WaAS_W1Record> lookup;
    /**
     * All CASEW1 IDs.
     */
    public final TreeSet<WaAS_W1ID> all;

    public WaAS_W1Data() {
        lookup = new TreeMap<>();
        all = new TreeSet<>();
    }
}

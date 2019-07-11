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
package uk.ac.leeds.ccg.andyt.generic.data.waas.data.subset;

import uk.ac.leeds.ccg.andyt.generic.data.waas.data.id.WaAS_CollectionID;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.id.WaAS_W5ID;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.TreeSet;
import uk.ac.leeds.ccg.andyt.generic.data.waas.core.WaAS_Environment;

public class WaAS_DataSubsetW5 extends WaAS_DataSubset {

    /**
     * Lookup from WaAS_CollectionID to WaAS_W5ID.
     */
    public TreeMap<WaAS_CollectionID, HashSet<WaAS_W5ID>> c_To_w5;

    /**
     * Lookup from WaAS_W5ID to WaAS_CollectionID.
     */
    public HashMap<WaAS_W5ID, WaAS_CollectionID> w5_To_c;

    /**
     *
     * @param e
     * @param w5IDs
     * @param cSize
     */
    public WaAS_DataSubsetW5(WaAS_Environment e, TreeSet<WaAS_W5ID> w5IDs) {
        super(e);
        c_To_w5 = new TreeMap<>();
        w5_To_c = new HashMap<>();
        Iterator<WaAS_W5ID> ite = w5IDs.iterator();
        short s = 0;
        WaAS_CollectionID cID = e.data.cIDs.get(s);
        int i = 0;
        while (ite.hasNext()) {
            WaAS_W5ID w5ID = ite.next();
            w5_To_c.put(w5ID, cID);
            i++;
            if (i == e.data.collections.size()) {
                i = 0;
                s++;
                cID = e.data.cIDs.get(s);
            }
        }
        initCFs(WaAS_Environment.W5);
    }
}

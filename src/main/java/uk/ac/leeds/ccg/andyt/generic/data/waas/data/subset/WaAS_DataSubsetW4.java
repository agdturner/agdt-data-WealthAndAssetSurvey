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

import uk.ac.leeds.ccg.andyt.generic.data.waas.data.id.WaAS_W4ID;
import uk.ac.leeds.ccg.andyt.generic.data.waas.data.id.WaAS_CollectionID;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.TreeSet;
import uk.ac.leeds.ccg.andyt.generic.data.waas.core.WaAS_Environment;

public class WaAS_DataSubsetW4 extends WaAS_DataSubset {

    /**
     * Lookup from WaAS_CollectionID to WaAS_W4ID.
     */
    public TreeMap<WaAS_CollectionID, HashSet<WaAS_W4ID>> c_To_w4;

    /**
     * Lookup from WaAS_W4ID to WaAS_CollectionID.
     */
    public HashMap<WaAS_W4ID, WaAS_CollectionID> w4_To_c;

    /**
     *
     * @param e
     * @param w4IDs
     */
    public WaAS_DataSubsetW4(WaAS_Environment e, TreeSet<WaAS_W4ID> w4IDs) {
        super(e);
        c_To_w4 = new TreeMap<>();
        w4_To_c = new HashMap<>();
        Iterator<WaAS_W4ID> ite = w4IDs.iterator();
        short s = 0;
        WaAS_CollectionID cID = e.data.cIDs.get(s);
        int i = 0;
        while (ite.hasNext()) {
            WaAS_W4ID w4ID = ite.next();
            w4_To_c.put(w4ID, cID);
            i++;
            if (i == e.data.collections.size()) {
                i = 0;
                s++;
                cID = e.data.cIDs.get(s);
            }
        }
        initCFs(WaAS_Environment.W4);
    }
}

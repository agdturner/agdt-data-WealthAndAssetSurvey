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

import uk.ac.leeds.ccg.andyt.generic.data.waas.data.id.WaAS_W2ID;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.TreeSet;
import uk.ac.leeds.ccg.andyt.data.Data_CollectionID;
import uk.ac.leeds.ccg.andyt.generic.data.waas.core.WaAS_Environment;

public class WaAS_DataSubsetW2 extends WaAS_DataSubset {

    /**
     * Lookup from Data_CollectionID to WaAS_W2ID.
     */
    public TreeMap<Data_CollectionID, HashSet<WaAS_W2ID>> c_To_w2;

    /**
     * Lookup from WaAS_W2ID to Data_CollectionID.
     */
    public HashMap<WaAS_W2ID, Data_CollectionID> w2_To_c;

    /**
     *
     * @param e
     * @param cSize
     * @param w2IDs
     */
    public WaAS_DataSubsetW2(WaAS_Environment e, int cSize, TreeSet<WaAS_W2ID> w2IDs) {
        super(e, cSize);
        c_To_w2 = new TreeMap<>();
        w2_To_c = new HashMap<>();
        Iterator<WaAS_W2ID> ite = w2IDs.iterator();
        int i = 0;
        Data_CollectionID cID = getCollectionID(i);
        int ci = 0;
        while (ite.hasNext()) {
            WaAS_W2ID w2ID = ite.next();
            w2_To_c.put(w2ID, cID);
            ci++;
            if (ci == cSize) {
                ci = 0;
                i++;
                cID = getCollectionID(i);
            }
        }
        initCFs(we.W2);
    }
}

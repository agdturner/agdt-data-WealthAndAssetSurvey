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

import java.io.File;
import java.util.TreeMap;
import uk.ac.leeds.ccg.andyt.data.Data_CollectionID;
import uk.ac.leeds.ccg.andyt.generic.data.waas.core.WaAS_Environment;
import uk.ac.leeds.ccg.andyt.generic.data.waas.core.WaAS_Object;
import uk.ac.leeds.ccg.andyt.generic.data.waas.core.WaAS_Strings;

public class WaAS_DataSubset extends WaAS_Object {

    /**
     * For storing the normal number of things in a collection
     */
    public final int cSize;
    
    /**
     * Collection Files
     */
    public TreeMap<Data_CollectionID, File> cFs;

    /**
     * Create a new DataSubset
     *
     * @param e
     * @param cSize
     */
    public WaAS_DataSubset(WaAS_Environment e, int cSize) {
        super(e);
        this.cSize = cSize;
    }
    
    /**
     *
     * @param wave
     */
    public final void initCFs(byte wave) {
        cFs = new TreeMap<>();
        for (int i = 0; i < we.data.nOC; i++) {
            File f = new File(we.files.getGeneratedWaASSubsetsDir(),
                    WaAS_Strings.s_Data + WaAS_Strings.s_Subset + wave
                    + WaAS_Strings.symbol_underscore + i + ".tab");
            Data_CollectionID cID = getCollectionID(i);
//            env.log("s " + s);
//            env.log("File " + f);
//            env.log("Data_CollectionID " + cID);
            cFs.put(cID, f);
        }
    }
    
    protected final Data_CollectionID getCollectionID(int i) {
        Data_CollectionID r = we.data.cIDs.get(i);
        if (r == null) {
            env.log("No existing collection for int " + i);
            r = new Data_CollectionID(i);
            we.data.cIDs.put(i, r);
        }
        return r;
    }
}

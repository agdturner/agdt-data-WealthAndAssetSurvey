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
import java.io.File;
import java.util.TreeMap;
import uk.ac.leeds.ccg.andyt.generic.data.waas.core.WaAS_Environment;
import uk.ac.leeds.ccg.andyt.generic.data.waas.core.WaAS_Object;

public class WaAS_DataSubset extends WaAS_Object {

    /**
     * For storing the normal number of things in a collection
     */
    public final int cSize;
    
    /**
     * Collection Files
     */
    public TreeMap<WaAS_CollectionID, File> cFs;

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
        for (short s = 0; s < env.data.nOC; s++) {
            File f = new File(env.files.getGeneratedWaASSubsetsDir(),
                    env.strings.s_Data + env.strings.s_Subset + wave
                    + env.strings.symbol_underscore + s + ".tab");
            WaAS_CollectionID cID = getCollectionID(s);
//            env.log("s " + s);
//            env.log("File " + f);
//            env.log("WaAS_CollectionID " + cID);
            cFs.put(cID, f);
        }
    }
    
    protected final WaAS_CollectionID getCollectionID(short s) {
        WaAS_CollectionID r = env.data.cIDs.get(s);
        if (r == null) {
            env.log("No existing collection for short " + s);
            r = new WaAS_CollectionID(s);
            env.data.cIDs.put(s, r);
        }
        return r;
    }
}

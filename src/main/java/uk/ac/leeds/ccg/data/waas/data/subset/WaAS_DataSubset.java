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
package uk.ac.leeds.ccg.data.waas.data.subset;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.TreeMap;
import uk.ac.leeds.ccg.data.waas.core.WaAS_Environment;
import uk.ac.leeds.ccg.data.waas.core.WaAS_Object;
import uk.ac.leeds.ccg.data.waas.core.WaAS_Strings;
import uk.ac.leeds.ccg.data.waas.data.id.WaAS_CollectionID;
import uk.ac.leeds.ccg.generic.io.Generic_Path;

/**
 * WaAS_DataSubset
 * 
 * @author Andy Turner
 * @version 1.0.0
 */
public class WaAS_DataSubset extends WaAS_Object {

    /**
     * For storing the normal number of things in a collection
     */
    public final int cSize;
    
    /**
     * Collection Files
     */
    public TreeMap<WaAS_CollectionID, Generic_Path> cFs;

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
     * @throws java.io.IOException
     */
    public final void initCFs(byte wave) throws IOException {
        cFs = new TreeMap<>();
        for (int i = 0; i < we.data.nOC; i++) {
            Path f = Paths.get(we.files.getGeneratedWaASSubsetsDir().toString(),
                    WaAS_Strings.s_Data + WaAS_Strings.s_Subset + wave
                    + WaAS_Strings.symbol_underscore + i + ".tab");
            WaAS_CollectionID cID = getCollectionID(i);
//            env.log("s " + s);
//            env.log("File " + f);
//            env.log("Data_CollectionID " + cID);
            cFs.put(cID, new Generic_Path(f));
        }
    }
    
    protected final WaAS_CollectionID getCollectionID(int i) {
        WaAS_CollectionID r = we.data.cIDs.get(i);
        if (r == null) {
            env.log("No existing collection for int " + i);
            r = new WaAS_CollectionID(i);
            we.data.cIDs.put(i, r);
        }
        return r;
    }
}

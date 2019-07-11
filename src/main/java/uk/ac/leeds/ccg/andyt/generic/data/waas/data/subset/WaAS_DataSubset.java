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
import java.io.Serializable;
import java.util.TreeMap;
import uk.ac.leeds.ccg.andyt.generic.data.waas.core.WaAS_Environment;
import uk.ac.leeds.ccg.andyt.generic.data.waas.core.WaAS_Object;
import uk.ac.leeds.ccg.andyt.generic.data.waas.core.WaAS_Strings;

public class WaAS_DataSubset extends WaAS_Object implements Serializable {

    /**
     * Collection Files
     */
    public TreeMap<WaAS_CollectionID, File> cFs;

    /**
     * Create a new DataSubset
     *
     * @param e
     */
    public WaAS_DataSubset(WaAS_Environment e) {
        super(e);
    }
    
    /**
     *
     * @param wave
     */
    public final void initCFs(byte wave) {
        cFs = new TreeMap<>();
        for (short s = 0; s < env.data.collections.size(); s++) {
            File f = new File(env.files.getGeneratedWaASSubsetsDir(),
                    WaAS_Strings.s_Data + WaAS_Strings.s_Subset + wave
                    + WaAS_Strings.symbol_underscore + s + ".tab");
            WaAS_CollectionID cID = new WaAS_CollectionID(s);
            cFs.put(cID, f);
        }
    }
}

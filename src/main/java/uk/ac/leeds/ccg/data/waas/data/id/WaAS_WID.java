/*
 * Copyright 2018 geoagdt.
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
package uk.ac.leeds.ccg.data.waas.data.id;

import uk.ac.leeds.ccg.data.id.Data_RecordID;

/**
 * A Record ID for a Wave to help with type safety.
 * 
 * @author Andy Turner
 * @version 1.0.0
 */
public class WaAS_WID extends Data_RecordID {

    public WaAS_WID(long l) {
        super(l);
    }
}

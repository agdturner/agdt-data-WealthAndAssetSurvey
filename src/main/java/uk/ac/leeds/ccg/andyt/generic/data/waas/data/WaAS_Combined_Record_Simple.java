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
package uk.ac.leeds.ccg.andyt.generic.data.waas.data;

/**
 *
 * @author geoagdt
 */
public class WaAS_Combined_Record_Simple extends WaAS_Record {

    public WaAS_Wave1_Record w1Record;

    public WaAS_Wave2_Record w2Record;

    public WaAS_Wave3_Record w3Record;

    public WaAS_Wave4_Record w4Record;

    public WaAS_Wave5_Record w5Record;

    public WaAS_Combined_Record_Simple(short CASEW1) {
        super(CASEW1);
        w1Record = new WaAS_Wave1_Record(CASEW1);
    }
}

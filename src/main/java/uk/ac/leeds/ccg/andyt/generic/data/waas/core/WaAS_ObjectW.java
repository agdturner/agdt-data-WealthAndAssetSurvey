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
package uk.ac.leeds.ccg.andyt.generic.data.waas.core;

import uk.ac.leeds.ccg.andyt.generic.data.waas.io.WaAS_Files;

/**
 *
 * @author geoagdt
 */
public class WaAS_ObjectW extends WaAS_Object {

    public transient WaAS_Files files;
    public static final byte W1 = 1;
    public static final byte W2 = 2;
    public static final byte W3 = 3;
    public static final byte W4 = 4;
    public static final byte W5 = 5;

    public WaAS_ObjectW(WaAS_Environment e) {
        super(e);
        files = e.files;
    }
}

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
package uk.ac.leeds.ccg.andyt.generic.data.waas.process;

import java.io.BufferedReader;
import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import uk.ac.leeds.ccg.andyt.generic.io.Generic_IO;
import uk.ac.leeds.ccg.andyt.generic.data.waas.core.WaAS_Environment;
import uk.ac.leeds.ccg.andyt.generic.data.waas.core.WaAS_Strings;
import uk.ac.leeds.ccg.andyt.generic.data.waas.io.WaAS_Files;
import uk.ac.leeds.ccg.andyt.generic.data.waas.core.WaAS_Object;

/**
 * This class produces source code for loading survey data. Source code classes
 * written in order to load the Wealth and Assets Survey (WaAS) household data
 * is written to uk.ac.leeds.ccg.andyt.generic.data.waas.data.hhold. Source
 * code classes written in order to load the WaAS person data is written to
 * uk.ac.leeds.ccg.andyt.generic.data.waas.data.person.
 *
 * As these survey data contained many variables, it was thought best to write
 * some code that wrote some code to load these data and provide access to the
 * variables. Most variables are loaded as Double types. Some such as dates have
 * been loaded as String types. There are documents:
 * data\input\WaAS\UKDA-7215-tab\mrdoc\pdf\7215_was_questionnaire_wave_1.pdf
 * data\input\WaAS\UKDA-7215-tab\mrdoc\pdf\7215_was_questionnaire_wave_2.pdf
 * data\input\WaAS\UKDA-7215-tab\mrdoc\pdf\7215_was_questionnaire_wave_3.pdf
 * data\input\WaAS\UKDA-7215-tab\mrdoc\pdf\7215_was_questionnaire_wave_4.pdf
 * data\input\WaAS\UKDA-7215-tab\mrdoc\pdf\7215_was_questionnaire_wave_5.pdf
 * that detail what values are expected from what variables. Another way to
 * create the data loading classes would be to parse this document. A thorough
 * job of exploring these data would check the data values to make sure that
 * they conformed to these schemas. This would also allow the variables to be
 * stored in the most appropriate way (e.g. as an integer, double, String, date
 * etc.).
 *
 * @author geoagdt
 */
public class WaAS_JavaCodeGenerator extends WaAS_Object {

    private static final long serialVersionUID = 1L;

    // For convenience
    public WaAS_Strings Strings;
    public WaAS_Files Files;

    protected WaAS_JavaCodeGenerator() {
        super();
        Strings = Env.Strings;
        Files = Env.Files;
    }

    public WaAS_JavaCodeGenerator(WaAS_Environment env) {
        super(env);
        Strings = Env.Strings;
        Files = Env.Files;
    }

    public static void main(String[] args) {
        File dataDir = new File(System.getProperty("user.dir"), "data");
        WaAS_JavaCodeGenerator p;
        p = new WaAS_JavaCodeGenerator(new WaAS_Environment(dataDir));
        int nwaves;
        nwaves = 5;
        String type;
        type = "hhold";
        Object[] hholdTypes;
        hholdTypes = p.getFieldTypes(type, nwaves);
        p.run(type, hholdTypes, nwaves);
        type = "person";
        Object[] personTypes;
        personTypes = p.getFieldTypes(type, nwaves);
        p.run(type, personTypes, nwaves);
    }

    /**
     * Pass through the data and works out what numeric type is best to store
     * each field in the data.
     *
     * @param type
     * @param nwaves
     * @return keys are standardised field names, value is: 0 if field is to be
     * represented by a String; 1 if field is to be represented by a double; 2
     * if field is to be represented by a int; 3 if field is to be represented
     * by a short; 4 if field is to be represented by a byte; 5 if field is to
     * be represented by a boolean.
     */
    protected Object[] getFieldTypes(String type, int nwaves) {
        Object[] r;
        r = new Object[4];
        File indir;
        File outdir;
        File generateddir;
        indir = Files.getWaASInputDir();
        generateddir = Files.getGeneratedWaASDir();
        outdir = new File(generateddir, "Subsets");
        outdir.mkdirs();
        HashMap<String, Integer>[] allFieldTypes;
        allFieldTypes = new HashMap[nwaves];
        String[][] headers;
        headers = new String[nwaves][];
        HashMap<String, Byte>[] v0ms;
        v0ms = new HashMap[nwaves];
        HashMap<String, Byte>[] v1ms;
        v1ms = new HashMap[nwaves];
        for (int w = 0; w < nwaves; w++) {
            Object[] t;
            t = loadTest(w + 1, type, indir);
            HashMap<String, Integer> fieldTypes;
            fieldTypes = new HashMap<>();
            allFieldTypes[w] = fieldTypes;
            String[] fields;
            boolean[] strings;
            boolean[] doubles;
            boolean[] ints;
            boolean[] shorts;
            boolean[] bytes;
            boolean[] booleans;
            HashMap<String, Byte> v0m;
            HashMap<String, Byte> v1m;
            fields = (String[]) t[0];
            headers[w] = fields;
            strings = (boolean[]) t[1];
            doubles = (boolean[]) t[2];
            ints = (boolean[]) t[3];
            shorts = (boolean[]) t[4];
            bytes = (boolean[]) t[5];
            booleans = (boolean[]) t[6];
            v0m = (HashMap<String, Byte>) t[7];
            v1m = (HashMap<String, Byte>) t[8];
            v0ms[w] = v0m;
            v1ms[w] = v1m;
            String field;
            for (int i = 0; i < strings.length; i++) {
                field = fields[i];
                if (strings[i]) {
                    System.out.println("" + i + " " + "String");
                    fieldTypes.put(field, 0);
                } else {
                    if (doubles[i]) {
                        System.out.println("" + i + " " + "double");
                        fieldTypes.put(field, 1);
                    } else {
                        if (ints[i]) {
                            System.out.println("" + i + " " + "int");
                            fieldTypes.put(field, 2);
                        } else {
                            if (shorts[i]) {
                                System.out.println("" + i + " " + "short");
                                fieldTypes.put(field, 3);
                            } else {
                                if (bytes[i]) {
                                    System.out.println("" + i + " " + "byte");
                                    fieldTypes.put(field, 4);
                                } else {
                                    if (booleans[i]) {
                                        System.out.println("" + i + " " + "boolean");
                                        fieldTypes.put(field, 5);
                                    } else {
                                        try {
                                            throw new Exception("unrecognised type");
                                        } catch (Exception ex) {
                                            ex.printStackTrace(System.err);
                                            Logger.getLogger(WaAS_JavaCodeGenerator.class.getName()).log(Level.SEVERE, null, ex);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        Iterator<String> ite;
        String field;
        int fieldType;
        int consolidatedFieldType;
        HashMap<String, Integer> consolidatedFieldTypes;
        consolidatedFieldTypes = new HashMap<>();
        consolidatedFieldTypes.putAll(allFieldTypes[0]);
        for (int w = 1; w < nwaves; w++) {
            HashMap<String, Integer> fieldTypes;
            fieldTypes = allFieldTypes[w];
            ite = fieldTypes.keySet().iterator();
            while (ite.hasNext()) {
                field = ite.next();
                fieldType = fieldTypes.get(field);
                if (consolidatedFieldTypes.containsKey(field)) {
                    consolidatedFieldType = consolidatedFieldTypes.get(field);
                    if (fieldType != consolidatedFieldType) {
                        consolidatedFieldTypes.put(field,
                                Math.min(fieldType, consolidatedFieldType));
                    }
                } else {
                    consolidatedFieldTypes.put(field, fieldType);
                }
            }
        }
        r[0] = consolidatedFieldTypes;
        r[1] = headers;
        r[2] = v0ms;
        r[3] = v1ms;
        return r;
    }

    /**
     *
     * @param wave
     * @param TYPE
     * @param indir
     * @return
     */
    public Object[] loadTest(int wave, String TYPE, File indir) {
        Object[] r;
        r = new Object[9];
        String[] fields;
        boolean[] strings;
        boolean[] doubles;
        boolean[] ints;
        boolean[] shorts;
        boolean[] bytes;
        boolean[] booleans;
        HashMap<String, Byte> v0m;
        HashMap<String, Byte> v1m;
        v0m = new HashMap<>();
        v1m = new HashMap<>();
        byte[] v0;
        byte[] v1;

        File f;
        f = getInputFile(wave, TYPE, indir);
        System.out.println("<Test load wave " + wave + " " + TYPE + " WaAS "
                + "data from " + f + ">");
        BufferedReader br;
        br = Generic_IO.getBufferedReader(f);
        String line;
        int n;
        line = br.lines()
                .findFirst()
                .get();
        fields = parseHeader(line, wave);
        n = fields.length;
        strings = new boolean[n];
        doubles = new boolean[n];
        ints = new boolean[n];
        shorts = new boolean[n];
        bytes = new boolean[n];
        booleans = new boolean[n];
        v0 = new byte[n];
        v1 = new byte[n];

        for (int i = 0; i < n; i++) {
            strings[i] = false;
            doubles[i] = false;
            ints[i] = false;
            shorts[i] = false;
            //bytes[i] = true;
            bytes[i] = false;
            booleans[i] = true;
            v0[i] = Byte.MIN_VALUE;
            v1[i] = Byte.MIN_VALUE;
        }
        br.lines()
                .skip(1)
                .forEach(l -> {
                    String[] split = l.split("\t");
                    for (int i = 0; i < n; i++) {
                        parse(split[i], fields[i], i, strings, doubles, ints,
                                shorts, bytes, booleans, v0, v1, v0m, v1m);
                    }
                });
        /**
         * Order v0m and v1m so that v0m always has the smaller value and v1m
         * the larger.
         */
        Iterator<String> ite;
        ite = v0m.keySet().iterator();
        String s;
        byte v00;
        byte v11;
        while (ite.hasNext()) {
            s = ite.next();
            v00 = v0m.get(s);
            if (v1m.containsKey(s)) {
                v11 = v1m.get(s);
                if (v00 > v11) {
                    v0m.put(s, v11);
                    v1m.put(s, v00);
                }
            }
        }
        System.out.println("</Loading wave " + wave + " " + TYPE + " WaAS "
                + "data from " + f + ">");
        r[0] = fields;
        r[1] = strings;
        r[2] = doubles;
        r[3] = ints;
        r[4] = shorts;
        r[5] = bytes;
        r[6] = booleans;
        r[7] = v0m;
        r[8] = v1m;
        return r;
    }

    public File getInputFile(int wave, String type, File indir) {
        File f;
        String filename;
        filename = "was_wave_" + wave + "_" + type + "_eul_final";
        if (wave < 4) {
            filename += "_v2";
        }
        filename += ".tab";
        f = new File(indir, filename);
        return f;
    }

    /**
     * If s can be represented as a byte reserving Byte.Min_Value for a
     * noDataValue,
     *
     * @param s
     * @param field
     * @param index
     * @param strings
     * @param doubles
     * @param ints
     * @param shorts
     * @param bytes
     * @param booleans
     * @param v0
     * @param v1
     * @param v0m
     * @param v1m
     */
    public void parse(String s, String field, int index, boolean[] strings,
            boolean[] doubles, boolean[] ints, boolean[] shorts,
            boolean[] bytes, boolean[] booleans, byte[] v0, byte[] v1,
            HashMap<String, Byte> v0m, HashMap<String, Byte> v1m) {
        if (!s.trim().isEmpty()) {
            if (!strings[index]) {
                if (doubles[index]) {
                    doDouble(s, index, strings, doubles);
                } else {
                    if (ints[index]) {
                        doInt(s, index, strings, doubles, ints);
                    } else {
                        if (shorts[index]) {
                            doShort(s, index, strings, doubles, ints, shorts);
                        } else {
                            if (bytes[index]) {
                                doByte(s, index, strings, doubles, ints,
                                        shorts, bytes);
                            } else {
                                if (booleans[index]) {
                                    if (isByte(s)) {
                                        byte b = Byte.valueOf(s);
                                        if (v0[index] > Byte.MIN_VALUE) {
                                            if (!(b == v0[index])) {
                                                if (v1[index] > Byte.MIN_VALUE) {
                                                    if (!(b == v1[index])) {
                                                        booleans[index] = false;
                                                        bytes[index] = true;
                                                    }
                                                } else {
                                                    v1[index] = b;
                                                    v1m.put(field, b);
                                                }
                                            }
                                        } else {
                                            v0[index] = b;
                                            v0m.put(field, b);
                                        }
                                    } else {
                                        booleans[index] = false;
                                        shorts[index] = true;
                                        doShort(s, index, strings, doubles, ints,
                                                shorts);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    protected void doByte(String s, int index, boolean[] strings,
            boolean[] doubles, boolean[] ints, boolean[] shorts,
            boolean[] bytes) {
        if (!isByte(s)) {
            bytes[index] = false;
            shorts[index] = true;
            doShort(s, index, strings, doubles, ints, shorts);
        }
    }

    protected void doShort(String s, int index, boolean[] strings,
            boolean[] doubles, boolean[] ints, boolean[] shorts) {
        if (!isShort(s)) {
            shorts[index] = false;
            ints[index] = true;
            doInt(s, index, strings, doubles, ints);
        }

    }

    protected void doInt(String s, int index, boolean[] strings,
            boolean[] doubles, boolean[] ints) {
        if (!isInt(s)) {
            ints[index] = false;
            doubles[index] = true;
            doDouble(s, index, strings, doubles);
        }
    }

    protected void doDouble(String s, int index, boolean[] strings,
            boolean[] doubles) {
        if (!isDouble(s)) {
            doubles[index] = false;
            strings[index] = true;
        }
    }

    public boolean isByte(String s) {
        try {
            byte b = Byte.parseByte(s);
            return b > Byte.MIN_VALUE;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    public boolean isShort(String s) {
        try {
            short sh = Short.parseShort(s);
            return sh > Short.MIN_VALUE;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    public boolean isInt(String s) {
        try {
            int i = Integer.parseInt(s);
            return i > Integer.MIN_VALUE;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    public boolean isDouble(String s) {
        try {
            Double.parseDouble(s);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    public void run(String type, Object[] types, int nwaves) {
        HashMap<String, Integer> fieldTypes;
        fieldTypes = (HashMap<String, Integer>) types[0];
        String[][] headers;
        headers = (String[][]) types[1];
        HashMap<String, Byte>[] v0ms;
        v0ms = (HashMap<String, Byte>[]) types[2];
        HashMap<String, Byte>[] v1ms;
        v1ms = (HashMap<String, Byte>[]) types[3];

        TreeSet<String>[] fields;
        fields = getFields(headers);

        HashMap<String, Byte> v0m0;
        v0m0 = setCommonBooleanMaps(v0ms, v1ms, fields, fieldTypes);

        File outdir;
        outdir = new File(Files.getDataDir(), "..");
        outdir = new File(outdir, "src");
        outdir = new File(outdir, "main");
        outdir = new File(outdir, "java");
        outdir = new File(outdir, "uk");
        outdir = new File(outdir, "ac");
        outdir = new File(outdir, "leeds");
        outdir = new File(outdir, "ccg");
        outdir = new File(outdir, "andyt");
        outdir = new File(outdir, "generic");
        outdir = new File(outdir, "data");
        outdir = new File(outdir, "waas");
        outdir = new File(outdir, "data");
        outdir = new File(outdir, type);
        outdir.mkdirs();
        String packageName;
        packageName = "uk.ac.leeds.ccg.andyt.generic.data.waas.data.";
        packageName += type;

        File fout;
        PrintWriter pw;
        int wave;
        String className;
        String extendedClassName;
        String prepend;
        prepend = "WaAS_";
        type = type.toUpperCase();

        for (int w = 0; w < fields.length; w++) {
            if (w < nwaves) {
                // Non-abstract classes
                wave = w + 1;
                HashMap<String, Byte> v0m;
                v0m = v0ms[w];
                className = prepend + "Wave" + wave + "_" + type + "_Record";
                fout = new File(outdir, className + ".java");
                pw = Generic_IO.getPrintWriter(fout, false);
                writeHeaderPackageAndImports(pw, packageName, "");
                switch (w) {
                    case 0:
                        extendedClassName = prepend + "Wave1Or2_" + type + "_Record";
                        break;
                    case 1:
                        extendedClassName = prepend + "Wave1Or2_" + type + "_Record";
                        break;
                    case 2:
                        extendedClassName = prepend + "Wave3Or4Or5_" + type + "_Record";
                        break;
                    case 3:
                        extendedClassName = prepend + "Wave4Or5_" + type + "_Record";
                        break;
                    case 4:
                        extendedClassName = prepend + "Wave4Or5_" + type + "_Record";
                        break;
                    default:
                        extendedClassName = "";
                        break;
                }
                printClassDeclarationSerialVersionUID(pw, packageName,
                        className, "", extendedClassName);
                // Print Field Declarations Inits And Getters
                printFieldDeclarationsInitsAndGetters(pw, fields[w], fieldTypes,
                        v0m);
                // Constructor
                pw.println("public " + className + "(String line) {");
                pw.println("s = line.split(\"\\t\");");
                for (int j = 0; j < headers[w].length; j++) {
                    pw.println("init" + headers[w][j] + "(s[" + j + "]);");
                }
                pw.println("}");
                pw.println("}");
                pw.close();
            } else {
                // Abstract classes
                pw = null;
                if (w == nwaves) {
                    className = prepend + "Wave1Or2Or3Or4Or5_" + type + "_Record";
                    fout = new File(outdir, className + ".java");
                    pw = Generic_IO.getPrintWriter(fout, false);
                    writeHeaderPackageAndImports(pw, packageName,
                            "java.io.Serializable");
                    printClassDeclarationSerialVersionUID(pw, packageName,
                            className, "Serializable", "");
                    pw.println("protected String[] s;");
                } else if (w == (nwaves + 1)) {
                    className = prepend + "Wave1Or2_" + type + "_Record";
                    fout = new File(outdir, className + ".java");
                    pw = Generic_IO.getPrintWriter(fout, false);
                    writeHeaderPackageAndImports(pw, packageName, "");
                    extendedClassName = prepend + "Wave1Or2Or3Or4Or5_" + type + "_Record";
                    printClassDeclarationSerialVersionUID(pw, packageName,
                            className, "", extendedClassName);
                } else if (w == (nwaves + 2)) {
                    className = prepend + "Wave3Or4Or5_" + type + "_Record";
                    fout = new File(outdir, className + ".java");
                    pw = Generic_IO.getPrintWriter(fout, false);
                    writeHeaderPackageAndImports(pw, packageName, "");
                    extendedClassName = prepend + "Wave1Or2Or3Or4Or5_" + type + "_Record";
                    printClassDeclarationSerialVersionUID(pw, packageName,
                            className, "", extendedClassName);
                } else if (w == (nwaves + 3)) {
                    className = prepend + "Wave4Or5_" + type + "_Record";
                    fout = new File(outdir, className + ".java");
                    pw = Generic_IO.getPrintWriter(fout, false);
                    writeHeaderPackageAndImports(pw, packageName, "");
                    extendedClassName = prepend + "Wave3Or4Or5_" + type + "_Record";
                    printClassDeclarationSerialVersionUID(pw, packageName,
                            className, "", extendedClassName);
                }
                // Print Field Declarations Inits And Getters
                printFieldDeclarationsInitsAndGetters(pw, fields[w], fieldTypes, v0m0);
                pw.println("}");
                pw.close();
            }
        }

    }

    /**
     *
     * @param pw
     * @param packageName
     * @param imports
     */
    public void writeHeaderPackageAndImports(PrintWriter pw,
            String packageName, String imports) {
        pw.println("/**");
        pw.println(" * Source code generated by " + this.getClass().getName());
        pw.println(" */");
        pw.println("package " + packageName + ";");
        if (!imports.isEmpty()) {
            pw.println("import " + imports + ";");
        }
    }

    /**
     *
     * @param pw
     * @param packageName
     * @param className
     * @param implementations
     * @param extendedClassName
     */
    public void printClassDeclarationSerialVersionUID(PrintWriter pw,
            String packageName, String className, String implementations,
            String extendedClassName) {
        pw.print("public class " + className);
        if (!extendedClassName.isEmpty()) {
            pw.print(" extends " + extendedClassName + " {");
        }
        if (!implementations.isEmpty()) {
            pw.print(" implements " + implementations + " {");
        }
        pw.println();
        /**
         * This is not included for performance reasons. pw.println("private
         * static final long serialVersionUID = " + serialVersionUID + ";");
         */
    }

    /**
     * @param pw
     * @param fields
     * @param fieldTypes
     * @param v0
     */
    public void printFieldDeclarationsInitsAndGetters(PrintWriter pw,
            TreeSet<String> fields, HashMap<String, Integer> fieldTypes,
            HashMap<String, Byte> v0) {
        // Field declarations
        printFieldDeclarations(pw, fields, fieldTypes);
        // Field init
        printFieldInits(pw, fields, fieldTypes, v0);
        // Field getters
        printFieldGetters(pw, fields, fieldTypes);
    }

    /**
     * @param pw
     * @param fields
     * @param fieldTypes
     */
    public void printFieldDeclarations(PrintWriter pw, TreeSet<String> fields,
            HashMap<String, Integer> fieldTypes) {
        String field;
        int fieldType;
        Iterator<String> ite;
        ite = fields.iterator();
        while (ite.hasNext()) {
            field = ite.next();
            fieldType = fieldTypes.get(field);
            switch (fieldType) {
                case 0:
                    pw.println("protected String " + field + ";");
                    break;
                case 1:
                    pw.println("protected double " + field + ";");
                    break;
                case 2:
                    pw.println("protected int " + field + ";");
                    break;
                case 3:
                    pw.println("protected short " + field + ";");
                    break;
                case 4:
                    pw.println("protected byte " + field + ";");
                    break;
                default:
                    pw.println("protected boolean " + field + ";");
                    break;
            }
        }
    }

    /**
     *
     * @param pw
     * @param fields
     * @param fieldTypes
     */
    public void printFieldGetters(PrintWriter pw, TreeSet<String> fields,
            HashMap<String, Integer> fieldTypes) {
        String field;
        int fieldType;
        Iterator<String> ite;
        ite = fields.iterator();
        while (ite.hasNext()) {
            field = ite.next();
            fieldType = fieldTypes.get(field);
            switch (fieldType) {
                case 0:
                    pw.println("public String get" + field + "() {");
                    break;
                case 1:
                    pw.println("public double get" + field + "() {");
                    break;
                case 2:
                    pw.println("public int get" + field + "() {");
                    break;
                case 3:
                    pw.println("public short get" + field + "() {");
                    break;
                case 4:
                    pw.println("public byte get" + field + "() {");
                    break;
                default:
                    pw.println("public boolean get" + field + "() {");
                    break;
            }
            pw.println("return " + field + ";");
            pw.println("}");
            pw.println();
        }
    }

    /**
     *
     * @param pw
     * @param fields
     * @param fieldTypes
     * @param v0
     */
    public void printFieldInits(PrintWriter pw, TreeSet<String> fields,
            HashMap<String, Integer> fieldTypes, HashMap<String, Byte> v0) {
        String field;
        int fieldType;
        Iterator<String> ite;
        ite = fields.iterator();
        while (ite.hasNext()) {
            field = ite.next();
            fieldType = fieldTypes.get(field);
            switch (fieldType) {
                case 0:
                    pw.println("protected final void init" + field + "(String s) {");
                    pw.println("if (!s.trim().isEmpty()) {");
                    pw.println(field + " = s;");
                    break;
                case 1:
                    pw.println("protected final void init" + field + "(String s) {");
                    pw.println("if (!s.trim().isEmpty()) {");
                    pw.println(field + " = Double.parseDouble(s);");
                    pw.println("} else {");
                    pw.println(field + " = Double.NaN;");
                    break;
                case 2:
                    pw.println("protected final void init" + field + "(String s) {");
                    pw.println("if (!s.trim().isEmpty()) {");
                    pw.println(field + " = Integer.parseInt(s);");
                    pw.println("} else {");
                    pw.println(field + " = Integer.MIN_VALUE;");
                    break;
                case 3:
                    pw.println("protected final void init" + field + "(String s) {");
                    pw.println("if (!s.trim().isEmpty()) {");
                    pw.println(field + " = Short.parseShort(s);");
                    pw.println("} else {");
                    pw.println(field + " = Short.MIN_VALUE;");
                    break;
                case 4:
                    pw.println("protected final void init" + field + "(String s) {");
                    pw.println("if (!s.trim().isEmpty()) {");
                    pw.println(field + " = Byte.parseByte(s);");
                    pw.println("} else {");
                    pw.println(field + " = Byte.MIN_VALUE;");
                    break;
                default:
                    pw.println("protected final void init" + field + "(String s) {");
                    pw.println("if (!s.trim().isEmpty()) {");
                    pw.println("byte b = Byte.parseByte(s);");
                    if (v0.get(field) == null) {
                        pw.println(field + " = false;");
                    } else {
                        pw.println("if (b == " + v0.get(field) + ") {");
                        pw.println(field + " = false;");
                        pw.println("} else {");
                        pw.println(field + " = true;");
                        pw.println("}");
                    }
                    break;
            }
            pw.println("}");
            pw.println("}");
            pw.println();
        }
    }

    /**
     * Thinking to returns a lists of IDs...
     *
     * @param header
     * @param wave
     * @return
     */
    public String[] parseHeader(String header, int wave) {
        String[] r;
        String ws;
        ws = "W" + wave;
        String keyIdentifier1;
        keyIdentifier1 = "CASE" + ws;
        String keyIdentifier2;
        keyIdentifier2 = "PERSON" + ws;
        String uniqueString1;
        uniqueString1 = "uniqueString1";
        String uniqueString2;
        uniqueString2 = "uniqueString2";
        String h1;
        h1 = header.toUpperCase();
        try {
            if (h1.contains(uniqueString1)) {
                throw new Exception(uniqueString1 + " is not unique!");
            }
            if (h1.contains(uniqueString2)) {
                throw new Exception(uniqueString2 + " is not unique!");
            }
        } catch (Exception ex) {
            Logger.getLogger(WaAS_JavaCodeGenerator.class.getName()).log(Level.SEVERE, null, ex);
        }
        h1 = h1.replaceAll("\t", " ,");
        h1 = h1 + " ";
        h1 = h1.replaceAll(keyIdentifier1, uniqueString1);
        h1 = h1.replaceAll(keyIdentifier2, uniqueString2);
        h1 = h1.replaceAll(ws + " ", " ");
        h1 = h1.replaceAll(" " + ws, " ");
        h1 = h1.replaceAll(ws + "_", "_");
        h1 = h1.replaceAll("_" + ws, "_");
        h1 = h1.replaceAll(ws + " ", "___" + ws + " ");
        h1 = h1.trim();
        h1 = h1.replaceAll(" ,", "\t");
        h1 = h1.replaceAll(uniqueString1, keyIdentifier1);
        h1 = h1.replaceAll(uniqueString2, keyIdentifier2);
        r = h1.split("\t");
        return r;
    }

    protected HashMap<String, Byte> setCommonBooleanMaps(
            HashMap<String, Byte>[] v0ms,
            HashMap<String, Byte>[] v1ms,
            TreeSet<String>[] allFields,
            HashMap<String, Integer> fieldTypes) {
        HashMap<String, Byte> v0m;
        HashMap<String, Byte> v1m;
        Iterator<String> ites0;
        Iterator<String> ites1;
        String field0;
        String field1;
        TreeSet<String> fields;
        fields = allFields[5];
        HashMap<String, Byte> v0m1;
        HashMap<String, Byte> v1m1;
        v0m1 = new HashMap<>();
        v1m1 = new HashMap<>();
        ites0 = fields.iterator();
        byte v0;
        Byte v1;
        Byte v01;
        Byte v11;
        while (ites0.hasNext()) {
            field0 = ites0.next();
            if (fieldTypes.get(field0) == 5) {
                for (int w = 0; w < v0ms.length; w++) {
                    v0m = v0ms[w];
                    v1m = v1ms[w];
                    ites1 = v0m.keySet().iterator();
                    while (ites1.hasNext()) {
                        field1 = ites1.next();
                        if (field0.equalsIgnoreCase(field1)) {
                            v0 = v0m.get(field1);
                            if (v1m == null) {
                                v1 = Byte.MIN_VALUE;
                            } else {
                                //System.out.println("field1 " + field1);
                                //System.out.println("field1 " + field1);
                                v1 = v1m.get(field1);
                                if (v1 == null) {
                                    v1 = Byte.MIN_VALUE;
                                }
                            }
                            v01 = v0m1.get(field1);
                            v11 = v1m1.get(field1);
                            if (v01 == null) {
                                v0m1.put(field1, v0);
                            } else {
                                if (v01 != v0) {
                                    // Field better stored as a byte than boolean.
                                    fieldTypes.put(field1, 4);
                                }
                                if (v11 == null) {
                                    v1m1.put(field1, v1);
                                } else {
                                    if (v1 != v11.byteValue()) {
                                        // Field better stored as a byte than boolean.
                                        fieldTypes.put(field1, 4);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return v0m1;
    }

    /**
     * Finds and returns r where. r[0] are the fields in common with all waves.
     * r[1] are the fields in common with all waves. r[2] are the fields in
     * common with all waves. r[3] are the fields in common with all waves. r[4]
     * are the fields in common with all waves. r[5] fields common to waves 1,
     * 2, 3, 4 and 5 (12345) r[6] fields other than 12345 that are common to
     * waves 1 and 2 (12). r[7] fields other than 12345 that are in common to
     * waves 3, 4 and 5 (345) r[8] fields other than 345 that are in common to
     * waves 4 and 5 (45)
     *
     * @param headers
     * @return
     */
    public TreeSet<String>[] getFields(String[][] headers) {
        TreeSet<String>[] r;
        int size;
        size = headers.length;
        r = new TreeSet[(size * 2) - 1];
        for (int i = 0; i < 5; i++) {
            r[i] = getFields(headers[i]);
        }
        // Get fields common to waves 1, 2, 3, 4 and 5 (12345)
        r[5] = getFieldsInCommon(r[0], r[1], r[2], r[3], r[4]);
        System.out.println("Number of fields common to waves 1, 2, 3, 4 and 5 (12345) " + r[5].size());
        // Get fields other than 12345 that are common to waves 1 and 2 (12)
        r[6] = getFieldsInCommon(r[0], r[1], null, null, null);
        r[6].removeAll(r[5]);
        System.out.println("Number of fields other than 12345 that are common to waves 1 and 2 (12) " + r[6].size());
        // Get fields other than 12345 that are in common to waves 3, 4 and 5 (345)
        r[7] = getFieldsInCommon(r[2], r[3], r[4], null, null);
        r[7].removeAll(r[5]);
        System.out.println("Number of fields other than 12345 that are in common to waves 3, 4 and 5 (345) " + r[7].size());
        // Get fields other than 345 that are in common to waves 4 and 5 (45)
        r[8] = getFieldsInCommon(r[3], r[4], null, null, null);
        r[8].removeAll(r[5]);
        r[8].removeAll(r[7]);
        System.out.println("Number of fields other than 345 that are in common to waves 4 and 5 (45) " + r[8].size());
        r[0].removeAll(r[5]);
        r[0].removeAll(r[6]);
        r[1].removeAll(r[5]);
        r[1].removeAll(r[6]);
        r[2].removeAll(r[5]);
        r[2].removeAll(r[7]);
        r[3].removeAll(r[5]);
        r[3].removeAll(r[7]);
        r[3].removeAll(r[8]);
        r[4].removeAll(r[5]);
        r[4].removeAll(r[7]);
        r[4].removeAll(r[8]);
        return r;
    }

    /**
     * Finds and returns those fields that are in common and those fields .
     * result[0] are the fields in common with all.
     *
     * @param headers
     * @return
     */
    public ArrayList<String>[] getFieldsList(ArrayList<String> headers) {
        ArrayList<String>[] r;
        int size;
        size = headers.size();
        r = new ArrayList[size];
        Iterator<String> ite;
        ite = headers.iterator();
        int i;
        i = 0;
        while (ite.hasNext()) {
            r[i] = getFieldsList(ite.next());
            i++;
        }
        return r;
    }

    /**
     *
     * @param fields
     * @return
     */
    public TreeSet<String> getFields(String[] fields) {
        TreeSet<String> r;
        r = new TreeSet<>();
        r.addAll(Arrays.asList(fields));
        return r;
    }

    /**
     *
     * @param s
     * @return
     */
    public ArrayList<String> getFieldsList(String s) {
        ArrayList<String> r;
        r = new ArrayList<>();
        String[] split;
        split = s.split("\t");
        r.addAll(Arrays.asList(split));
        return r;
    }

    /**
     * Returns all the values common to s1, s2, s3, s4 and s5 and removes all
     * these common fields from s1, s2, s3, s4 and s5.
     *
     * @param s1
     * @param s2
     * @param s3 May be null.
     * @param s4 May be null.
     * @param s5 May be null.
     * @return
     * @Todo generalise
     */
    public TreeSet<String> getFieldsInCommon(TreeSet<String> s1,
            TreeSet<String> s2, TreeSet<String> s3, TreeSet<String> s4,
            TreeSet<String> s5) {
        TreeSet<String> result;
        result = new TreeSet<>();
        result.addAll(s1);
        result.retainAll(s2);
        if (s3 != null) {
            result.retainAll(s3);
        }
        if (s4 != null) {
            result.retainAll(s4);
        }
        if (s5 != null) {
            result.retainAll(s5);
        }
        return result;
    }
}

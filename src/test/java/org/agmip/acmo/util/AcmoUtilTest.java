package org.agmip.acmo.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import static org.junit.Assert.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.agmip.util.MapUtil;
import org.agmip.ace.util.AcePathfinderUtil;
import org.agmip.acmo.util.AcmoUtil;

public class AcmoUtilTest {
    private static final Logger log = LoggerFactory.getLogger(AcmoUtilTest.class);
    private HashMap<String, Object> coreMap = new HashMap<String, Object>();

    @Before
    public void populateCoreMap() {
        AcePathfinderUtil.insertValue(coreMap, "pdate", "19810101");
        AcePathfinderUtil.insertValue(coreMap, "crid", "MAZ");
        AcePathfinderUtil.insertValue(coreMap, "cul_name", "OBATAMPA");
        AcePathfinderUtil.insertValue(coreMap, "dssat_cul_id", "GH0010");
    }

    @Test
    public void checkCropTest() {
        HashMap<String, String> results = AcmoUtil.extractEventData(coreMap, "dssat");
        assertEquals("Crop name invalid", "Maize", results.get("crid"));
    }

    @Test
    public void checkFertilizerTest() {
        // Create two fertilizer events and see what we get.
        AcePathfinderUtil.insertValue(coreMap, "fedate", "19810101");
        AcePathfinderUtil.insertValue(coreMap, "feamn", "100.0");
        AcePathfinderUtil.insertValue(coreMap, "fedate", "19810102");
        AcePathfinderUtil.insertValue(coreMap, "feamk", "25.2");
        AcePathfinderUtil.insertValue(coreMap, "feamn", "15.5");
        AcePathfinderUtil.insertValue(coreMap, "feamp", ".65");
        HashMap<String, String> results = AcmoUtil.extractEventData(coreMap, "dssat");

        assertEquals("Number of fertilizer applications incorrect", "2", results.get("fe_count"));
        assertEquals("Incorrect fen_tot", "115.5", results.get("fen_tot"));
        assertEquals("Incorrect fek_tot", "25.2", results.get("fek_tot"));
        assertEquals("Incorrect fep_tot", "0.65", results.get("fep_tot"));
    }

    @Test
    public void checkIrrigationTest() {
        AcePathfinderUtil.insertValue(coreMap, "idate", "19810101");
        AcePathfinderUtil.insertValue(coreMap, "irval", "25");
        AcePathfinderUtil.insertValue(coreMap, "irop", "IR004");
        AcePathfinderUtil.insertValue(coreMap, "idate", "19810105");
        AcePathfinderUtil.insertValue(coreMap, "irval", "15.2");
        AcePathfinderUtil.insertValue(coreMap, "irop", "IR004");
        AcePathfinderUtil.insertValue(coreMap, "idate", "19810215");
        AcePathfinderUtil.insertValue(coreMap, "irop", "IR003");
        AcePathfinderUtil.insertValue(coreMap, "irval", "50");
        HashMap<String, String> results = AcmoUtil.extractEventData(coreMap, "dssat");

        assertEquals("Number of irrigation events incorrect", "3", results.get("ir_count"));
        assertEquals("Incorrect ir_tot", "90.2", results.get("ir_tot"));
        assertEquals("Incorrect irop", "Sprinkler, mm|Flood, mm", results.get("irop"));
    }

    @Test
    public void checkOrganicMatterTest() {
        AcePathfinderUtil.insertValue(coreMap, "omdat", "19810101");
        AcePathfinderUtil.insertValue(coreMap, "omamt", "25");
        AcePathfinderUtil.insertValue(coreMap, "omdat", "19810202");
        AcePathfinderUtil.insertValue(coreMap, "omamt", ".25");
        HashMap<String, String> results = AcmoUtil.extractEventData(coreMap, "dssat");

        assertEquals("Total amount of OM incorrect", "25.25", results.get("omamt"));
    }

    @Test
    public void checkTillageTest() {
        AcePathfinderUtil.insertValue(coreMap, "tdate", "19810101");
        AcePathfinderUtil.insertValue(coreMap, "tiimp", "TI003");
        AcePathfinderUtil.insertValue(coreMap, "tdate", "19810202");
        AcePathfinderUtil.insertValue(coreMap, "tiimp", "TI005");
        HashMap<String, String> results = AcmoUtil.extractEventData(coreMap, "dssat");

        assertEquals("Number of tillage events incorrect", "2", results.get("ti_count"));
        assertEquals("Tillage implements incorrect", "Moldboard plow 20 cm depth|Chisel plow, straight point", results.get("tiimp"));
    }

    @Test
    public void createCsvFileTest() {
        String mode = "TEST";
        String outputCsvPath = "";
        String expected_1 = "ACMO_" + mode + ".csv";
        String expected_2 = "ACMO_" + mode + " (1).csv";

        // Test the case of no pre-existing file 
        File f1 = AcmoUtil.createCsvFile(outputCsvPath, mode);
        assertEquals(mode, expected_1, f1.getName());

        // Test the case of pre-existing file 
        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter(f1));
            bw.close();
        } catch (IOException ex) {
            assertTrue(f1.exists());
        }
        File f2 = AcmoUtil.createCsvFile(outputCsvPath, mode);
        assertEquals(mode, expected_2, f2.getName());
        f1.delete();
        f2.delete();
    }
}

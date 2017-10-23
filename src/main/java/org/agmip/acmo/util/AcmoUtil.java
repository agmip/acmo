package org.agmip.acmo.util;

import au.com.bytecode.opencsv.CSVReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.agmip.util.MapUtil;
import org.agmip.ace.LookupCodes;
import org.agmip.ace.util.AcePathfinderUtil;
import org.agmip.dome.DomeUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AcmoUtil {
    private static final Logger log = LoggerFactory.getLogger(AcmoUtil.class);
    /**
     * Do not instantiate
     */
    private AcmoUtil(){}


    /**
     * Write an ACMO file to the specified {@code outputPath} which includes
     * all the experiments in the {@code datapackage}.
     *
     * @param outputPath the directory to write the ACMO_meta.dat file.
     * @param datapackage a standard AgMIP package
     * @param destModel the destination model name
     * @param domeIdHashMap the map hold the data as [DOME ID] : [calculated hash for its content]
     *
     */
    public static void writeAcmo(String outputPath, HashMap<String, Object> datapackage, String destModel, HashMap<String, String> domeIdHashMap) {
        if (domeIdHashMap == null) {
            domeIdHashMap = new HashMap();
        }
        HashMap<String, String> wstClimIdMap = new HashMap<String, String>();
        HashMap<String, String> wstClimCatMap = new HashMap<String, String>();
        HashMap<String, String> widMap = new HashMap<String, String>();
        HashMap<String, String> sidMap = new HashMap<String, String>();
        try {
            // Make sure the outputPath exists
            File f = new File(outputPath);
            f.mkdirs();
            String fileName   = outputPath+File.separator+"ACMO_meta.dat";
            log.debug("Attempting to write {}", fileName);
            FileWriter fw     = new FileWriter(fileName);
            BufferedWriter bw = new BufferedWriter(fw);
            // Index the Weather Stations
            for (HashMap<String, Object> wst : MapUtil.getRawPackageContents(datapackage, "weathers")) {
                String wst_id = MapUtil.getValueOr(wst, "wst_id", "");
                wstClimIdMap.put(wst_id, MapUtil.getValueOr(wst, "clim_id", "0XXX"));
                wstClimCatMap.put(wst_id, MapUtil.getValueOr(wst, "clim_cat", ""));
                widMap.put(wst_id, MapUtil.getValueOr(wst, "wid", ""));
            }
            // Index the Soil Site
            for (HashMap<String, Object> soil : MapUtil.getRawPackageContents(datapackage, "soils")) {
                String soilId = MapUtil.getValueOr(soil, "soil_id", "");
                String sid = MapUtil.getValueOr(soil, "sid", "");
                // Fix the issue that replicated soil ID been used in the ACMO meta file which modify the original data link
                if (!sidMap.containsKey(soilId) && sidMap.containsValue(sid)) {
                    ArrayList<String> rplSoilIds = new ArrayList();
                    for (String key : sidMap.keySet()) {
                        if (sid.equals(sidMap.get(key))) {
                            if (key.length() < soilId.length()) {
                                rplSoilIds.add(soilId);
                                soilId = key;
                            } else {
                                rplSoilIds.add(key);
                            }
                        }
                    }
                    sidMap.put(soilId, sid);
                    for (String key : rplSoilIds) {
                        sidMap.put(key, soilId);
                    }
                } else {
                    sidMap.put(soilId, sid);
                }
            }

            try {
                // First write the header
                bw.write(generateAcmoHeader());
                // Then write the lines
                ArrayList<HashMap<String, Object>> experiments = MapUtil.getRawPackageContents(datapackage, "experiments");
                for (HashMap<String, Object> experiment : experiments) {
                    // get WSTID and pass the CLIM_ID from that.
                    String wstId = MapUtil.getValueOr(experiment, "wst_id", "");
                    String soilId = MapUtil.getValueOr(experiment, "soil_id", "");
                    String climId = MapUtil.getValueOr(experiment, "ctwn_clim_id", MapUtil.getValueOr(wstClimIdMap, wstId, "0XXX"));
                    String climCat = MapUtil.getValueOr(wstClimCatMap, wstId, "");
                    String quaduiVer = MapUtil.getValueOr(experiment, "quaduiVer", "");;
                    String wid = MapUtil.getValueOr(widMap, wstId, "");
                    String sid = MapUtil.getValueOr(sidMap, soilId, "");
                    if (sidMap.containsKey(sid)) {
                        soilId = sid;
                        sid = MapUtil.getValueOr(sidMap, sid, "");
                    }
                    String acmoData = extractAcmoData(experiment, destModel, domeIdHashMap, climId, climCat, wid, sid, soilId, quaduiVer);
                    log.debug("ACMO dataline: {}", acmoData);
                    bw.write(acmoData);
                    bw.write("\n");
                }
            } finally {
                // Finally close the file.
                log.debug("Attempting to close the ACMO file");
                bw.flush();
                bw.close();
                fw.close();
            }
        } catch (Exception ex) {
            log.error("Error writing ACMO_meta.dat: {}", ex.getMessage());
        }
    }

    /**
     * Extract a CSV-compatable ACMO line from a single experiment
     *
     * @param dataset a single AgMIP dataset
     * @param destModel the destination model name
     * @param domeIdHashMap the map hold the data as [DOME ID] : [calculated hash for its content]
     * @param ids including clime ID, wid and sid
     *
     * @return ACMO compatible CSV line.
     */
    public static String extractAcmoData(HashMap<String, Object> dataset, String destModel, HashMap<String, String> domeIdHashMap, String... ids) {
        ArrayList<String> acmoData = new ArrayList<String>();
        HashMap<String, Object> observed = MapUtil.getRawBucket(dataset, "observed");
        HashMap<String, String> events   = extractEventData(dataset, destModel);
        String climId = "0XXX";
        String climCat = "";
        String wid = "";
        String sid = "";
        String soil_id;
        String quaduiVer = "";
        if (ids.length > 0) {
            climId = ids[0];
        }
        if (ids.length > 1) {
            climCat = ids[1];
        }
        if (ids.length > 2) {
            wid = ids[2];
        }
        if (ids.length > 3) {
            sid = ids[3];
        }
        if (ids.length > 4) {
            soil_id = ids[4];
        } else {
            soil_id = MapUtil.getValueOr(dataset, "soil_id", "");
        }
        if (ids.length > 5) {
            quaduiVer = ids[5].trim();
        }


        /**
         * Data to get:
         * wst_id (root)
         * soil_id (root)
         * fl_lat (root)
         * fl_lon (root)
         * crid (events)
         * cul_id (events)
         * cul_name (events)
         * sdat (root)
         * pdate (events)
         * hdate (observed)
         * hwah (observed)
         * cwah (observed)
         * ir_count (events)
         * ir_tot (events)
         * irop (events)
         * fe_count (events)
         * fen_tot (events)
         * fep_tot (events)
         * fek_tot (events)
         * omamt (events)
         * ti_count (events)
         * tiimp (events)
         * crop model (passed into writeAcmo())
         */

        String fieldOverlayString = MapUtil.getValueOr(dataset, "field_overlay", "");
        String seasonalStrategyString = MapUtil.getValueOr(dataset, "seasonal_strategy", "");
        ArrayList<HashMap<String, String>> domeBases = new ArrayList();

        acmoData.add("*"); // Just an indication mark, this line stands alone
        acmoData.add(""); // Suite ID, not implemented yet
        String exname = MapUtil.getValueOr(dataset, "exname", "");
        acmoData.add(quoteMe(exname));
        String doStr = getDomeIds(dataset, "field_overlay", "field_dome_applied").toUpperCase();
        String dsStr = getDomeIds(dataset, "seasonal_strategy", "seasonal_dome_applied").toUpperCase();
        String drStr = getDomeIds(dataset, "rotational_analysis", "rotational_dome_applied").toUpperCase();
        String batStr = getDomeIds(dataset, "batch_dome", "batch_dome_applied").toUpperCase();
        String batRunNum = getDomeIds(dataset, "batch_run#", "batch_dome_applied").toUpperCase();
        acmoData.add(quoteMe(doStr));
        acmoData.add(quoteMe(dsStr));
        acmoData.add(quoteMe(drStr));
        acmoData.add(quoteMe(batStr));
        acmoData.add(quoteMe(batRunNum));
        String runNum = "1";
        if (exname.matches(".*__\\d+")) {
            runNum = exname.substring(exname.lastIndexOf("__") + 2);
        }
        acmoData.add(runNum);
        acmoData.add(quoteMe(MapUtil.getValueOr(dataset, "trt_name", "")));
        acmoData.add(quoteMe(climId));
        acmoData.add(quoteMe(climCat));
        acmoData.add("1");
        if (! seasonalStrategyString.equals("")) {
            domeBases.addAll(getDomeMetaInfos(seasonalStrategyString));
        }
        domeBases.addAll(getDomeMetaInfos(fieldOverlayString));

        String reg_id = getDomeMetaInfo(domeBases, "reg_id", "");
        String rap_id = getDomeMetaInfo(domeBases, "rap_id", "");
        String man_id = getDomeMetaInfo(domeBases, "man_id", "");

        acmoData.add(quoteMe(checkCMSeries(exname, climId, rap_id, man_id))); // CMSS
        acmoData.add(quoteMe(reg_id)); // Region
        acmoData.add(quoteMe(getDomeMetaInfo(domeBases, "stratum", ""))); // Stratum
        acmoData.add(rap_id); // RAP ID
        acmoData.add(man_id); // MAN ID
        acmoData.add("AgMIP"); // Institution
        acmoData.add(MapUtil.getValueOr(dataset, "rotation", "0"));
        String wst_id = MapUtil.getValueOr(dataset, "wst_id", "");
        if (wst_id.length() > 4) {
            wst_id = wst_id.substring(0, 4);
        }
        acmoData.add(wst_id);
        acmoData.add(soil_id);
        acmoData.add(MapUtil.getValueOr(dataset, "fl_lat", ""));
        acmoData.add(MapUtil.getValueOr(dataset, "fl_long", ""));
        acmoData.add(quoteMe(MapUtil.getValueOr(events, "crid", "")));
        acmoData.add(MapUtil.getValueOr(events, "cul_id", ""));
        acmoData.add(quoteMe(MapUtil.getValueOr(events, "cul_name", "")));
        acmoData.add(correctDateFormat(MapUtil.getValueOr(dataset, "sdat", "")));
        acmoData.add(correctDateFormat(MapUtil.getValueOr(events, "pdate", "")));
        acmoData.add(MapUtil.getValueOr(observed, "hwah", ""));
        acmoData.add(MapUtil.getValueOr(observed, "cwah", ""));
        acmoData.add(correctDateFormat(MapUtil.getValueOr(observed, "hdate", "")));
        acmoData.add(MapUtil.getValueOr(events, "ir_count", ""));
        acmoData.add(MapUtil.getValueOr(events, "ir_tot", ""));
        acmoData.add(quoteMe(MapUtil.getValueOr(events, "irop", "")));
        acmoData.add(MapUtil.getValueOr(events, "fe_count", ""));
        acmoData.add(MapUtil.getValueOr(events, "fen_tot", ""));
        acmoData.add(MapUtil.getValueOr(events, "fep_tot", ""));
        acmoData.add(MapUtil.getValueOr(events, "fek_tot", ""));
        acmoData.add(MapUtil.getValueOr(events, "omamt", ""));
        acmoData.add(MapUtil.getValueOr(events, "ti_count", ""));
        acmoData.add(quoteMe(MapUtil.getValueOr(events, "tiimp", "")));
        acmoData.add(quoteMe(MapUtil.getValueOr(dataset, "eid", ""))); // Will be generated by the database
        acmoData.add(quoteMe(wid)); // Will be generated by the database
        acmoData.add(quoteMe(sid)); // Will be generated by the database
        acmoData.add(quoteMe(getDomeHash(domeIdHashMap, doStr))); // Will be generated by the database
        acmoData.add(quoteMe(getDomeHash(domeIdHashMap, dsStr))); // Will be generated by the database
        acmoData.add(quoteMe(getDomeHash(domeIdHashMap, drStr))); // Will be generated by the database
        acmoData.add(quoteMe(getDomeHash(domeIdHashMap, batStr))); // Will be generated by the database
        acmoData.add(quoteMe("quadui=" + quaduiVer + "|acmoui="));
        acmoData.add(destModel.toUpperCase());
        return joinList(acmoData, ",");
    }

    private static ArrayList<HashMap<String, String>> getDomeMetaInfos(String domeStr) {
        ArrayList<HashMap<String, String>> ret = new ArrayList();
        String[] domes = domeStr.split("[|]");
        for (String dome : domes) {
            ret.add(DomeUtil.unpackDomeName(dome));
        }
        return ret;
    }

    private static String getDomeMetaInfo(ArrayList<HashMap<String, String>> domeBases, String metaId, String defVal) {
        String ret = "";
        for (HashMap<String, String> domeBase : domeBases) {
            ret = MapUtil.getValueOr(domeBase, metaId, "");
            if (!ret.equals("")) {
                break;
            }
        }
        if (ret.equals("")) {
            return defVal;
        } else {
            return ret;
        }
    }

    private static String getDomeIds(HashMap dataset, String domeType, String domeAppliedFlg) {
        if (MapUtil.getValueOr(dataset, domeAppliedFlg, "").equals("Y")) {
            return MapUtil.getValueOr(dataset, domeType, "");
        } else {
            return "";
        }
    }

    private static String getDomeHash(HashMap<String, String> domeIdHashMap, String domeIds) {
        ArrayList<String> domeHashs = new ArrayList();
        String[] ids = domeIds.split("\\|");
        for (String id : ids) {
            String hash = MapUtil.getValueOr(domeIdHashMap, id, "");
            if (!hash.equals("")) {
                domeHashs.add(hash);
            }
        }

        return joinList(domeHashs, "|");
    }

    /**
     * Generates the standard ACMO header (with all the variables defined)
     *
     * @return ACMO header
     */
    public static String generateAcmoHeader() {
        // Update on 2014/04/29 for ACMO template version 4.1.0
        return "!,\"ID for suite of sites or experiments\",\"Name of experiment, field test or survey\",Field Overlay (DOME) ID,Seaonal Strategy (DOME) ID,Rotational Analysis (DOME) ID,BATCH (DOME) ID,,,Treatment Name,4-character Climate ID code,Climate scenario category,Climate replication number for multiple realizations of weather data (ask Alex),Crop model simulation set,Region ID,Regional stratum identification number,RAP ID,\"Management regimen ID, for multiple management regimens per RAP\",Names of institutions involved in collection of field or survey data,\"Crop rotation indicator (=1 to indicate that this is a continuous, multi-year simulation, =0 for single year simulations)\",Weather station ID,Soil ID,Site Latitude,Site Longitude,Crop type (common name) ,Crop model-specific cultivar ID,Cultivar name,Start of simulation date,Planting date,\"Observed harvested yield, dry weight\",Observed total above-ground biomass at harvest,Observed harvest date,Total number of irrigation events,Total amount of irrigation,Type of irrigation application,Total number of fertilizer applications,Total N applied,Total P applied,Total K applied,Manure and applied oganic matter,Total number of tillage applications,\"Tillage type (hand, animal or mechanized)\",Experiment ID,Weather ID,Soil ID,DOME ID for Overlay,DOME ID for Seasonal  ,DOME ID for Rotational ,DOME ID for Batch DOME,Translator version,\"Short name of crop model used for simulations (e.g., DSSAT, APSIM, Aquacrop, STICS, etc.)\",Model name and version number of the crop model used to generate simulated outputs,\"Simulated harvest yield, dry matter\",\"Simulated above-ground biomass at harvest, dry matter\",Simulated anthesis date,Simulated maturity date,Simulated harvest date,\"Simulated leaf area index, maximum\",Total precipitation from planting to harvest,\"Simulated evapotranspiration, planting to harvest\",Simulated N uptake during season,Simulated N leached up to harvest maturity,\"Transpiration, cumulative from planting to harvest\",\"Evaporation,soil, cumulative from planting to harvest\",\"Solar radiation, average, sowing to harvest\",\"Maximum daily air temperature, average, sowing to harvest\",\"Minimum daily air temperature, average, sowing to harvest\",\"Daily air temperature, average, sowing to harvest\",\"CO2 concentration, atmospheric average over day\",\"Total number of irrigation events\",\"Total amount of irrigation\"\n!,text,text,text,text,text,text,number,number,text,code,code,number,code,code,number,code,code,text,number,text,text,decimal degrees,decimal degrees,text,text,text,yyyy-mm-dd,yyyy-mm-dd,kg/ha,kg/ha,yyyy-mm-dd,number,mm,text,number,kg[N]/ha,kg[P]/ha,kg[K]/ha,kg/ha,#,text,text,text,text,text,text,text,text,text,text,text,kg/ha,kg/ha,yyyy-mm-dd,yyyy-mm-dd,yyyy-mm-dd,m2/m2,mm,mm,kg/ha,kg/ha,mm,mm,MJ/m2.d,C,C,C,vpm,number,mm\n#,SUITE_ID,EXNAME,FIELD_OVERLAY,SEASONAL_STRATEGY,ROTATIONAL_ANALYSIS,BATCH_DOME,BATCH_RUN#,RUN#,TRT_NAME,CLIM_ID,CLIM_CAT,CLIM_REP,CMSS,REG_ID,STRATUM,RAP_ID,MAN_ID,INSTITUTION,ROTATION,WST_ID,SOIL_ID,FL_LAT,FL_LONG,CRID_text,CUL_ID,CUL_NAME,SDAT,PDATE,HWAH,CWAH,HDATE,IR#C,IR_TOT,IROP_text,FE_#,FEN_TOT,FEP_TOT,FEK_TOT,OM_TOT,TI_#,TIIMP_text,EID,WID,SID,DOID,DSID,DRID,BDID,TOOL_VERSION,CROP_MODEL,MODEL_VER,HWAH_S,CWAH_S,ADAT_S,MDAT_S,HADAT_S,LAIX_S,PRCP_S,ETCP_S,NUCM_S,NLCM_S,EPCP_S,ESCP_S,SRAA_S,TMAXA_S,TMINA_S,TAVGA_S,CO2D_S,IR#C_S,IR_TOT_S\n";
    }

    protected static HashMap<String, String> extractEventData(HashMap<String, Object> dataset, String destModel) {
        destModel = destModel.toLowerCase();
        HashMap<String, String> results = new HashMap<String, String>();
        HashMap<String, Object> management = MapUtil.getRawBucket(dataset, "management");
        ArrayList<HashMap<String, String>> events = (ArrayList<HashMap<String, String>>) MapUtil.getObjectOr(management, "events", new ArrayList<HashMap<String, String>>());
        int irrCount = 0;
        boolean isAutoIrrigation = false;
        int feCount = 0;
        int tilCount = 0;
        ArrayList<String> irop = new ArrayList<String>();
        ArrayList<String> timp = new ArrayList<String>();
        BigDecimal irrAmount, fenAmount, fekAmount, fepAmount, omAmount;
        try {
            irrAmount = new BigDecimal(0.0);
            fenAmount = new BigDecimal(0.0);
            fekAmount = new BigDecimal(0.0);
            fepAmount = new BigDecimal(0.0);
            omAmount  = new BigDecimal(0.0);
        } catch (Exception ex) {
            //Something really weird happened here. Like really really weird.
            log.error("Error converting 0.0 to a decimal, hard Java error");
            return new HashMap<String, String>();
        }
        // Process only the items needed by ACMO.

        for (HashMap<String, String> event : events) {
            // From planting, we need to extract PDATE, CUL_ID, CUL_NAME,
            // and CRID (as full text)

            String currentEvent = MapUtil.getValueOr(event, "event", "");
            log.debug("Current event: {}", event.toString());
            if (currentEvent.equals("planting")) {
                populateEventMap(results, event, "pdate", destModel);
                populateEventMap(results, event, "cul_name", destModel);

                if (event.containsKey(destModel+"_cul_id")) {
                    populateEventMap(results, event, destModel+"_cul_id", destModel);
                } else {
                    populateEventMap(results, event, "cul_id", destModel);
                }

                String crop = LookupCodes.lookupCode("crid", MapUtil.getValueOr(event, "crid", ""), "cn");
                results.put("crid", crop);
            } else if (currentEvent.equals("irrigation")) {
                String irval = MapUtil.getValueOr(event, "irval", "");
                String sIrIpCode = MapUtil.getValueOr(event, "irop", "");
                String sIrOp = LookupCodes.lookupCode("irop", sIrIpCode, "cn");
                boolean isPaddy = false;
                if (sIrIpCode.equals("IR008") || sIrIpCode.equals("IR009") || sIrIpCode.equals("IR010")) {
                    isPaddy = true;
                } else if (sIrIpCode.equals("IR011")) {
                    isAutoIrrigation = true;
                    isPaddy = true;
                }
                if (!isPaddy) {
                    irrCount++;
                    try {
                        if (!irval.equals("")) {
                            irrAmount = irrAmount.add(new BigDecimal(irval));
                        }
                    } catch (Exception ex) {
                        log.error("Error converting irrigation amount with value {}", irval);
                        continue;
                    }
                }
                
                if (! irop.contains(sIrOp)) {
                    irop.add(sIrOp);
                }
            } else if (currentEvent.equals("fertilizer")) {
                feCount++;
                String feamn = MapUtil.getValueOr(event, "feamn", "");
                String feamk = MapUtil.getValueOr(event, "feamk", "");
                String feamp = MapUtil.getValueOr(event, "feamp", "");
                log.debug("Feamn amount: {}", feamn);
                try {
                    if (!feamn.equals("")) {
                        fenAmount = fenAmount.add(new BigDecimal(feamn));
                    }
                } catch (Exception ex) {
                    log.error("Error converting fertilizer [nitrogen] with value {}", feamn);
                    continue;
                }
                log.debug(fenAmount.toString());

                try {
                    if (!feamk.equals("")) {
                        fekAmount = fekAmount.add(new BigDecimal(feamk));
                    }
                } catch (Exception ex) {
                    log.error("Error converting fertilizer [potassium] with value {}", feamn);
                    continue;
                }

                try {
                    if (!feamp.equals("")) {
                        fepAmount = fepAmount.add(new BigDecimal(feamp));
                    }
                } catch (Exception ex) {
                    log.error("Error converting fertilizer [phosphorus] with value {}", feamn);
                }
            } else if (currentEvent.equals("organic_matter")) {
                String omamt = MapUtil.getValueOr(event, "omamt", "");
                if (! omamt.equals("")) {
                    try {
                        omAmount = omAmount.add(new BigDecimal(omamt));
                    } catch (Exception ex) {
                        log.error("Error converting organic matter amount with value {}", omamt);
                    }
                }
            } else if (currentEvent.equals("tillage")) {
                tilCount++;
                String tiimp = LookupCodes.lookupCode("tiimp", MapUtil.getValueOr(event, "tiimp", ""), "ca");
                if (! timp.contains(tiimp)) {
                    timp.add(tiimp);
                }
            }
        }
        // After processing all the events, consume the results of the counters.
        if (isAutoIrrigation) {
            results.put("ir_count", "auto");
            results.put("ir_tot", "auto");
            results.put("irop", joinList(irop, "|"));
        } else if (irrCount > 0) {
            results.put("ir_count", Integer.toString(irrCount));
            results.put("ir_tot", irrAmount.toString());
            results.put("irop", joinList(irop, "|"));
        }
        if (feCount > 0) {
            results.put("fe_count", Integer.toString(feCount));
            results.put("fen_tot", fenAmount.toString());
            results.put("fek_tot", fekAmount.toString());
            results.put("fep_tot", fepAmount.toString());
        }
        String om_tot = omAmount.toString();
        if (! om_tot.equals("0")) {
            results.put("omamt", om_tot);
        }
        if (tilCount > 0) {
            results.put("ti_count", Integer.toString(tilCount));
            results.put("tiimp", joinList(timp, "|"));
        }
        log.debug("extractEventData results: {}", results.toString());
        return results;
    }

    private static void populateEventMap(HashMap<String, String> eventMap, HashMap<String, String> sourceMap, String var, String destModel) {
        String tempVar = AcePathfinderUtil.setEventDateVar(var, true);
        String value = MapUtil.getValueOr(sourceMap, tempVar, "");
        if (var.startsWith(destModel)) {
            var = var.substring(destModel.length()+1);
        }
        eventMap.put(var, value);
    }

    private static String joinList(ArrayList<String> list, String joint) {
        StringBuilder joinedList = new StringBuilder();
        for (String item : list) {
            joinedList.append(item);
            joinedList.append(joint);
        }
        if (joinedList.length() > 0)
            joinedList.setLength(joinedList.length()-1);
        return joinedList.toString();
    }

    private static String correctDateFormat(String date) {
        if (! date.equals("")) {
            StringBuilder d = new StringBuilder(date);
            d.insert(4, "-");
            d.insert(7, "-");
            return d.toString();
        } else {
            return date;
        }
    }

    private static String quoteMe(String unquoted) {
        return "\""+unquoted.replaceAll("\"", "\"\"").replaceAll("\\\\", "\\\\\\\\") +"\"";
    }

    /**
     * Generate an ACMO CSV file object with a non-repeated file name in the
     * given directory. The naming rule is as follow,
     * ACMO-[Region]-[stratum]-[climate_id]-[RAP_id]-[Management_id]-[model].csv
     *
     * @param outputCsvPath The output path for CSV file
     * @param mode The name of model which provide the model output data
     * @return The {@code File} for CSV file
     */
    public static File createCsvFile(String outputCsvPath, String mode) {
        return createCsvFile(outputCsvPath, mode, null);
    }

    /**
     * Generate an ACMO CSV file object with a non-repeated file name in the
     * given directory.
     *
     * @param outputCsvPath The output path for CSV file
     * @param model The name of model which provide the model output data
     * @param metaFilePath The path of meta data file
     * @return The {@code File} for CSV file
     */
    public static File createCsvFile(String outputCsvPath, String model, String metaFilePath) {
        if (!outputCsvPath.endsWith(File.separator) && !outputCsvPath.equals("")) {
            outputCsvPath += File.separator;
        }
        String domeInfo = "";
        if (metaFilePath != null) {
            try {
                // Read meta data
                CSVReader reader = new CSVReader(new FileReader(metaFilePath), ',', '"');
                List<String[]> metaData = reader.readAll();
                reader.close();
                // Get Title and first record
                String[] title = new String[0];
                ArrayList<String[]> dataArr = new ArrayList();
                for (int i = 0; i < metaData.size() - 1; i++) {
                    if ("#".equals(metaData.get(i)[0])) {
                        title = metaData.get(i);
                    } else if ("*".equals(metaData.get(i)[0])) {
                        dataArr.add(metaData.get(i));
                    }
                }
                // Get the position index of Region, stratum, climate ID, RAP ID and Management ID
                int region = -1;
                int crop = -1;
                int climateId = -1;
                int rapId = -1;
                int mgnId = -1;
                int field = -1;
                int seasonal = -1;
                int count = 0;
                for (int i = 0; i < title.length; i++) {
                    if ("REG_ID".equalsIgnoreCase(title[i])) {
                        region = i;
                        count++;
                    } else if ("CRID_text".equalsIgnoreCase(title[i])) {
                        crop = i;
                        count++;
                    } else if ("CLIM_ID".equalsIgnoreCase(title[i])) {
                        climateId = i;
                        count++;
                    } else if ("RAP_ID".equalsIgnoreCase(title[i])) {
                        rapId = i;
                        count++;
                    } else if ("MAN_ID".equalsIgnoreCase(title[i])) {
                        mgnId = i;
                        count++;
                    } else if ("FIELD_OVERLAY".equalsIgnoreCase(title[i])) {
                        field = i;
                        count++;
                    } else if ("SEASONAL_STRATEGY".equalsIgnoreCase(title[i])) {
                        seasonal = i;
                        count++;
                    } else {
                        continue;
                    }
                    if (count == 7) {
                        break;
                    }
                }
                // Get dome info for creating ACMO file name
                if (!dataArr.isEmpty() && region != -1 && (crop != -1 || rapId != -1 || mgnId != -1 || climateId != -1)) {
                    String str;
                    if ((str = getDomeInfoStr(dataArr.get(0), region)).equals("0-")) {
                        if (!(str = getDomeInfoStr(dataArr.get(0), seasonal)).equals("0-")) {
                        } else if (!(str = getDomeInfoStr(dataArr.get(0), field)).equals("0-")) {
                        } else {
                            str = "";
                        }
                        if (!"".equals(str)) {
                            HashMap<String, String> domeBase = DomeUtil.unpackDomeName(str);
                            str = MapUtil.getValueOr(domeBase, "reg_id", "");
                            if (!str.equals("")) {
                                str += "-";
                            }
                        }
                    } else {
                        if (!str.equals("")) {
                            domeInfo = str;
                            domeInfo += getDomeInfoStr(dataArr.get(0), crop).replaceAll(" ", "").toUpperCase();
                            domeInfo += getDomeInfoStr(dataArr.get(0), climateId);
                            domeInfo += getDomeInfoStr(dataArr, rapId);
                            domeInfo += getDomeInfoStr(dataArr, mgnId);
                        }
                    }
                }
            } catch (IOException ex) {
                domeInfo = "";
            }
        }
        // Create CSV file name
        outputCsvPath += "ACMO-" + domeInfo + model;
        File f = new File(outputCsvPath + ".csv");
        int count = 1;
        while (f.exists()) {
            f = new File(outputCsvPath + " (" + count + ").csv");
            count++;
        }

        return f;
    }

    public static String addAcmouiVersion(String line, String acmouiVer) {
        return line.replaceFirst("acmoui=", "acmoui="+acmouiVer);
    }

    private static String getDomeInfoStr(String[] data, int id) {
        if (id < 0) {
            return "0-";
        } else if (id < data.length) {
            if ("".equals(data[id])) {
                return "0-";
            } else {
                return data[id] + "-";
            }
        } else {
            return "0-";
        }
    }

    private static String getDomeInfoStr(ArrayList<String[]> dataArr, int id) {
        if (id < 0) {
            return "0-";
        } else if (dataArr.isEmpty()) {
            return "0-";
        } else {
            String ret = getDomeInfoStr(dataArr.get(0), id);
            for (int i = 1; i < dataArr.size(); i++) {
                String tmp = ret;
                ret = getDomeInfoStr(dataArr.get(i), id);
                if (!tmp.equals(ret)) {
                    return "M-";
                }
            }
            return ret;
        }
    }

    /**
     * CSV Escape handling for given string.
     *  " -> ""
     *  , -> ","
     *
     * @param str The string will be escaped for CSV format output
     * @return Escaped CSV string
     */
    public static String escapeCsvStr(String str) {
        if (str != null && !str.equals("")) {
            boolean needQuote = false;
            if (str.contains("\"")) {
                str = str.replaceAll("\"", "\"\"");
                needQuote = true;
            }
            if (!needQuote && str.contains(",")) {
                needQuote = true;
            }
            if (needQuote) {
                str = "\"" + str + "\"";
            }
            return str;
        } else {
            return "";
        }
    }

    public static String checkCMSeries(String exname, String climId, String rapId, String manId) {

        String cmSeries = "";

        if (exname != null && !exname.equals("")) {

            if (!exname.matches("(\\w+_\\d+)_b\\w+__\\d+")
                    && !exname.matches("(\\w+_\\d+)__\\d+")) {
                cmSeries = "CM0";
            } // According to the AgMIP Protocols, using X as the last
            // indicator means no scenarios.
            else if (climId.startsWith("0") && climId.endsWith("X")) {
                if (rapId.equals("")) {
                    if (manId.equals("")) {
                        cmSeries = "CM1";
                    } else {
                        cmSeries = "CM3";
                    }
                } else {
                    cmSeries = "CM4";
                }
            } else if (rapId.equals("")) {
                cmSeries = "CM2";
            } else if (manId.equals("")) {
                cmSeries = "CM5";
            } else {
                cmSeries = "CM6";
            }
        }

        return cmSeries;
    }
}

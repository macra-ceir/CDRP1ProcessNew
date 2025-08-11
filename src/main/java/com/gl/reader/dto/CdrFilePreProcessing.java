package com.gl.reader.dto;

import com.gl.reader.constants.Alerts;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Repository;
import org.springframework.stereotype.Service;

import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import static com.gl.reader.FileReaderHashApplication.*;

@Repository
public class CdrFilePreProcessing {
    static Logger logger = LogManager.getLogger(CdrFilePreProcessing.class);

    public static void insertReportv2( String fileType, String fileName, Long totalRecords, Long totalErrorRecords,
                                      Long totalDuplicateRecords, Long totalOutputRecords,Long totalNullImeis, String startTime, String endTime, Float timeTaken,
                                      Float tps, String operatorName, String sourceName, long volume, String tag, Integer FileCount,
                                      Integer headCount, String servername) {
        logger.info("Connection:::::" + conn);
        try (Statement stmt = conn.createStatement();) {
            endTime = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date());
            if (fileType.equalsIgnoreCase("O")) {
                headCount = headCount + 1;
            }
            String dateFunc = defaultStringtoDate( procesStart_timeStamp);
            String sql = "Insert into " + appdbName
                    + ".cdr_file_pre_processing_detail(CREATED_ON,MODIFIED_ON,FILE_TYPE,TOTAL_RECORDS,TOTAL_ERROR_RECORDS,TOTAL_DUPLICATE_RECORDS,TOTAL_OUTPUT_RECORDS,TOTAL_NULL_IMEIS,FILE_NAME,START_TIME,END_TIME,TIME_TAKEN,TPS,OPERATOR_NAME,SOURCE_NAME,VOLUME,TAG,FILE_COUNT , HEAD_COUNT ,servername )"
                    + "values(" + dateFunc + " , CURRENT_TIMESTAMP , '" + fileType + "'," + totalRecords + "," + totalErrorRecords + ","
                    + totalDuplicateRecords + "," + totalOutputRecords + ","+totalNullImeis+",'" + fileName + "'," + defaultStringtoDate( startTime) + ","
                    + defaultStringtoDate( endTime) + "," + timeTaken + "," + tps + ",'" + operatorName + "','" + sourceName + "'," + volume
                    + ",'" + tag + "'," + FileCount + "  ," + headCount + " , '" + servername + "'    )";

            //  Working with mysql
//            String sql = "Insert into " + appdbName
//                    + ".cdr_file_pre_processing_detail(CREATED_ON,MODIFIED_ON,FILE_TYPE,TOTAL_RECORDS,TOTAL_ERROR_RECORDS,TOTAL_DUPLICATE_RECORDS,TOTAL_OUTPUT_RECORDS,FILE_NAME,START_TIME,END_TIME,TIME_TAKEN,TPS,OPERATOR_NAME,SOURCE_NAME,VOLUME,TAG,FILE_COUNT , HEAD_COUNT ,servername )"
//                    + "values(" + dateFunc + " , " + dateFunction + " , '" + fileType + "'," + totalRecords + "," + totalErrorRecords + ","
//                    + totalDuplicateRecords + "," + totalOutputRecords + ",'" + fileName + "','" + startTime + "','"
//                    + endTime + "'," + timeTaken + "," + tps + ",'" + operatorName + "','" + sourceName + "'," + volume
//                    + ",'" + tag + "'," + FileCount + "  ," + headCount + " , '" + servername + "'    )";
            logger.info("Inserting into table  cdr _pre_processing  _report:: " + sql);
            stmt.executeUpdate(sql);
        } catch (Exception e) {
            Alert.raiseAlert( Alerts.ALERT_006, Map.of("<e>", "not able to insert in cdr_file_pre_processing_detail " + e.toString() + ". in   ", "<process_name>", "CDR_pre_processor"), 0);
            //      System.exit(0);
        }
    }


    public static String defaultStringtoDate( String date1) {
        if (conn.toString().contains("oracle")) {
            return "to_timestamp('" + date1 + "','YYYY-MM-DD HH24:MI:SS')";
        } else {
            return "'" + date1 + "'";
        }
    }

    public static String defaultDateNow(boolean isOracle) {
        if (isOracle) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String val = sdf.format(new Date());
            return "TO_DATE('" + val + "','YYYY-MM-DD HH24:MI:SS')"; // commented by sharad

        } else {
            return "now()";
        }
    }


}

package com.gl.reader;

import com.gl.reader.configuration.ConnectionConfiguration;
import com.gl.reader.configuration.PropertiesReader;
 import com.gl.reader.dto.CdrFilePreProcessing;
import com.gl.reader.dto.ModulesAudit;
import com.gl.reader.dto.SysParam;
import com.gl.reader.model.Book;
import com.ulisesbocchio.jasyptspringboot.annotation.EnableEncryptableProperties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.annotation.EnableAsync;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

import static com.gl.reader.dto.Alert.raiseAlert;
import static com.gl.reader.dto.ModulesAudit.updateModuleAudit;
import static com.gl.reader.dto.SysParam.getFilePatternByOperatorSource;
import static com.gl.reader.model.Book.createBook;
import static com.gl.reader.service.impl.CsvCreater.*;
import static com.gl.reader.service.impl.FileReaderService.getArrivalTimeFromFilePattern;

@EnableAsync
@SpringBootConfiguration
@SpringBootApplication(scanBasePackages = {"com.gl.reader"})
@EnableEncryptableProperties
public class FileReaderHashApplication {

    static Logger logger = LogManager.getLogger(FileReaderHashApplication.class);

    static long duplicate = 0;
    static long error = 0;
    static long inSet = 0;
    static long totalCount = 0;
    static long iduplicate = 0;
    static long ierror = 0;
    static long iinSet = 0;
    static long itotalCount = 0;
    static String type;
    static long value;
    static long processed = 0;
    static String fileName;
    static String extension;
    static String servername;
    static Integer sleep;
    static String sourceName;
    static String operatorName;
    static String eventTime;
    static String errorFlag;
    static long errorDuplicate = 0;
    static long inErrorSet = 0;
    static long totalFileCount = 0;
    static long totalFileRecordsCount = 0;
    static String inputLocation;
    static String outputLocation;
    static Long timeTaken;
    static Float Tps;
    static Integer returnCount;
    static long inputOffset = 0;
    static long outputOffset = 0;
    static String tag;
    static Integer fileCount = 0;
    static Integer headCount = 0;
    public static String appdbName = null;
    public static String auddbName = null;
    static Set<Book> errorFile = new HashSet<>();
    static Set<String> reportTypeSet = new HashSet<>();
    static List<String> pattern = new ArrayList<>();
    public static HashMap<String, HashMap<String, Book>> BookHashMap = new HashMap<>();
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss");
    static Clock offsetClock = Clock.offset(Clock.systemUTC(), Duration.ofHours(+7));
    static LocalDate currentdate = LocalDate.now();
    static Integer day = currentdate.getDayOfMonth();
    static Month month = currentdate.getMonth();
    static Integer year = currentdate.getYear();
    static List<String> ims_sources = new ArrayList<String>();
    public static PropertiesReader propertiesReader = null;
    static Map<String, String> cdrImeiCheckMap = new HashMap<String, String>();
    public static String procesStart_timeStamp = null;
    public static Connection conn = null;
    static String attributeSplitor = null;
   public static List<String> file_patterns = null;
    static ConnectionConfiguration connectionConfiguration = null;
    public static void main(String[] args) {
        File file = null;
        int insertedKey = 0;
        long startexecutionTime = new Date().getTime();
        try {
            operatorName = args[0];
            sourceName = args[1];

            ApplicationContext context = SpringApplication.run(FileReaderHashApplication.class, args);
            connectionConfiguration = (ConnectionConfiguration) context.getBean("connectionConfiguration");
            conn = connectionConfiguration.getConnection();
            logger.info("Connection:" + conn);

            DateTimeFormatter tagDtf = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
            LocalDateTime tagNow = LocalDateTime.now();
            tag = tagDtf.format(tagNow);
            propertiesReader = (PropertiesReader) context.getBean("propertiesReader");
            procesStart_timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime());
            appdbName = propertiesReader.appdbName;
            auddbName = propertiesReader.auddbName;
            type = propertiesReader.typeOfProcess;
            value = propertiesReader.filesCount;  // FILES-COUNT-PER-REPORT=-1
            extension = propertiesReader.extension;
            sleep = propertiesReader.sleepTime;
            inputLocation = propertiesReader.inputLocation.replace("${DATA_HOME}", System.getenv("DATA_HOME"));
            outputLocation = propertiesReader.outputLocation.replace("${DATA_HOME}", System.getenv("DATA_HOME"));
            errorFlag = propertiesReader.errorReportFlag;
            returnCount = sourceName.equalsIgnoreCase("all") ? propertiesReader.rowCountForSplit : 0;
            servername = propertiesReader.servername;
            ims_sources = propertiesReader.imsSources;
            attributeSplitor = sourceName.equalsIgnoreCase("all") ? propertiesReader.commaDelimiter : propertiesReader.attributeSeperator;
            cdrImeiCheckMap = SysParam.getCdrImeiLengthValues(conn);

            long startexecutionTimeNew = new Date().getTime();
            if (!sourceName.contains("all")) {
                file_patterns= getFilePatternByOperatorSource(conn,operatorName,sourceName);
            }
            if (!(ims_sources.contains(sourceName))) { // "sm_ims".equals(folderName)
                reportTypeSet.addAll(propertiesReader.reportType);
                if (reportTypeSet.contains("null")) {
                    reportTypeSet = new HashSet<>();
                }
            }
            if (value == -1) {
                File directory = new File(inputLocation + "/" + operatorName + "/" + sourceName);
                value = directory.list().length;
                logger.info("Total File Count:" + value + " " + new File(directory, "null").exists());
                if (value == 0) {
                    logger.info("No file present");
                    String currentTime = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date());
                    CdrFilePreProcessing.insertReportv2("O", "0", 0L, 0L, 0L, 0L,
                            currentTime, currentTime, (float) 0, (float) 0, operatorName, sourceName,
                            0L, tag, 0, headCount, servername);
                    insertedKey = ModulesAudit.insertModuleAudit(conn, sourceName.equalsIgnoreCase("all") ? "P2" : "P1", operatorName + "_" + sourceName, servername);
                    updateModuleAudit(conn, 200, "Success", "", insertedKey, startexecutionTimeNew, 0, 0);
                    System.exit(0);
                }
            }

            long filRetriver = 0;
            insertedKey = ModulesAudit.insertModuleAudit(conn, sourceName.equalsIgnoreCase("all") ? "P2" : "P1", operatorName + "_" + sourceName, servername);
            while (true) {
                startexecutionTimeNew = new Date().getTime();
                Instant startTimeOutput = Instant.now(offsetClock);
                String startTimeOutput1 = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date());
                File folder = new File(inputLocation + "/" + operatorName + "/" + sourceName);
                File[] listOfFiles = folder.listFiles();
                int filesLength = listOfFiles.length;
                logger.info(" file present Count {}", filesLength);
                if ((filesLength <= 0)) {  //&& (processed < value)
                    logger.info("No file present.  files Length " + filesLength + " Now processed- " + processed + "  and Value- " + value);
                    if (insertedKey != 0) {
                        updateModuleAudit(conn, 200, "Success", "", insertedKey, startexecutionTime, totalFileRecordsCount, totalFileCount);
                    }
                    System.exit(0);
                }
                filRetriver = filesLength > value ? value : filesLength;
                logger.info("Total Files Left " + filesLength + ", Files to be processed now " + filRetriver);
                for (int j = 0; j < filRetriver; j++) {
                    file = listOfFiles[j];
                    Instant startTime = Instant.now(offsetClock);
                    String startTime1 = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date());
                    if (file.isFile() && !file.getName().endsWith(extension)) {
                        eventTime = getArrivalTimeFromFilePattern(sourceName, file.getName());
                        if ((!sourceName.contains("all")) && (eventTime == null)) {
                            logger.info("File Move to Error Folder: III FileName: " + file.getName() + ", Date: " + DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss")
                                    + ", Start Time: " + startTime + ", End Time: " + Instant.now(offsetClock) + ", Time Taken: , Operator Name: " + operatorName + ", Source Name: " + sourceName + ", TPS: " + Tps + ", Error: " + ierror + ", inSet: " + iinSet + ", totalCount: " + itotalCount + ", duplicate: " + iduplicate + ", volume: " + inputOffset + ", tag: " + tag + ", EventTime Tag  is null");
                            Path pathFolder = Paths.get(outputLocation + "/" + operatorName + "/" + sourceName + "/error/" + year + "/" + month + "/" + day);
                            if (!Files.exists(pathFolder)) {
                                Files.createDirectories(pathFolder);
                            }
                            Files.move(Paths.get(inputLocation + "/" + operatorName + "/" + sourceName + "/" + file.getName()),
                                    Paths.get(outputLocation + "/" + operatorName + "/" + sourceName + "/error/" + year + "/" + month + "/" + day + "/" + file.getName()));

                            CdrFilePreProcessing.insertReportv2("I", file.getName(), itotalCount, ierror, iduplicate, iinSet,
                                    startTime1.toString(), Instant.now(offsetClock).toString(), 0.0f, Tps, operatorName, sourceName, inputOffset, tag, 1, headCount, servername);
                            processed++;
                            continue;
                        }
                        logger.info("Inside Loop::  Value: " + filRetriver + " . Processed : " + processed + " folder/sourceName" + sourceName);
                        logger.info("ims_sources:: " + ims_sources);
                        if (processed < filRetriver) {
                            fileName = file.getName();
                            boolean check = readBooksFromCSV(file.getName());
                            if (!check) {
                                processed++;
                                moveFileToError(fileName);
                                continue;
                            }
                            createNRenameFileIfExists(outputLocation + "/" + operatorName + "/" + sourceName + "/processed/" + year + "/" + month + "/" + day, fileName);
                            // move file
                            Path temp = Files.move(Paths.get(inputLocation + "/" + operatorName + "/" + sourceName + "/" + file.getName()),
                                    Paths.get(outputLocation + "/" + operatorName + "/" + sourceName + "/processed/" + year + "/" + month + "/" + day + "/" + fileName));
                            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
                            LocalDateTime now = LocalDateTime.now();
                            Instant endTime = Instant.now(offsetClock);
                            timeTaken = Duration.between(startTime, endTime).toMillis();
                            float timeTakenF = ((float) timeTaken / 1000);
                            if (timeTakenF == 0.0) {
                                timeTakenF = (float) 0.001;
                            }
                            Tps = itotalCount / timeTakenF;
                            logger.info(" Input File Report -- III FileName: " + fileName + ", Date: " + dtf.format(now) + ", Start Time: " + startTime + ", End Time: " + endTime + ", Time Taken: " + timeTakenF + ", Operator Name: " + operatorName + ", Source Name: " + sourceName + ", TPS: " + Tps + ", Error: " + ierror + ", inSet: " + iinSet + ", totalCount: " + itotalCount + ", duplicate: " + iduplicate + ", volume: " + inputOffset + ", tag: " + tag);
                            fileCount++;
                            CdrFilePreProcessing.insertReportv2("I", fileName, itotalCount, ierror, iduplicate, iinSet,
                                    startTime1.toString(), endTime.toString(), timeTakenF, Tps,
                                    operatorName, sourceName, inputOffset, tag, 1, headCount, servername);
                            headCount = 0;
                            ierror = 0;
                            iinSet = 0;
                            itotalCount = 0;
                            iduplicate = 0;
                            inputOffset = 0;
                            logger.info("File moved successfully and data inserted");
                            processed++;
                        } else {
                            logger.info("Output File Report Inside {if(nothing logs)remove block} :Value : " + filRetriver + "  Processed : " + processed);
                            makeCsv(outputLocation, operatorName, sourceName, fileName, returnCount);
                            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
                            LocalDateTime now = LocalDateTime.now();
                            Instant endTimeOutput = Instant.now(offsetClock);
                            timeTaken = Duration.between(startTimeOutput, endTimeOutput).toMillis();
                            float timeTakenF = ((float) timeTaken / 1000);
                            if (timeTakenF == 0.0) {
                                timeTakenF = (float) 0.001;
                            }
                            Tps = totalCount / timeTakenF;
                            logger.info("Output File Report XXXX In FileName: " + fileName + ", Date: " + dtf.format(now) + ", Start Time: " + startTimeOutput1 + ", End Time: " + endTimeOutput + ", Time Taken: " + timeTakenF + ", Operator Name: " + operatorName + ", Source Name: " + sourceName + ", TPS: " + Tps + ", Error: " + error + ", inSet: " + inSet + ", totalCount: " + totalCount + ", duplicate: " + duplicate + ", volume: " + outputOffset + ", tag: " + tag);
                            CdrFilePreProcessing.insertReportv2("O", fileName, totalCount, error, duplicate, inSet,
                                    startTimeOutput1, endTimeOutput.toString(), timeTakenF,
                                    Tps, operatorName, sourceName, outputOffset, tag, fileCount,
                                    headCount, servername);
                            headCount = 0;
                            updateModuleAudit(conn, 202, "Processing", "", insertedKey, startexecutionTimeNew, totalFileRecordsCount, totalFileCount);
                            error = 0;
                            inSet = 0;
                            totalCount = 0;
                            duplicate = 0;
                            outputOffset = 0;
                            fileCount = 0;
                            processed = 0;
                            BookHashMap.clear();
                            makeErrorCsv(outputLocation, operatorName, sourceName, fileName, errorFile);//makeErrorCsv();
                            logger.info("Error Csv Created In FileName: " + fileName + ", Date: " + dtf.format(now) + ", Error: " + errorDuplicate + ", inFile: " + inErrorSet);
                            errorDuplicate = 0;
                            inErrorSet = 0;
                            errorFile.clear();
                        }
                    } else {   // file Extention Check
                        logger.info("No file or Incorrect file format present {}  moving file to error " + file.getName());
                        Path pathFile = Paths.get(outputLocation + "/" + operatorName + "/" + sourceName + "/error/" + year + "/" + month + "/" + day + "/errorFile/");
                        Files.createDirectories(pathFile);
                        Files.move(Paths.get(inputLocation + "/" + operatorName + "/" + sourceName + "/" + file.getName()),
                                Paths.get(pathFile + "/" + file.getName()));
                        logger.info("File moved to {}" , pathFile);
                        processed++;
                        continue;
                    }
                }

                logger.info("End Loop-- " + "Processed- : " + processed + "Value- : " + filRetriver);
                if (processed >= filRetriver) {  //processed <= value
                    logger.info("Final Processed is more than Retriver ***** *****  To check if working ");
                    makeCsv(outputLocation, operatorName, sourceName, fileName, returnCount);
                    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
                    LocalDateTime now = LocalDateTime.now();
                    Instant endTimeOutput = Instant.now(offsetClock);
                    timeTaken = Duration.between(startTimeOutput, endTimeOutput).toMillis();
                    float timeTakenF = ((float) timeTaken / 1000);
                    if (timeTakenF == 0.0) {
                        timeTakenF = (float) 0.001;
                    }
                    Tps = totalCount / timeTakenF;
                    logger.info("Output File Report Final Out FileName: " + fileName + ", Date: " + dtf.format(now) + ", Start Time: "
                            + startTimeOutput1 + ", End Time: " + endTimeOutput + ", Time Taken: " + timeTakenF
                            + ", Operator Name: " + operatorName + ", Source Name: " + sourceName + ", TPS: " + Tps
                            + ", Error: " + error + ", inSet: " + inSet + ", totalCount: " + totalCount
                            + ", duplicate: " + duplicate + ", volume: " + outputOffset + ", tag: " + tag + "; File Processed  " + fileCount + "Total Final File count " + totalFileCount);
                    CdrFilePreProcessing.insertReportv2("O", fileName, totalCount, error, duplicate, inSet, startTimeOutput1,
                            endTimeOutput.toString(), timeTakenF, Tps, operatorName, sourceName, outputOffset, tag,
                            fileCount, headCount, servername);
                    totalFileCount += fileCount;
                    totalFileRecordsCount += totalCount;
                    updateModuleAudit(conn, 202, "Processing", "", insertedKey, startexecutionTimeNew, totalFileRecordsCount, totalFileCount);
                    headCount = 0;
                    error = 0;
                    inSet = 0;
                    totalCount = 0;
                    duplicate = 0;
                    outputOffset = 0;
                    fileCount = 0;
                    processed = 0;
                    BookHashMap.clear();
                    makeErrorCsv(outputLocation, operatorName, sourceName, fileName, errorFile);//makeErrorCsv();//
                    logger.info("Error Csv Created Out FileName: " + fileName + ", Date: " + dtf.format(now) + ", Error: " + errorDuplicate + ", inFile: " + inErrorSet);
                    errorDuplicate = 0;
                    inErrorSet = 0;
                    errorFile.clear();
                }
            }

        } catch (Exception e) {
            logger.error(e + "in [" + Arrays.stream(e.getStackTrace()).filter(ste -> ste.getClassName().equals(FileReaderHashApplication.class.getName())).collect(Collectors.toList()).get(0) + "]");
            raiseAlert("alert006", Map.of("<e>", e.toString() + ". in file  ", "<process_name>", "CDR_pre_processor"), 0);
            updateModuleAudit(conn, 500, "Failure", e.getLocalizedMessage(), insertedKey, startexecutionTime, totalFileRecordsCount, totalFileCount);//numberOfRecord ,long totalFileCount
        } finally {
            try {
                conn.close();
            } catch (Exception e) {
                logger.error("Not able to close the connection");
            }
        }
    }

      static boolean readBooksFromCSV(String fileName) {
        logger.info(" ReadBooksFrom CSV with fileName " + fileName);
        Path pathToFile = Paths.get(inputLocation + "/" + operatorName + "/" + sourceName + "/" + fileName);
        String line = null;
        String folder_name;
        String file_name = "";
        String event_time;
        String imei = "";
        String imsi = "";
        String msisdn = "";
        String systemType = "";
        String recordType = "";
        logger.info("File With Path  : " + pathToFile);
        try {
            String[] myArray = cdrImeiCheckMap.get("CDR_IMEI_LENGTH_VALUE").split(",");
            BufferedReader br = Files.newBufferedReader(pathToFile, StandardCharsets.US_ASCII);
            if (sourceName.equals("all")) {
                br.readLine();
                headCount++;
            }
            line = br.readLine();
            while (line != null) {
                itotalCount++; // dec
                totalCount++; // dec
                logger.info("Actual LINE--: " + line);
                String[] attributes = line.split(attributeSplitor, -1);
                if (attributes.length < 5) {// return error move line to error  + add conters // go to next line
                    logger.info("Line length is less");
                }
                inputOffset += line.getBytes(StandardCharsets.US_ASCII).length + 1; // 1 is for line separator
                if (sourceName.equals("all")) {
                    folder_name = attributes[5];
                    file_name = attributes[6];
                    event_time = attributes[7];
                } else {
                    folder_name = sourceName;
                    file_name = fileName;
                    event_time = eventTime;
                }
                if (ims_sources.contains(sourceName)) {
                    if (attributes[0].equalsIgnoreCase("role-of-Node") || attributes[0].equalsIgnoreCase("role_of_Node")) {    //role_of_Node
                        line = br.readLine();
                        headCount++;
                        continue;
                    }
                    if (attributes[1].equalsIgnoreCase("IMEI")) {
                        imei = attributes[2].replaceAll("-", "");  //.substring(0, 14)
                        if (attributes[3].toLowerCase().contains("imsi")) {
                            imsi = attributes[4];
                        }
                        if ("6".equals(attributes[9])) {
                            msisdn = attributes[10].replace("tel:+", "");
                        } else {
                            if ("0".equals(attributes[0]) || "originating".equalsIgnoreCase(attributes[0])) {
                                msisdn = attributes[5].replace("tel:+", "");
                            } else if ("1".equals(attributes[0]) || "terminating".equalsIgnoreCase(attributes[0])) {
                                msisdn = attributes[6].replace("tel:+", "");
                            }
                        }
                        String[] systemTypeTemp = attributes[7].split(propertiesReader.semiColonDelimiter, -1);
                        systemType = systemTypeTemp[0];
                        if (("0".equals(attributes[0]) || "originating".equalsIgnoreCase(attributes[0]))
                                && ("INVITE".equals(attributes[8]) || "BYE".equals(attributes[8]))) {
                            recordType = "0";
                        } else if (("1".equals(attributes[0]) || "terminating".equalsIgnoreCase(attributes[0]))
                                && ("INVITE".equals(attributes[8]) || "BYE".equals(attributes[8]))) {
                            recordType = "1";
                        } else if (("0".equals(attributes[0]) || "originating".equalsIgnoreCase(attributes[0]))
                                && "MESSAGE".equals(attributes[8])) {
                            recordType = "6";
                        } else if (("1".equals(attributes[0]) || "terminating".equalsIgnoreCase(attributes[0]))
                                && "MESSAGE".equals(attributes[8])) {
                            recordType = "7";
                        } else {
                            recordType = "100";
                        }
                    } else {
                        line = br.readLine();
                        error++;
                        ierror++;
                    }
                } else {
                    if (attributes[0].equalsIgnoreCase("IMEI")) {
                        headCount++;
                        line = br.readLine();
                        continue;
                    }
                    imei = attributes[0];
                    imsi = attributes[1];
                    msisdn = attributes[2];
                    recordType = attributes[3];
                    systemType = attributes[4];
                }
                logger.info("CDR Line ----" + imei, imsi, msisdn, recordType, systemType);
                Book book = createBook(imei, imsi, msisdn, recordType, systemType, folder_name, file_name, event_time);

                if ((imei.isEmpty() || imei.matches("^[0]*$"))) {
                    if (cdrImeiCheckMap.get("CDR_NULL_IMEI_CHECK").equalsIgnoreCase("true")) {
                        logger.info("Null Imei ,Check True, Error generator : " + imei);
                        Book bookError = createBook(imei, imsi, msisdn, recordType, systemType, folder_name, file_name, event_time);
                        if (errorFile.contains(bookError)) {
                            errorDuplicate++;
                        } else {
                            inErrorSet++;
                            errorFile.add(bookError);
                        }
                        line = br.readLine();
                        error++;
                        ierror++;
                        continue;
                    } else {
                        imei = cdrImeiCheckMap.get("CDR_NULL_IMEI_REPLACE_PATTERN");
                        logger.info("Null Imei and Check  is False, now Converting  imei :" + imei);
                    }
                }
                if (imsi.isEmpty() || msisdn.isEmpty()
                        || imsi.length() > 20 || msisdn.length() > 20 || (!imsi.matches("^[a-zA-Z0-9_]*$"))
                        || (!msisdn.matches("^[a-zA-Z0-9_]*$"))
                        || ((cdrImeiCheckMap.get("CDR_IMEI_LENGTH_CHECK").equalsIgnoreCase("true")) && !(Arrays.asList(myArray).contains(String.valueOf(imei.length()))))
                        || (!imei.matches("^[ 0-9 ]+$") && cdrImeiCheckMap.get("CDR_ALPHANUMERIC_IMEI_CHECK").equalsIgnoreCase("true"))) {
                    logger.info("Wrong record: imsi/mssidn-> empty, >20, !a-Z0-9 :: [" + imsi + "][ " + msisdn + "]"
                            + " OR imei->When length check defined & length criteria not met,non numeric with alphaNum Check true :[" + imei + "] ");
                    Book bookError = createBook(imei, imsi, msisdn, recordType, systemType, folder_name, file_name, event_time);
                    if (errorFile.contains(bookError)) {
                        errorDuplicate++;
                    } else {
                        inErrorSet++;
                        errorFile.add(bookError);
                    }
                    line = br.readLine();
                    error++;
                    ierror++;
                    continue;
                }



//                if (!reportTypeSet.isEmpty()) { // set is empty
//                    if (!reportTypeSet.contains(recordType)) {
//                        line = br.readLine();
//                        error++;
//                        totalCount++;
//                        ierror++;
//                        itotalCount++;
//                        continue;
//                    }
//                }
                //    HashMap<String, HashMap<String, Book>> BookHashMap

                if (BookHashMap.containsKey(book.getIMEI().length() > 14 ? book.getIMEI().substring(0, 14) : book.getIMEI())) {
                    if (!BookHashMap.get(book.getIMEI().length() > 14 ? book.getIMEI().substring(0, 14) : book.getIMEI()).containsKey(book.getMSISDN())) {
                        BookHashMap.get(book.getIMEI().length() > 14 ? book.getIMEI().substring(0, 14) : book.getIMEI()).put(book.getMSISDN(), book);
                        inSet++;
                        iinSet++;
                        outputOffset += line.getBytes(StandardCharsets.US_ASCII).length + 1; // 1 is for line separator
                    } else {
                        duplicate++;
                        iduplicate++;
                    }
                } else {
                    HashMap<String, Book> bookMap = new HashMap<>();
                    bookMap.put(book.getMSISDN(), book);
                    BookHashMap.put(book.getIMEI().length() > 14 ? book.getIMEI().substring(0, 14) : book.getIMEI(), bookMap);
                    // logger.info("If no imei then object: " + book);
                    inSet++;
                    iinSet++;
                    outputOffset += line.getBytes(StandardCharsets.US_ASCII).length + 1; // 1 is for line separator
                }
                line = br.readLine();
            }
            br.close();
        } catch (Exception e) {
            logger.error("Alert in  " + line + "Error: " + e + "in [" + Arrays.stream(e.getStackTrace()).filter(ste -> ste.getClassName().equals(FileReaderHashApplication.class.getName())).collect(Collectors.toList()).get(0) + "]");
            raiseAlert( "alert006", Map.of("<e>", e.toString() + ". in file  " + file_name, "<process_name>", "CDR_pre_processor"), 0);
            return false;
        }
        return true;
    }


//    public static String getArrivalTimeFromFilePattern(String fileName) {
//        String date = "";
//        if (!folderName.contains("all")) {
//            pattern.addAll(patternz);
//            if (pattern.contains("null")) {
//                pattern = new ArrayList<String>();
//            }
//        }
//        for (String filePattern : pattern) {
//            String[] attributes = filePattern.split("-", -1);
//            if (fileName.contains(attributes[0])) {
//                date = fileName.substring(fileName.indexOf(attributes[0]) + Integer.parseInt(attributes[1]),
//                        fileName.indexOf(attributes[0]) + Integer.parseInt(attributes[1])
//                                + Integer.parseInt(attributes[2]));
//            }
//        }
//        String imei_arrivalTime = null;
//        try {
//            String dateType = "yyyyMMdd";
//            if (propertiesReader.ddMMyyyySource.contains(folderName)) {
//                dateType = "ddMMyyyy";
//            } else if (propertiesReader.yyMMddSource.contains(folderName)) {
//                dateType = "yyMMdd";
//            } else if (propertiesReader.ddMMyySource.contains(folderName)) {
//                dateType = "ddMMddyy";
//            }
//            imei_arrivalTime = new SimpleDateFormat("yyyy-MM-dd").format(new SimpleDateFormat(dateType).parse(date));
//        } catch (Exception e) {
//            logger.info(fileName + " Unable to parse Date ,Defined Pattern" + patternz + ", Error " + e);
//        }
//        return imei_arrivalTime;
//    }


    public static void moveFileToError(String fileName) throws IOException {
        Path pathFile = Paths.get(outputLocation + "/" + operatorName + "/" + sourceName + "/error/" + year + "/" + month + "/" + day + "/errorFile");
        if (!Files.exists(pathFile)) {
            Files.createDirectories(pathFile);
            logger.info("Directory created");
        }
        // rename file
        if (Files.exists(Paths.get(outputLocation + "/" + operatorName + "/" + sourceName + "/error/" + year + "/" + month + "/" + day + "/errorFile/" + fileName))) {
            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
            File sourceFile = new File(outputLocation + "/" + operatorName + "/" + sourceName + "/error/" + year + "/" + month + "/" + day + "/errorFile/" + fileName);
            String newName = fileName + "-" + sdf.format(timestamp);
            File destFile = new File(outputLocation + "/" + operatorName + "/" + sourceName + "/error/" + year + "/" + month + "/" + day + "/errorFile/" + newName);
            if (sourceFile.renameTo(destFile)) {
                logger.info("File renamed successfully");
            } else {
                logger.info("Failed to rename file");
            }
        }
        // move file
        Path temp = null;
        try {
            temp = Files.move(Paths.get(inputLocation + "/" + operatorName + "/" + sourceName + "/" + fileName),
                    Paths.get(outputLocation + "/" + operatorName + "/" + sourceName + "/error/" + year + "/" + month + "/" + day + "/errorFile/" + fileName));
        } catch (Exception e) {
            logger.warn(" File   " + fileName + " Not able to move ");
        }
        if (temp != null) {
            logger.info("File moved in Error Folder successfully");
        } else {
            logger.warn("Failed to move the file in Error Folder" + fileName);
        }
    }

}
 
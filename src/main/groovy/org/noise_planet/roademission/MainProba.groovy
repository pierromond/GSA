package org.noise_planet.roademission

import com.opencsv.CSVWriter
import groovy.sql.Sql
import groovy.transform.CompileStatic
import org.h2gis.api.ProgressVisitor
import org.h2gis.functions.io.shp.SHPRead
import org.h2gis.utilities.SFSUtilities
import org.noise_planet.noisemodelling.propagation.ComputeRays
import org.noise_planet.noisemodelling.propagation.ComputeRaysOut
import org.noise_planet.noisemodelling.propagation.IComputeRaysOut
import org.noise_planet.noisemodelling.propagation.RootProgressVisitor
import org.noise_planet.noisemodelling.propagation.jdbc.PointNoiseMap
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.sql.Connection
import java.text.DateFormat
import java.text.SimpleDateFormat

/**
 * To run
 * Just type "gradlew -Pworkdir=out/"
 */
@CompileStatic
class MainProba {
    static void main(String[] args) {
        // Read working directory argument
        String workingDir = ""
        if (args.length > 0) {
            workingDir = args[0]
        }

        // Init output logger
        Logger logger = LoggerFactory.getLogger(MainProba.class)
        logger.info(String.format("Working directory is %s", new File(workingDir).getAbsolutePath()))


        // Create spatial database
        //TimeZone tz = TimeZone.getTimeZone("UTC")
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss", Locale.getDefault())

        //df.setTimeZone(tz)
        String dbName = new File(workingDir + "database").toURI()
        Connection connection = SFSUtilities.wrapConnection(DbUtilities.createSpatialDataBase(dbName, true))
        Sql sql = new Sql(connection)

        // Evaluate receiver points using provided buildings

        sql.execute("DROP TABLE IF EXISTS BUILDINGS")

        logger.info("Read building file")
        SHPRead.readShape(connection, "data/BUILDINGS_ZONE_CAPTEUR.shp", "BUILDINGS")
        sql.execute("CREATE SPATIAL INDEX ON BUILDINGS(THE_GEOM)")
        logger.info("Building file loaded")

        // Load or create receivers points
        sql.execute("DROP TABLE IF EXISTS RECEIVERS")
        SHPRead.readShape(connection, "data/RecepteursQuest3D.shp", "RECEIVERS")
        sql.execute("CREATE SPATIAL INDEX ON RECEIVERS(THE_GEOM)")

        // Load roads
        logger.info("Read road geometries and traffic")
        // ICA 2019 - Questionnaire
        SHPRead.readShape(connection, "data/CARS3D.shp", "CARS")
        sql.execute("CREATE SPATIAL INDEX ON CARS(THE_GEOM)")
        logger.info("Road file loaded")

        // Load ground type
        logger.info("Read ground surface categories")
        SHPRead.readShape(connection, "data/land_use_zone_capteur2D.shp", "GROUND_TYPE")
        sql.execute("CREATE SPATIAL INDEX ON GROUND_TYPE(THE_GEOM)")
        logger.info("Surface categories file loaded")

        // Load Topography
        logger.info("Read topography")
        SHPRead.readShape(connection, "data/DEM_2503D2.shp", "DEM")
        sql.execute("DROP TABLE TOPOGRAPHY if exists;")
        sql.execute("CREATE TABLE TOPOGRAPHY AS SELECT PK2, THE_GEOM from DEM;")
        sql.execute("CREATE SPATIAL INDEX ON TOPOGRAPHY(THE_GEOM)")
        logger.info("Topography file loaded")

        // Init NoiseModelling
        PointNoiseMap pointNoiseMap = new PointNoiseMap("BUILDINGS", "CARS", "RECEIVERS")
        pointNoiseMap.setSoilTableName("GROUND_TYPE")
        pointNoiseMap.setDemTable("TOPOGRAPHY")
        pointNoiseMap.setMaximumPropagationDistance(500.0d) // 300 ICA sensitivity
        pointNoiseMap.setMaximumReflectionDistance(100.0d) // 100 ICA sensitivity
        pointNoiseMap.setWallAbsorption(0.1d)
        pointNoiseMap.soundReflectionOrder = 1
        pointNoiseMap.computeHorizontalDiffraction = true
        pointNoiseMap.computeVerticalDiffraction = true
        pointNoiseMap.setHeightField("HAUTEUR")
        pointNoiseMap.setThreadCount(8) // Use 4 cpu threads
        pointNoiseMap.setReceiverHasAbsoluteZCoordinates(false)
        pointNoiseMap.setSourceHasAbsoluteZCoordinates(false)
        pointNoiseMap.setMaximumError(0.1d)
        PropagationPathStorageFactory storageFactory = new PropagationPathStorageFactory()
        ProbaPropagationProcessDataFactory probaPropagationProcessDataFactory = new ProbaPropagationProcessDataFactory()
        pointNoiseMap.setPropagationProcessDataFactory(probaPropagationProcessDataFactory)
        pointNoiseMap.setComputeRaysOutFactory(storageFactory)
        storageFactory.setWorkingDir(workingDir)


        logger.info("Start time :" + df.format(new Date()))

        List<ComputeRaysOut.verticeSL> allLevels = new ArrayList<>()
        try {
            storageFactory.openPathOutputFile(new File("raysProba.gz").absolutePath)
            RootProgressVisitor progressLogger = new RootProgressVisitor(2, true, 1)
            pointNoiseMap.initialize(connection, progressLogger)
            progressLogger.endStep()
            // Set of already processed receivers
            Set<Long> receivers = new HashSet<>()
            ProgressVisitor progressVisitor = progressLogger.subProcess(pointNoiseMap.getGridDim()*pointNoiseMap.getGridDim())
            for (int i = 0; i < pointNoiseMap.getGridDim(); i++) {
                for (int j = 0; j < pointNoiseMap.getGridDim(); j++) {
                    logger.info("Compute... i:" + i.toString() + " j: " +j.toString() )
                    IComputeRaysOut out = pointNoiseMap.evaluateCell(connection, i, j, progressVisitor, receivers)
                    if (out instanceof ComputeRaysOut) {
                        allLevels.addAll(((ComputeRaysOut) out).getVerticesSoundLevel())
                    }
                }
            }


            logger.info("Compute results by receivers...")


            DynamicProcessData dynamicProcessData = new DynamicProcessData()
            dynamicProcessData.setProbaTable("CARS", sql)


            logger.info("End time :" + df.format(new Date()))



            logger.info("Write results to csv file...")
            CSVWriter writer = new CSVWriter(new FileWriter(workingDir + "/ResultatsProba2.csv"))

            def t_old = -1
            def idSource_old = -1
            for (int t=1;t<900;t++){
                Map<Integer, double[]> soundLevels = new HashMap<>()
                Map<Integer, double[]> sourceLev = new HashMap<>()

                for (int i=0;i< allLevels.size() ; i++) {

                    int idReceiver = (Integer) allLevels.get(i).receiverId
                    int idSource = (Integer) allLevels.get(i).sourceId
                    double[] soundLevel = allLevels.get(i).value

                    if (!sourceLev.containsKey(idSource)) {
                        sourceLev.put(idSource, dynamicProcessData.getCarsLevel( t,idSource))
                    }


                    if (sourceLev.get(idSource)[0]>0){
                        if (soundLevels.containsKey(idReceiver)) {
                            soundLevel = ComputeRays.sumDbArray(sumLinearArray(soundLevel,sourceLev.get(idSource)), soundLevels.get(idReceiver))
                            soundLevels.replace(idReceiver, soundLevel)
                        } else {
                            soundLevels.put(idReceiver, sumLinearArray(soundLevel,sourceLev.get(idSource)))
                        }


                        // closing writer connection

                    }
                }
                for (Map.Entry<Integer, double[]> entry : soundLevels.entrySet()) {
                    Integer key = entry.getKey()
                    double[] value = entry.getValue()
                    value = DBToDBA(value)
                    writer.writeNext([key.toString(),t.toString(), ComputeRays.wToDba(ComputeRays.sumArray(ComputeRays.dbaToW(value))).toString()] as String[])
                }
            }
            writer.close()


            Map<Integer, double[]> soundLevels2 = new HashMap<>()

            logger.info("Write results to csv file...")
            CSVWriter writer2 = new CSVWriter(new FileWriter(workingDir + "/ResultatsProba.csv"))

            for (int i=0;i< allLevels.size() ; i++) {
                int idReceiver = (Integer) allLevels.get(i).receiverId
                int idSource = (Integer) allLevels.get(i).sourceId
                double[] soundLevel = allLevels.get(i).value
                 if (!Double.isNaN(soundLevel[0])
                         && !Double.isNaN(soundLevel[1])
                         && !Double.isNaN(soundLevel[2])
                         && !Double.isNaN(soundLevel[3])
                         && !Double.isNaN(soundLevel[4])
                         && !Double.isNaN(soundLevel[5])
                         && !Double.isNaN(soundLevel[6])
                         && !Double.isNaN(soundLevel[7])

                 ) {

                     writer2.writeNext([idReceiver,idSource, DBToDBA(soundLevel)] as String[])



                   /* if (soundLevels.containsKey(idReceiver)) {
                        soundLevel = ComputeRays.sumDbArray(soundLevel, soundLevels.get(idReceiver))
                        soundLevels.replace(idReceiver, soundLevel)
                    } else {
                        soundLevels.put(idReceiver, soundLevel)
                    }*/
                } else {
                     logger.info("NaN on Rec :" + idReceiver + "and Src :" + idSource)
                 }
            }

            logger.info("End time :" + df.format(new Date()))
            writer2.close()



            /*sql.execute("drop table if exists receiver_lvl_day_zone, receiver_lvl_evening_zone, receiver_lvl_night_zone;")
            sql.execute("create table receiver_lvl_day_zone (idrecepteur integer, idsource integer,att63 double precision, att125 double precision, att250 double precision, att500 double precision, att1000 double precision, att2000 double precision, att4000 double precision, att8000 double precision);")

            def qry = 'INSERT INTO RECEIVER_LVL_DAY_ZONE (IDRECEPTEUR, IDSOURCE,' +
                    'ATT63, ATT125, ATT250, ATT500, ATT1000,ATT2000, ATT4000, ATT8000) ' +
                    'VALUES (?,?,?,?,?,?,?,?,?,?);'
            sql.withBatch(100, qry) { ps ->
                for (int i=0;i< allLevels.size() ; i++) {
                    ps.addBatch(allLevels.get(i).receiverId, allLevels.get(i).sourceId,
                            allLevels.get(i).value[0], allLevels.get(i).value[1], allLevels.get(i).value[2],
                            allLevels.get(i).value[3], allLevels.get(i).value[4], allLevels.get(i).value[5],
                            allLevels.get(i).value[6], allLevels.get(i).value[7])
                    ps
                }
            }*/

        } finally {
            storageFactory.closeWriteThread()
        }



    }
    static double[] DBToDBA(double[] db){
        double[] dbA = [-26.2,-16.1,-8.6,-3.2,0,1.2,1.0,-1.1]
        for(int i = 0; i < db.length; ++i) {
            db[i] = db[i] + dbA[i]
        }
        return db

    }
    static double[] sumLinearArray(double[] array1, double[] array2) {
        if (array1.length != array2.length) {
            throw new IllegalArgumentException("Not same size array")
        } else {
            double[] sum = new double[array1.length]

            for(int i = 0; i < array1.length; ++i) {
                sum[i] = array1[i] + array2[i]
            }

            return sum
        }
    }

}

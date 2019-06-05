package org.noise_planet.roademission

import com.opencsv.CSVWriter
import groovy.sql.Sql
import groovy.transform.CompileStatic
import org.h2gis.api.EmptyProgressVisitor
import org.h2gis.api.ProgressVisitor
import org.h2gis.functions.io.shp.SHPRead
import org.h2gis.utilities.SFSUtilities
import org.noise_planet.noisemodelling.propagation.ComputeRays
import org.noise_planet.noisemodelling.propagation.ComputeRaysOut
import org.noise_planet.noisemodelling.propagation.IComputeRaysOut
import org.noise_planet.noisemodelling.propagation.PropagationPath
import org.noise_planet.noisemodelling.propagation.RootProgressVisitor
import org.noise_planet.noisemodelling.propagation.jdbc.PointNoiseMap
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.sql.Connection
import java.text.DateFormat
import java.text.DecimalFormat
import java.text.SimpleDateFormat

/**
 * To run
 * Just type "gradlew -Pworkdir=out/"
 */
@CompileStatic
class Main {
    static void main(String[] args) {
        // Read working directory argument
        String workingDir = ""
        if (args.length > 0) {
            workingDir = args[0]
        }

        // Init output logger
        Logger logger = LoggerFactory.getLogger(Main.class)
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
        SHPRead.readShape(connection, "data/STUDY_AREA.shp", "STUDY_AREA")
        sql.execute("CREATE SPATIAL INDEX ON BUILDINGS(THE_GEOM)")
        logger.info("Building file loaded")

        // Load or create receivers points
        sql.execute("DROP TABLE IF EXISTS RECEIVERS")
        if(!new File("data/receivers_build_pop.shp").exists()) {
            DbUtilities.createReceiversFromBuildings(sql, "BUILDINGS", "STUDY_AREA")
        } else {
            SHPRead.readShape(connection, "data/receivers_build_pop.shp", "RECEIVERS")
        }
        sql.execute("CREATE SPATIAL INDEX ON RECEIVERS(THE_GEOM)")

        // Load roads
        logger.info("Read road geometries and traffic")
        SHPRead.readShape(connection, "data/ROADS_TRAFFIC_ZONE_CAPTEUR.shp", "ROADS2")
        sql.execute("DROP TABLE ROADS if exists;")
        sql.execute('CREATE TABLE ROADS AS SELECT id, ST_UpdateZ(THE_GEOM, 0.05) the_geom, \n' +
                        'lv_d_speed,mv_d_speed,hv_d_speed,wav_d_spee,wbv_d_spee,\n' +
                        'lv_e_speed,mv_e_speed,hv_e_speed,wav_e_spee,wbv_e_spee,\n' +
                        'lv_n_speed,mv_n_speed,hv_n_speed,wav_n_spee,wbv_n_spee,\n' +
                        'vl_d_per_h,ml_d_per_h,pl_d_per_h,wa_d_per_h,wb_d_per_h,\n' +
                        'vl_e_per_h,ml_e_per_h,pl_e_per_h,wa_e_per_h,wb_e_per_h,\n' +
                        'vl_n_per_h,ml_n_per_h,pl_n_per_h,wa_n_per_h,wb_n_per_h,\n' +
                        'Zstart,Zend, Juncdist, Junc_type,road_pav FROM ROADS2;')
        sql.execute('ALTER TABLE ROADS ALTER COLUMN ID SET NOT NULL;')
        sql.execute('ALTER TABLE ROADS ADD PRIMARY KEY (ID);')
        sql.execute("CREATE SPATIAL INDEX ON ROADS(THE_GEOM)")
        logger.info("Road file loaded")

        // Load ground type
        logger.info("Read ground surface categories")
        SHPRead.readShape(connection, "data/land_use_zone_capteur4.shp", "GROUND_TYPE")
        sql.execute("CREATE SPATIAL INDEX ON GROUND_TYPE(THE_GEOM)")
        logger.info("Surface categories file loaded")

        // Load Topography
        logger.info("Read topography")
        SHPRead.readShape(connection, "data/DEM_LITE.shp", "DEM")
        sql.execute("DROP TABLE TOPOGRAPHY if exists;")
        sql.execute("CREATE TABLE TOPOGRAPHY AS SELECT ST_UpdateZ(THE_GEOM, CONTOUR) the_geom from DEM;")
        sql.execute("CREATE SPATIAL INDEX ON TOPOGRAPHY(THE_GEOM)")
        logger.info("Topography file loaded")

        // Init NoiseModelling
        PointNoiseMap pointNoiseMap = new PointNoiseMap("BUILDINGS", "ROADS", "RECEIVERS")
        pointNoiseMap.setSoilTableName("GROUND_TYPE")
        pointNoiseMap.setDemTable("TOPOGRAPHY")
        pointNoiseMap.setMaximumPropagationDistance(50.0d)
        pointNoiseMap.setMaximumReflectionDistance(10.0d)
        pointNoiseMap.setWallAbsorption(0.1d)
        pointNoiseMap.soundReflectionOrder = 0
        pointNoiseMap.computeHorizontalDiffraction = false
        pointNoiseMap.computeVerticalDiffraction = false
        pointNoiseMap.setHeightField("HAUTEUR")
        pointNoiseMap.setThreadCount(11) // Use 4 cpu threads
        pointNoiseMap.setReceiverHasAbsoluteZCoordinates(false)
        pointNoiseMap.setSourceHasAbsoluteZCoordinates(false)
        //pointNoiseMap.setMaximumError(0.0d)
        PropagationPathStorageFactory storageFactory = new PropagationPathStorageFactory()
        TrafficPropagationProcessDataFactory trafficPropagationProcessDataFactory = new TrafficPropagationProcessDataFactory()
        pointNoiseMap.setPropagationProcessDataFactory(trafficPropagationProcessDataFactory)
        pointNoiseMap.setComputeRaysOutFactory(storageFactory)
        storageFactory.setWorkingDir(workingDir)

        logger.info("Start time :" + df.format(new Date()))

        List<ComputeRaysOut.verticeSL> allLevels = new ArrayList<>()
        try {
            storageFactory.openPathOutputFile(new File("D:\\aumond\\Documents\\CENSE\\LorientMapNoise\\out2\\rays0506_250.gz").absolutePath)
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

            Map<Integer, double[]> soundLevels = new HashMap<>()

            for (int i=0;i< allLevels.size() ; i++) {
                int idReceiver = (Integer) allLevels.get(i).receiverId
                int idSource = (Integer) allLevels.get(i).sourceId
                double[] soundLevel = allLevels.get(i).value
                 if (!Double.isNaN(soundLevel[0])) {
                    if (soundLevels.containsKey(idReceiver)) {
                        soundLevel = ComputeRays.sumDbArray(soundLevel, soundLevels.get(idReceiver))
                        soundLevels.replace(idReceiver, soundLevel)
                    } else {
                        soundLevels.put(idReceiver, soundLevel)
                    }
                } else {
                     logger.info("NaN on Rec :" + idReceiver + "and Src :" + idSource)
                 }
            }

            logger.info("End time :" + df.format(new Date()))

            logger.info("Write results to csv file...")
            CSVWriter writer = new CSVWriter(new FileWriter(workingDir + "/Resultat.csv"))
            for (Map.Entry<Integer, double[]> entry : soundLevels.entrySet()) {
                Integer key = entry.getKey()
                double[] value = entry.getValue()
                value = DBToDBA(value)
                writer.writeNext([key.toString(), ComputeRays.wToDba(ComputeRays.sumArray(ComputeRays.dbaToW(value))).toString()] as String[])
            }
            // closing writer connection
            writer.close()


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
            db[i] = ComputeRays.wToDba(ComputeRays.dbaToW(db[i]) + ComputeRays.dbaToW(dbA[i]))
        }
        return db

    }

}
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
        sql.execute("CREATE SPATIAL INDEX ON BUILDINGS(THE_GEOM)")
        logger.info("Building file loaded")

        // Load or create receivers points
        sql.execute("DROP TABLE IF EXISTS RECEIVERS")
        SHPRead.readShape(connection, "data/RECEIVERS.shp", "RECEIVERS")
        sql.execute("CREATE SPATIAL INDEX ON RECEIVERS(THE_GEOM)")
        sql.execute('DELETE FROM RECEIVERS WHERE ID <> 1 ;')

        // Load roads
        logger.info("Read road geometries and traffic")
        SHPRead.readShape(connection, "data/DRONE.shp", "DRONE")

        //sql.execute('ALTER TABLE DRONE ALTER COLUMN ID SET NOT NULL;')
        //sql.execute('ALTER TABLE DRONE ADD PRIMARY KEY (ID);')
        sql.execute("CREATE SPATIAL INDEX ON DRONE(THE_GEOM)")

        logger.info("Road file loaded")

        // Load ground type
        /*logger.info("Read ground surface categories")
        SHPRead.readShape(connection, "data/land_use_zone_capteur4.shp", "GROUND_TYPE")
        sql.execute("CREATE SPATIAL INDEX ON GROUND_TYPE(THE_GEOM)")
        logger.info("Surface categories file loaded")

        // Load Topography
        logger.info("Read topography")
        SHPRead.readShape(connection, "data/DEM_250.shp", "DEM")
        sql.execute("DROP TABLE TOPOGRAPHY if exists;")
        sql.execute("CREATE TABLE TOPOGRAPHY AS SELECT ST_UpdateZ(THE_GEOM, CONTOUR) the_geom from DEM;")
        sql.execute("CREATE SPATIAL INDEX ON TOPOGRAPHY(THE_GEOM)")
        logger.info("Topography file loaded")
*/

        // Init NoiseModelling
        PointNoiseMap pointNoiseMap = new PointNoiseMap("BUILDINGS", "DRONE", "RECEIVERS")
        //pointNoiseMap.setSoilTableName("GROUND_TYPE")
        //pointNoiseMap.setDemTable("TOPOGRAPHY")
        pointNoiseMap.setMaximumPropagationDistance(250.0d)
        pointNoiseMap.setMaximumReflectionDistance(50.0d)
        pointNoiseMap.setWallAbsorption(0.1d)
        pointNoiseMap.soundReflectionOrder = 1
        pointNoiseMap.computeHorizontalDiffraction = true
        pointNoiseMap.computeVerticalDiffraction = true
        pointNoiseMap.setHeightField("HAUTEUR")
        pointNoiseMap.setThreadCount(10) // Use 4 cpu threads
        pointNoiseMap.setReceiverHasAbsoluteZCoordinates(false)
        pointNoiseMap.setSourceHasAbsoluteZCoordinates(false)
        pointNoiseMap.setMaximumError(0.1d)
        PropagationPathStorageFactory storageFactory = new PropagationPathStorageFactory()
        DronePropagationProcessDataFactory dronePropagationProcessDataFactory = new DronePropagationProcessDataFactory()
        pointNoiseMap.setPropagationProcessDataFactory(dronePropagationProcessDataFactory)
        pointNoiseMap.setComputeRaysOutFactory(storageFactory)
        storageFactory.setWorkingDir(workingDir)

        logger.info("Start time :" + df.format(new Date()))

        List<ComputeRaysOut.verticeSL> allLevels = new ArrayList<>()
        try {
            storageFactory.openPathOutputFile(new File("D:\\aumond\\Documents\\CENSE\\LorientMapNoise\\out\\rays.gz").absolutePath)
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

            logger.info("End time :" + df.format(new Date()))



            logger.info("Write results to csv file...")
            CSVWriter writer = new CSVWriter(new FileWriter(workingDir + "/Resultats.csv"))

            for (int t=0;t<100;t++){
                Map<Integer, double[]> soundLevels = new HashMap<>()
                for (int i=0;i< allLevels.size() ; i++) {
                    int idReceiver = (Integer) allLevels.get(i).receiverId
                    int idSource = (Integer) allLevels.get(i).sourceId
                    double[] soundLevel = allLevels.get(i).value
                    double[] sourceLev = dynamicProcessData.getDroneLevel("DRONE", sql, t,idSource)
                    if (sourceLev[0]>0){
                        if (soundLevels.containsKey(idReceiver)) {
                            soundLevel = ComputeRays.sumDbArray(sumLinearArray(soundLevel,sourceLev), soundLevels.get(idReceiver))
                            soundLevels.replace(idReceiver, soundLevel)
                        } else {
                            soundLevels.put(idReceiver, sumLinearArray(soundLevel,sourceLev))
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
            double[] sum = new double[array1.length];

            for(int i = 0; i < array1.length; ++i) {
                sum[i] = array1[i] + array2[i]
            }

            return sum;
        }
    }

}

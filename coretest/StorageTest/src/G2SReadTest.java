import mmcorej.CMMCore;
import mmcorej.LongVector;
import mmcorej.StorageDataType;

import java.util.Arrays;

public class G2SReadTest {
    public static void main(String[] args) {
        // Test program call syntax:
        // java -cp <classpath> G2SReadTest <storage_engine> <data_dir> <dataset_name> [direct_io] [optimized_access]
        //
        // First argument determines the storage engine
        // Supported options are:
        // - bigtiff  : G2SBigTiffStorage
        // - zarr     : AcquireZarrStorage
        //
        if (args.length < 3) {
            System.out.println("Usage: java -cp <classpath> G2SReadTest <storage_engine> <data_dir> <dataset_name> [direct_io] [optimized_access]");
            return;
        }
        String storageEngine = args[0];
        if(!storageEngine.equals("zarr") && !storageEngine.equals("bigtiff")) {
            System.out.println("Invalid storage engine selected: " + storageEngine);
            return;
        }

        boolean directio = args.length > 3 && Integer.parseInt(args[3]) == 1;
        boolean optimalaccess = args.length <= 4 || Integer.parseInt(args[4]) != 0;
        boolean printmeta = args.length > 5 && Integer.parseInt(args[5]) == 1;

        // Dataset location
        String readDir = args[1];
        String datasetName = args[2];

        // instantiate MMCore
        CMMCore core = new CMMCore();

        // decide how are we going to call our devices within this script
        String store = "Store";

        try {
            // enable verbose logging
            core.enableStderrLog(true);
            core.enableDebugLog(true);

            // load the storage device
            if(storageEngine.equals("zarr"))
                core.loadDevice(store, "go2scope", "AcquireZarrStorage");
            else
                core.loadDevice(store, "go2scope", "G2SBigTiffStorage"); // alternative storage driver

            // initialize the system, this will in turn initialize each device
            core.initializeAllDevices();

            if(storageEngine.equals("bigtiff"))
                core.setProperty(store, "DirectIO", directio ? 1 : 0);

            long startRead = System.currentTimeMillis();
            String handle = core.loadDataset(readDir + "/" + datasetName);
            long dsReadTime = System.currentTimeMillis() - startRead;
            mmcorej.LongVector shape = core.getDatasetShape(handle);
            mmcorej.StorageDataType type = core.getDatasetPixelType(handle);
            assert(shape.size() > 2);
            int w = shape.get(0);
            int h = shape.get(1);
            int numImages = 1;
            for (int i = 2; i < shape.size(); i++) {
                numImages *= shape.get(i);
            }
            System.out.printf("Dataset: %s, %d x %d, images %d, type %s, loaded in %d ms\n", readDir + "/" + datasetName, w, h,
                    numImages, type, dsReadTime);

            // fetch some images
            mmcorej.LongVector coords = new LongVector(shape.size());
            for (int i=0; i<shape.size(); i++) {
                coords.set(i, 0);
            }
            for (int i = 0; i < numImages; i++) {
                // Reverse engineer coordinates
                if(optimalaccess)
                    calcCoordsOptimized(i, shape, coords);
                else
                    calcCoordsRandom(i, shape, coords);

                startRead = System.nanoTime();
                Object img = core.getImage(handle, coords);
                long imgReadTime = System.nanoTime() - startRead;
                if (img == null) {
                    System.out.println("Failed to fetch image " + i);
                    return;
                }
                int[] intCoords = new int[(int)coords.size()];
                for (int j = 0; j < coords.size(); j++)
                    intCoords[j] = coords.get(j);

                if(type == StorageDataType.StorageDataType_GRAY16) {
                    short[] bimage = (short[])img;
                    double bw = (2 * bimage.length / 1048576.0) / (imgReadTime / 1000000000.0);
                    System.out.println("Image " + String.format("%3d", i) + ", " + Arrays.toString(intCoords) + " size: " + bimage.length * 2 + ", in " + String.format("%.1f", imgReadTime / 1000000.0) + " ms (" + String.format("%.1f", bw) + " MB/s)");
                } else {
                    byte[] bimage = (byte[]) img;
                    double bw = (bimage.length / 1048576.0) / (imgReadTime / 1000000000.0);
                    System.out.println("Image " + String.format("%3d", i) + ", " + Arrays.toString(intCoords) + " size: " + bimage.length + ", in " + String.format("%.1f", imgReadTime / 1000000.0) + " ms (" + String.format("%.1f", bw) + " MB/s)");
                }

                String meta = core.getImageMeta(handle, coords);
                if(printmeta)
                    System.out.println("Image metadata: " + meta);
            }

            // we are done so close the dataset
            core.closeDataset(handle);

            // unload all devices (not really necessary)
            core.unloadAllDevices();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Calculate image coordinates for optimized access
     * @param ind Image index
     * @param shape Dataset shape
     * @param coords Image coordinates [out]
     */
    private static void calcCoordsOptimized(int ind, mmcorej.LongVector shape, mmcorej.LongVector coords) {
        int fx = 0;
        for(int j = (int)shape.size() - 1; j >= 2; j--) {
            int sum = 1;
            for(int k = 2; k < j; k++)
                sum *= shape.get(k);
            int ix = (ind - fx) / sum;
            coords.set(j, ix);
            fx += ix * sum;
        }
    }
    /**
     * Calculate image coordinates for random access
     * @param ind Image index
     * @param shape Dataset shape
     * @param coords Image coordinates [out]
     */
    private static void calcCoordsRandom(int ind, mmcorej.LongVector shape, mmcorej.LongVector coords) {
        int fx = 0;
        for(int j = 2; j < (int)shape.size(); j++) {
            int sum = 1;
            for(int k = j + 1; k < (int)shape.size(); k++)
                sum *= shape.get(k);
            int ix = (ind - fx) / sum;
            coords.set(j, ix);
            fx += ix * sum;
        }
    }
}

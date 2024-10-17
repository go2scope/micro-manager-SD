import mmcorej.CMMCore;
import mmcorej.LongVector;
import mmcorej.StorageDataType;
import mmcorej.TaggedImage;

import java.nio.ByteBuffer;
import java.nio.ShortBuffer;

public class G2SReadTest {
    public static void main(String[] args) {
        // First argument determines the storage engine
        // Supported options are:
        // - bigtiff  : G2SBigTiffStorage
        // - zarr     : AcquireZarrStorage
        //
        if (args.length != 3) {
            System.out.println("Usage: java -cp <classpath> G2SReadTest <storage engine> <data dir> <dataset name>");
            return;
        }
        String storageEngine = args[0];
        if(!storageEngine.equals("zarr") && !storageEngine.equals("bigtiff")) {
            System.out.println("Invalid storage engine selected: " + storageEngine);
            return;
        }

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

            long startRead = System.currentTimeMillis();
            String handle = core.loadDataset(readDir + "/" + datasetName, datasetName);
            long dsReadTime = System.currentTimeMillis() - startRead;
            mmcorej.LongVector shape = core.getDatasetShape(handle);
            mmcorej.StorageDataType type = core.getDatasetPixelType(handle);
            assert(shape.size() > 2);
            int w = (int) shape.get(0);
            int h = (int) shape.get(1);
            int numImages = 1;
            for (int i = 2; i < shape.size(); i++) {
                numImages *= (int) shape.get(i);
            }
            System.out.printf("Dataset: %s, %d x %d, images %d, type %s, loaded in %d ms\n", readDir + "/" + datasetName, w, h,
                    numImages, type, dsReadTime);

            // fetch some images
            mmcorej.LongVector coords = new LongVector(shape.size());
            for (int i=0; i<shape.size(); i++) {
                coords.set(i, 0);
            }
            for (int i = 0; i < numImages; i++) {
                Object img = core.getImage(handle, coords);
                if (img == null) {
                    System.out.println("Failed to fetch image " + i);
                    return;
                }
                byte[] bimage = (byte[]) img;
                System.out.println("Image " + i + " size: " + bimage.length);
            }

            // we are done so close the dataset
            core.closeDataset(handle);

            // unload all devices (not really necessary)
            core.unloadAllDevices();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

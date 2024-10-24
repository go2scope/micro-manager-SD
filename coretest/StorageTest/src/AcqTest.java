import mmcorej.CMMCore;
import mmcorej.LongVector;
import mmcorej.StorageDataType;
import mmcorej.TaggedImage;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ShortBuffer;

public class AcqTest {
    public static void main(String[] args) {
        // Test program call syntax:
        // java -cp <classpath> G2SWriteTest <storage_engine> <data_dir> <channeL_count> <time_points> [direct_io]
        //
        // First argument determines the storage engine
        // Supported options are:
        // - bigtiff  : G2SBigTiffStorage
        // - zarr     : AcquireZarrStorage
        //
        // By default 'AcquireZarrStorage' is selected
        String storageengine = args.length > 0 ? args[0] : "zarr";
        if(!storageengine.equals("zarr") && !storageengine.equals("bigtiff")) {
            System.out.println("Invalid storage engine selected: " + storageengine);
            return;
        }

        // Second argument determines the save location for the storage engine
        // If not specified working directory will be used
        String savelocation = args.length > 1 ? args[1] : ".";

        // Third argument determines number of channels to acquire (1 by default)
        int numberOfChannels = args.length > 2 ? Integer.parseInt(args[2]) : 1;

        // Fourth argument determines number of time points to acquire (2 by default)
        int numberOfTimepoints = args.length > 3 ? Integer.parseInt(args[3]) : 2;

        // fifth argument determines direct or cached I/O
        boolean directio = args.length > 5 && Integer.parseInt(args[5]) == 1;

        // sixth argument determines flush cycle
        int flushCycle = args.length > 6 ? Integer.parseInt(args[6]) : 0;

        // instantiate MMCore
        CMMCore core = new CMMCore();

        // decide how are we going to call our devices within this script
        String store = "Store";
        String camera = "Camera";

        try {
            // enable verbose logging
            core.enableStderrLog(true);
            core.enableDebugLog(true);

            // load the storage device
            if(storageengine.equals("zarr"))
                core.loadDevice(store, "go2scope", "AcquireZarrStorage");
            else
                core.loadDevice(store, "go2scope", "G2SBigTiffStorage"); // alternative storage driver

            // load the demo camera device
            core.loadDevice(camera, "HamamatsuHam", "HamamatsuHam_DCAM");

            // initialize the system, this will in turn initialize each device
            core.initializeAllDevices();

            // configure the camera device
            core.setProperty(camera, "PixelType", "16bit");
            core.setROI(1032, 0, 2368, 2368);
            core.setExposure(5.0);

            if(storageengine.equals("bigtiff")) {
                core.setProperty(store, "DirectIO", directio ? 1 : 0);
                core.setProperty(store, "FlushCycle", flushCycle);
            }

            // take one image to "warm up" the camera and get actual image dimensions
            core.snapImage();
            int w = (int)core.getImageWidth();
            int h = (int)core.getImageHeight();

            // fetch the image with metadata
            TaggedImage img = core.getTaggedImage();
            System.out.printf("Image %d X %d, size %d", w, h, ((short[])(img.pix)).length);

            // print the metadata provided by MMCore
            System.out.println(img.tags.toString());

           // create the new dataset
            LongVector shape = new LongVector();
            StorageDataType type = StorageDataType.StorageDataType_GRAY16;

            shape.add(w); // first dimension x
            shape.add(h); // second dimension y
            shape.add(numberOfChannels); // channels
            shape.add(numberOfTimepoints); // time points
            String handle = core.createDataset(savelocation, "test-" + storageengine, shape, type, "");

            core.logMessage("Dataset UID: " + handle);
            core.logMessage("START OF ACQUISITION");
            core.setCircularBufferMemoryFootprint(2000);
            core.clearCircularBuffer();
            int cap = core.getBufferFreeCapacity();
            System.out.println("Circular buffer free: " + cap + ", acquiring images " + numberOfTimepoints * numberOfChannels);
            core.startSequenceAcquisition(numberOfChannels * numberOfTimepoints, 0.0, true);
            Thread.sleep(50); // wait for sequence to start
            int imgind = 0;
            long start = System.currentTimeMillis();
            boolean cameraRunning = core.isSequenceRunning();
            for (int j = 0; j < numberOfTimepoints; j++) {
                if (core.isBufferOverflowed()) {
                    System.out.println("Buffer overflow!!");
                    break;
                }

                for (int k = 0; k < numberOfChannels; k++) {
                    // fetch the image
                    img = core.popNextTaggedImage();
                    cap = core.getBufferFreeCapacity();

                    // create coordinates for the image
                    LongVector coords = new LongVector();
                    coords.add(0);
                    coords.add(0);
                    coords.add(k);
                    coords.add(j);

                    // convert short buffer to byte buffer
                    // TODO: to avoid this conversion, MMCore storage API needs to support short data type directly
                    ByteBuffer bb = ByteBuffer.allocate(w * h * 2).order(ByteOrder.LITTLE_ENDIAN);
                    ShortBuffer sb = bb.asShortBuffer();
                    sb.put((short[]) img.pix);

                    // Add image index to the image metadata
                    img.tags.put("Image-index", imgind);

                    // add image to stream
                    double imgSizeMb = 2.0 * w * h / (1024.0 * 1024.0);
                    long startSave = System.nanoTime();
                    core.addImage(handle, bb.array().length, bb.array(), coords, img.tags.toString());
                    double imgSaveTime = System.nanoTime() - startSave;
                    double bw = imgSizeMb / (imgSaveTime / 1000000000.0);
                    System.out.printf("Saved image %d in %.2f ms, size %.1f MB, bw %.1f MB/s, cap=%d, camera=%s\n",
                            imgind++, imgSaveTime / 1000000.0, imgSizeMb, bw, cap, cameraRunning ? "running" : "stopped") ;

                }
            }
            long end = System.currentTimeMillis();
            core.stopSequenceAcquisition();
            // we are done so close the dataset
            core.closeDataset(handle);

            core.logMessage("END OF ACQUISITION");

            // Calculate storage driver bandwidth
            double elapseds = (end - start) / 1000.0;
            double sizemb = 2.0 * numberOfTimepoints * w * h / (1024.0 * 1024.0);
            double bw = sizemb / elapseds;
            System.out.printf("Acquisition completed in %.3f sec\n", elapseds);
            System.out.printf("Dataset size %.1f MB\n", sizemb);
            System.out.printf("Storage driver bandwidth %.1f MB/s\n", bw);

            // unload all devices (not really necessary)
            core.unloadAllDevices();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

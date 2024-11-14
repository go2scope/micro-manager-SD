import mmcorej.*;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ShortBuffer;

public class G2SWriteTest {
    public static void main(String[] args) {
        // Test program call syntax:
        // java -cp <classpath> G2SWriteTest <storage_engine> <data_dir> <channeL_count> <time_points> <positions> [direct_io] [flush_cycle]
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

        // Fifth argument determines number of positions to acquire (1 by default)
        int numberOfPositions = args.length > 4 ? Integer.parseInt(args[4]) : 1;

        // Sixth argument determines direct or cached I/O
        boolean directIo = args.length > 5 && Integer.parseInt(args[5]) == 1;

        // Seventh argument determines flush cycle
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
            core.loadDevice(camera, "DemoCamera", "DCam");

            // initialize the system, this will in turn initialize each device
            core.initializeAllDevices();

            // configure the camera device, simulate Hamamatsu Fire
            core.setProperty(camera, "PixelType", "16bit");
            core.setProperty(camera, "OnCameraCCDXSize", "4432");
            core.setProperty(camera, "OnCameraCCDYSize", "2368");
            core.setExposure(10.0);

            if(storageengine.equals("bigtiff")) {
                core.setProperty(store, "DirectIO", directIo ? 1 : 0);
                core.setProperty(store, "FlushCycle", flushCycle);
            }

            // take one image to "warm up" the camera and get actual image dimensions
            core.snapImage();
            int w = (int)core.getImageWidth();
            int h = (int)core.getImageHeight();

            // fetch the image with metadata
            TaggedImage img = core.getTaggedImage();

            // print the metadata provided by MMCore
            System.out.println(img.tags.toString());

           // create the new dataset
            mmcorej.LongVector shape = new LongVector();
            mmcorej.StorageDataType type = StorageDataType.StorageDataType_GRAY16;

            // zarr convention: T, C, Z, Y, X
            shape.add(numberOfPositions); // positions
            shape.add(numberOfTimepoints); // time points
            shape.add(numberOfChannels); // channels
            shape.add(h); // second dimension y
            shape.add(w); // first dimension x
            String handle = core.createDataset(savelocation, "test-" + storageengine, shape, type, "");

            core.logMessage("Dataset UID: " + handle);
            core.logMessage("START OF ACQUISITION");
            int imgind = 0;
            long start = System.nanoTime();
            double imgSaveTimeSumMs = 0.0;
            double imgSaveTimeMaxMs = 0;
            double imgSaveTimeMinMs = Long.MAX_VALUE;
            for(int p = 0; p < numberOfPositions; p++) {
                for (int t = 0; t < numberOfTimepoints; t++) {
                    for (int c = 0; c < numberOfChannels; c++) {
                        // snap an image
                        core.snapImage();

                        // fetch the image
                        img = core.getTaggedImage();

                        // create coordinates for the image
                        mmcorej.LongVector coords = new LongVector();
                        coords.add(p); // position
                        coords.add(t); // time point
                        coords.add(c); // channel
                        coords.add(0); // y
                        coords.add(0); // x

                        // convert short buffer to byte buffer
                        // TODO: to avoid this conversion, MMCore storage API needs to support short data type directly
                        ByteBuffer bb = ByteBuffer.allocate(w * h * 2).order(ByteOrder.LITTLE_ENDIAN);
                        ShortBuffer sb = bb.asShortBuffer();
                        sb.put((short[])img.pix);

                        // Add image index to the image metadata
                        img.tags.put("Image-index", imgind);

                        // add image to stream
                        double imgSizeMb = 2.0 * w * h / (1024.0 * 1024.0);
                        long startSaveNs = System.nanoTime();
                        core.addImage(handle, bb.array().length, bb.array(), coords, img.tags.toString());
                        double imgSaveTimeMs = (System.nanoTime() - startSaveNs)/1.0e6;
                        double bw = imgSizeMb / (imgSaveTimeMs / 1.0e3);
                        imgSaveTimeSumMs += imgSaveTimeMs;
                        imgSaveTimeMaxMs = Math.max(imgSaveTimeMaxMs, imgSaveTimeMs);
                        imgSaveTimeMinMs = Math.min(imgSaveTimeMinMs, imgSaveTimeMs);
                        System.out.printf("Saved image %d in %.2f ms, size %.1f MB, bw %.1f MB/s\n", imgind++, imgSaveTimeMs, imgSizeMb, bw);
                    }
                }
            }

            // we are done so close the dataset
            long startClose = System.nanoTime();
            core.closeDataset(handle);
            long end = System.nanoTime();
            double imgSaveTimeTotalMs = (end - startClose) / 1.0e6;
            System.out.printf("Image close time %.2f ms\n", imgSaveTimeTotalMs);

            core.logMessage("END OF ACQUISITION");

            // Calculate storage driver bandwidth
            double elapseds = (end - start) / 1.0e9;
            double sizemb = 2.0 * numberOfTimepoints * numberOfChannels * numberOfPositions * w * h / (1024.0 * 1024.0);
            double bw = sizemb / elapseds;
            System.out.printf("Acquisition completed in %.3f sec\n", elapseds);
            System.out.printf("Dataset size %.2f GB\n", sizemb / 1024.0);
            System.out.printf("Storage driver bandwidth %.1f MB/s\n", bw);
            System.out.printf("Average image save time %.2f ms, min=%.2f ms, max=%.2f ms\n",
                    imgSaveTimeSumMs / (numberOfTimepoints * numberOfChannels * numberOfPositions),
                    imgSaveTimeMinMs,
                    imgSaveTimeMaxMs);
            if (storageengine.equals("bigtiff"))
                System.out.printf("Direct io: %b, flush cycle: %d\n", directIo, flushCycle);
            // unload all devices (not really necessary)
            core.unloadAllDevices();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

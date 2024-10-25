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
        // java -cp <classpath> AcqTest <storage_engine> <data_dir> <channeL_count> <time_points> [direct_io]
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

        // Seventh argument determines camera device
        String selcamera = args.length > 7 ? args[7] : "hamamatsu";
        if(!selcamera.equals("hamamatsu") && !selcamera.equals("demo")) {
            System.out.println("Invalid camara device selected: " + selcamera);
            return;
        }

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
            if(selcamera.equals("hamamatsu"))
                core.loadDevice(camera, "HamamatsuHam", "HamamatsuHam_DCAM");
            else
                core.loadDevice(camera, "DemoCamera", "DCam");

            // initialize the system, this will in turn initialize each device
            core.initializeAllDevices();

            // configure the camera device
            if(selcamera.equals("hamamatsu")) {
                core.setProperty(camera, "PixelType", "16bit");
                core.setROI(1032, 0, 2368, 2368);
                core.setExposure(5.0);
            } else {
                core.setProperty(camera, "PixelType", "16bit");
                core.setProperty(camera, "OnCameraCCDXSize", "4432");
                core.setProperty(camera, "OnCameraCCDYSize", "2368");
                core.setExposure(5.0);
            }

            if(storageengine.equals("bigtiff")) {
                core.setProperty(store, "DirectIO", directio ? 1 : 0);
                core.setProperty(store, "FlushCycle", flushCycle);
            }

            core.setCircularBufferMemoryFootprint(2000);
            core.clearCircularBuffer();

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
            long start = System.nanoTime();
            String handle = core.createDataset(savelocation, "test-" + storageengine, shape, type, "");
            long endCreate = System.nanoTime();

            core.logMessage("Dataset UID: " + handle);
            int cap = core.getBufferFreeCapacity();
            System.out.println("Circular buffer free: " + cap + ", acquiring images " + numberOfTimepoints * numberOfChannels);
            core.logMessage("START OF ACQUISITION");
            core.startSequenceAcquisition(numberOfChannels * numberOfTimepoints, 0.0, true);
            ByteBuffer bb = ByteBuffer.allocate(w * h * 2).order(ByteOrder.LITTLE_ENDIAN);
            Thread.sleep(50); // wait for sequence to start
            int imgind = 0;
            long prepAcq = System.nanoTime();
            long startAcq = prepAcq;
            for(int j = 0; j < numberOfTimepoints; j++) {
                for(int k = 0; k < numberOfChannels; k++) {
                    if(core.isBufferOverflowed()) {
                        System.out.println("Buffer overflow!!");
                        break;
                    }
                    long waitStart = System.nanoTime();
                    while(core.getRemainingImageCount() == 0) { }
                    if(prepAcq == startAcq)
                        startAcq = System.nanoTime();
                    long imgStart = System.nanoTime();

                    // fetch the image
                    img = core.popNextTaggedImage();
                    long imgPop = System.nanoTime();

                    // create coordinates for the image
                    LongVector coords = new LongVector();
                    coords.add(0);
                    coords.add(0);
                    coords.add(k);
                    coords.add(j);

                    // Add image index to the image metadata
                    img.tags.put("Image-index", imgind);

                    // add image to stream
                    short[] bx = (short[])img.pix;
                    long startSave = System.nanoTime();
                    core.addImage(handle, bx.length, bx, coords, img.tags.toString());
                    long endSave = System.nanoTime();

                    // Calculate image statistics
                    double imgSizeMb = 2.0 * w * h / (1024.0 * 1024.0);
                    double tAcq = (endSave - imgStart) / 1000000.0;                 // ms
                    double tPop = (imgPop - imgStart) / 1000000.0;                  // ms
                    double tSave = (endSave - startSave) / 1000000.0;               // ms
                    double tWait = (imgStart - waitStart) / 1000000.0;              // ms
                    double bwacq = imgSizeMb / (tAcq / 1000.0);                     // MB/s
                    double bwpop = imgSizeMb / (tPop / 1000.0);                     // MB/s
                    double bwsav = imgSizeMb / (tSave / 1000.0);                    // MB/s
                    System.out.printf("Image %d acquired in %.1f ms, size %.1f MB, bw %.1f MB/s, wait time %.1f ms\n", imgind, tAcq, imgSizeMb, bwacq, tWait);
                    System.out.printf("Image %d saved in %.1f ms (%.1f MB/s), poped in %.1f ms (%.1f MB/s)\n", imgind, tSave, bwsav, tPop, bwpop);
                    imgind++;
                }
            }
            long endAcq = System.nanoTime();
            core.stopSequenceAcquisition();
            // we are done so close the dataset
            core.closeDataset(handle);

            core.logMessage("END OF ACQUISITION");
            long end = System.nanoTime();

            // Calculate storage driver bandwidth
            double imgsizemb = 2.0 * w * h / (1024.0 * 1024.0);
            double sizemb = numberOfTimepoints * numberOfChannels * imgsizemb;
            double tTotal = (end - start) / 1000000000.0;
            double tAcquisition = (endAcq - startAcq) / 1000000000.0;
            double tPrep = (startAcq - start) / 1000000000.0;
            double tCreate = (endCreate - start) / 1000000000.0;
            double bwtot = sizemb / tTotal;
            double bwacq = sizemb / tAcquisition;
            double fpsacq = bwacq / imgsizemb;
            double fpstot = bwtot / imgsizemb;
            System.out.printf("\nDataset size %.1f MB\n", sizemb);
            System.out.printf("Camera prep time %.3f sec\n", tPrep);
            System.out.printf("Dataset creation time %.3f sec\n", tCreate);
            System.out.printf("Active acquisition time %.3f sec\n", tAcquisition);
            System.out.printf("Storage driver bandwidth %.1f MB/s (%.1f fps)\n", bwacq, fpsacq);
            System.out.printf("Acquisition completed in %.3f sec\n", tTotal);
            System.out.printf("Acquisition bandwidth %.1f MB/s (%.1f fps)\n\n", bwtot, fpstot);

            // unload all devices (not really necessary)
            core.unloadAllDevices();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

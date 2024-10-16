import mmcorej.CMMCore;
import mmcorej.LongVector;
import mmcorej.StorageDataType;
import mmcorej.TaggedImage;

import java.nio.ByteBuffer;
import java.nio.ShortBuffer;

public class G2STest {
    public static void main(String[] args) {
        // instantiate MMCore
        CMMCore core = new CMMCore();

        // decide how are we going to call our devices within this script
        String store = "Store";
        String camera = "Camera";

        int numberOfTimepoints = 2;

        try {
            // enable verbose logging
            core.enableStderrLog(true);
            core.enableDebugLog(true);

            // load the storage device
            core.loadDevice(store, "go2scope", "AcquireZarrStorage");
            // core.loadDevice(store, "go2scope", "G2SBigTiffStorage"); // alternative storage driver

            // load the demo camera device
            core.loadDevice(camera, "DemoCamera", "DCam");

            // initialize the system, this will in turn initialize each device
            core.initializeAllDevices();

            // configure the camera device
            core.setProperty(camera, "PixelType", "16bit");

            // take one image to "warm up" the camera and get actual image dimensions
            core.snapImage();
            int w = (int)core.getImageHeight();
            int h = (int)core.getImageWidth();

            // fetch the image with metadata
            TaggedImage img = core.getTaggedImage();

            // print the metadata provided by MMCore
            System.out.println(img.tags.toString());

           // create the new dataset
            mmcorej.LongVector shape = new LongVector();
            mmcorej.StorageDataType type = StorageDataType.StorageDataType_GRAY16;

            shape.add(w); // first dimension x
            shape.add(h); // second dimension y
            shape.add(numberOfTimepoints); // time points
            String handle = core.createDataset("D:\\AcquisitionData\\g2sStorage", "test-zarr", shape, type, "");

            core.logMessage("START OF ACQUISITION");
            for (int i=0; i<numberOfTimepoints; i++) {
                // snap an image
                core.snapImage();

                // fetch the image
                img = core.getTaggedImage();

                // create coordinates for the image
                mmcorej.LongVector coords = new LongVector();
                coords.add(0);
                coords.add(0);
                coords.add(i);

                // convert short buffer to byte buffer
                // TODO: to avoid this conversion, MMCore storage API needs to support short data type directly
                ByteBuffer bb = ByteBuffer.allocate(w * h * 2);
                ShortBuffer sb = bb.asShortBuffer();
                sb.put((short[])img.pix);

                // add image to stream
                System.out.println("Adding image " + i);
                core.addImage(handle, bb.array().length, bb.array(), coords, img.tags.toString());
            }

            // we are done so close the dataset
            core.closeDataset(handle);
            core.logMessage("END OF ACQUISITION");

            // unload all devices (not really necessary)
            core.unloadAllDevices();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

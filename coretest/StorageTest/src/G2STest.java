import mmcorej.CMMCore;
import mmcorej.LongVector;
import mmcorej.StorageDataType;
import mmcorej.TaggedImage;

import java.nio.ByteBuffer;
import java.nio.ShortBuffer;

public class G2STest {
    public static void main(String[] args) {
        CMMCore core = new CMMCore();
        String store = "Store";
        String camera = "Camera";

        try {
            // core.loadDevice(store, "go2scope", "G2SBigTiffStorage");
            core.loadDevice(store, "go2scope", "AcquireZarrStorage");
            core.loadDevice(camera, "DemoCamera", "DCam");
            core.initializeAllDevices();
            core.setProperty(camera, "PixelType", "16bit");
            core.snapImage();
            TaggedImage img = core.getTaggedImage();
            System.out.println(img.tags.toString());
            mmcorej.LongVector shape = new LongVector();
            mmcorej.StorageDataType type = StorageDataType.StorageDataType_int16;
            int w = img.tags.getInt("Width");
            int h = img.tags.getInt("Height");

            shape.add(w); // first dimension x
            shape.add(h); // second dimension y
            shape.add(1); // time points
            String handle = core.createDataset("D:\\AcquisitionData\\g2sStorage", "test-tiff", shape, type, "");
            mmcorej.LongVector coords = new LongVector();
            coords.add(0);
            coords.add(0);
            coords.add(0);
            ByteBuffer bb = ByteBuffer.allocate(w * h * 2);
            ShortBuffer sb = bb.asShortBuffer();
            sb.put((short[])img.pix);
            core.addImage(handle, bb.array(), w, h, 2, coords, "");

            core.unloadAllDevices();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

package org.micromanager.data.internal.mmcstore;

import com.google.common.eventbus.Subscribe;
import mmcorej.CMMCore;
import mmcorej.LongVector;
import mmcorej.StorageDataType;
import mmcorej.TaggedImage;
import org.micromanager.data.*;
import org.micromanager.data.internal.*;
import org.micromanager.internal.MMStudio;
import org.micromanager.internal.propertymap.NonPropertyMapJSONFormats;
import org.micromanager.internal.utils.ReportingUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implements Storage on MMCore storage device.
 */
public class MMCStorageAdapter implements Storage {

   private CMMCore mmcStorage;
   private final DefaultDatastore store;
   private SummaryMetadata summaryMetadata;
   private String dsHandle = "";
   private String dataPath;
   private final ConcurrentHashMap<Coords, Image> writtenCoords = new ConcurrentHashMap<>();

   /**
    * Constructor of the MMCStorageAdapter.
    *
    * @param store Micro-Manager data store that will be used
    * @param dir Where to write the data
    * @param amInWriteMode Whether we are writing
    * @throws IOException Close to inevitable with data storage
    */
   public MMCStorageAdapter(Datastore store, String dir, Boolean amInWriteMode)
           throws IOException {
      // We must be notified of changes in the Datastore before everyone else,
      // so that others can read those changes out of the Datastore later.
      this.store = (DefaultDatastore) store;
      this.store.registerForEvents(this, 0);

      this.store.setSavePath(dir);
      this.store.setName(new File(dir).getName());

      // TODO: figure out prefix vs. parent directory

      // get instance of mmcore
      mmcStorage = MMStudio.getInstance().getCMMCore();

      // if we are not creating a new dataset, load the existing one
      if (!amInWriteMode) {
         try {
            dsHandle = mmcStorage.loadDataset(dir);
         } catch (Exception e) {
            throw new RuntimeException(e);
         }
      }

   }

   private HashMap<String, Object> coordsToHashMap(Coords coords) {
      HashMap<String, Object> axes = new HashMap<>();
      for (String s : coords.getAxes()) {
         axes.put(s, coords.getIndex(s));
      }
      // Axes with a value of 0 aren't explicitly encoded
      SummaryMetadata summaryMetadata = getSummaryMetadata();
      for (String s : getSummaryMetadata().getOrderedAxes()) {
         if (!axes.containsKey(s)) {
            axes.put(s, 0);
         }
      }

      return axes;
   }

   private static Coords hashMapToCoords(HashMap<String, Object> axes) {
      Coords.Builder builder = Coordinates.builder();
      for (String s : axes.keySet()) {
         builder.index(s, (Integer) axes.get(s));
      }
      return builder.build();
   }

   /**
    * Will be called when the event bus signals that there are new Summary Metadata.
    * New summary metadata may not have any effect on the storage.
    *
    * @param event The event gives access to the new SummaryMetadata.
    */
   @Subscribe
   public void onNewSummaryMetadata(DataProviderHasNewSummaryMetadataEvent event) {
      setSummaryMetadata(event.getSummaryMetadata());
   }

   /**
    * Sets summary metadata for the dataset
    *
    * @param summary To push to the storage.
    */
   public void setSummaryMetadata(SummaryMetadata summary) {
     if (!dsHandle.isEmpty())
        throw new IllegalStateException("Cannot set summary metadata after dataset is created");
      summaryMetadata = summary;
      Coords coordinates = summaryMetadata.getIntendedDimensions();
      if (coordinates == null) {
         throw new RuntimeException("Summary metadata must have intended dimensions");
      }

      // create an array of intended dimensions
      LongVector dimensions = new LongVector();
      // first add x and y
      dimensions.add(summaryMetadata.getImageWidth());
      dimensions.add(summaryMetadata.getImageHeight());
      for (String axis : coordinates.getAxes()) {
         dimensions.add(coordinates.getIndex(axis));
      }
      String summaryMDString = NonPropertyMapJSONFormats.summaryMetadata().toJSON(
                 ((DefaultSummaryMetadata) summary).toPropertyMap());
      try {
         dsHandle = mmcStorage.createDataset(new File(store.getSavePath()).getParent(),
                 store.getName(),
                 dimensions,
                 StorageDataType.StorageDataType_GRAY16,
                 summaryMDString);
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   @Override
   public void freeze() throws IOException {
      try {
         mmcStorage.closeDataset(dsHandle);
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
      store.unregisterForEvents(this);
   }

   @Override
   public void putImage(Image image) throws IOException {
      int pixelDepth = image.getBytesPerPixel();
      int width = image.getHeight();
      int height = image.getWidth();

      if (dsHandle.isEmpty()) {
         throw new RuntimeException("Cannot put image before dataset is created");
      }
      boolean rgb = image.getNumComponents() > 1;
      if (rgb) {
         throw new RuntimeException("RGB images are not supported");
      }
      Coords coords = image.getCoords();
      synchronized(writtenCoords) {
         writtenCoords.put(coords, image);
      }

      String mdString = NonPropertyMapJSONFormats.metadata().toJSON(
              ((DefaultMetadata) image.getMetadata()).toPropertyMap());
      try {
         LongVector coordinates = calcCoords(coords);
         mmcStorage.addImage(dsHandle, image.getByteArray().length, image.getByteArray(), coordinates, mdString);
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   @Override
   public Image getImage(Coords coords) throws IOException {
      boolean cvalid = false;
      synchronized(writtenCoords) {
         // Check cache
         if(writtenCoords.containsKey(coords)) {
            Image img = writtenCoords.get(coords);
            if(img != null)
               return img;
            cvalid = true;
         }
      }
      if(!cvalid && !hasCoords(coords))
         return null;
      LongVector cdx = calcCoords(coords);
      try {
         TaggedImage img = (TaggedImage) mmcStorage.getImage(dsHandle, cdx);
         return new DefaultImage(img);
         // TODO: implement this
      }
      catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   @Override
   public boolean hasImage(Coords coords) {
      if(!hasCoords(coords))
         return false;
      return writtenCoords.containsKey(coords);
   }

   @Override
   public Image getAnyImage() {
      synchronized(writtenCoords) {
         // Check cache
         if(!writtenCoords.isEmpty()) {
            Image img = writtenCoords.elements().nextElement();
            if(img != null)
               return img;
         }
      }
      return null;
//      LongVector coordinates = new LongVector();
//      // TODO: implement this
//      try {
//         TaggedImage img = (TaggedImage) mmcStorage.getImage(dsHandle, coordinates);
//         return new DefaultImage(img);
//      }
//      catch (Exception e) {
//         throw new RuntimeException(e);
//      }
   }

   @Override
   public Iterable<Coords> getUnorderedImageCoords() {
      // TODO: implement this
      return null;
   }

   /**
    * This implementation only check for an exact match. It does not actually return a list
    * of all images that match the given coordinates, as I am not quite sure how to implement that.
    *
    * @param coords Coordinates specifying images to match
    * @return List of images matching the coordinates
    * @throws IOException can always happen.
    */
   @Override
   public List<Image> getImagesMatching(Coords coords) throws IOException {
      ArrayList<Image> ret = new ArrayList<>();
      if(coords.getAxes().isEmpty())
         return ret;
      // TODO: implement this
      return ret;
   }

   @Override
   public List<Image> getImagesIgnoringAxes(Coords coords, String... ignoreTheseAxes) throws IOException {
      HashSet<Image> result = new HashSet<>();
      if(!hasCoords(coords))
         return new ArrayList<>(result);
      synchronized(writtenCoords) {
         for(Coords imageCoords : writtenCoords.keySet()) {
            if(coords.equals(imageCoords.copyRemovingAxes(ignoreTheseAxes))) {
               result.add(writtenCoords.get(imageCoords));
            }
         }
      }
      return new ArrayList<>(result);
   }

   @Override
   public int getMaxIndex(String axis) {
      Coords coordinates = summaryMetadata.getIntendedDimensions();
      if (coordinates == null) {
         throw new RuntimeException("Summary metadata missing intended dimensions");
      }
      return coordinates.getIndex(axis);
   }

   @Override
   public List<String> getAxes() {
      if (getSummaryMetadata() == null) {
         return null;
      }
      return getSummaryMetadata().getOrderedAxes();
   }

   @Override
   public Coords getMaxIndices() {
      // TODO: implement this
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public SummaryMetadata getSummaryMetadata() {
      return summaryMetadata;
   }

   @Override
   public int getNumImages() {
      if (dsHandle.isEmpty()) {
         return 0;
      }
      try {
         mmcorej.LongVector shape = mmcStorage.getDatasetShape(dsHandle);
         int numImages = 1;
         for (int i = 2; i < shape.size(); i++) {
            numImages *= shape.get(i);
         }
         return numImages;
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   @Override
   public void close() throws IOException {
      try {
         mmcStorage.closeDataset(dsHandle);
         synchronized (writtenCoords) {
            writtenCoords.clear();
         }
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   private LongVector calcCoords(Coords coords) {
      LongVector ret = new LongVector();
      if(dsHandle.isEmpty())
         return ret;
      Coords dims = summaryMetadata.getIntendedDimensions();
      if(dims == null || dims.getAxes().isEmpty())
         return ret;
      ret.add(0);       // Width
      ret.add(0);       // Height
      for(String axis : dims.getAxes()) {
         int index = coords.getIndex(axis);
         if(index >= dims.getIndex(axis))
            index = dims.getIndex(axis);
         ret.add(index);
      }
      return ret;
   }

   private boolean hasCoords(Coords coords) {
      if(dsHandle.isEmpty())
         return false;
      Coords dims = summaryMetadata.getIntendedDimensions();
      if(dims == null || dims.getAxes().isEmpty())
         return false;
      for(String axis : dims.getAxes()) {
         int index = coords.getIndex(axis);
         if(index >= dims.getIndex(axis))
            return false;
      }
      return true;
   }
}






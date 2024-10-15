package org.micromanager.data.internal.mmcstore;

import com.google.common.eventbus.Subscribe;
import mmcorej.CMMCore;
import mmcorej.LongVector;
import mmcorej.StorageDataType;
import org.micromanager.data.*;
import org.micromanager.data.internal.*;
import org.micromanager.internal.MMStudio;
import org.micromanager.internal.propertymap.NonPropertyMapJSONFormats;
import org.micromanager.internal.utils.ReportingUtils;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;

/**
 * Implements Storage on MMCore storage device.
 */
public class MMCStorageAdapter implements Storage {

   private CMMCore mmcStorage;
   private final DefaultDatastore store;
   private SummaryMetadata summaryMetadata;
   private String dsHandle = "";
   private String dataPath;

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
         dsHandle = mmcStorage.loadDataset(new File(dir).getParent(), store.getName());
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
      for (String axis : coordinates.getAxes()) {
         dimensions.add(coordinates.getIndex(axis));
      }
      String summaryMDString = NonPropertyMapJSONFormats.summaryMetadata().toJSON(
                 ((DefaultSummaryMetadata) summary).toPropertyMap());
      dsHandle = mmcStorage.createDataset(new File(store.getSavePath()).getParent(), store.getName(), dimensions,
              StorageDataType.StorageDataType_int16,
              summaryMDString);
   }

   @Override
   public void freeze() throws IOException {
      mmcStorage.closeDataset(dsHandle);
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

      LongVector coordinates = new LongVector();
      for (String axis : coords.getAxes()) {
         coordinates.add(coords.getIndex(axis));
      }

      String mdString = NonPropertyMapJSONFormats.metadata().toJSON(
              ((DefaultMetadata) image.getMetadata()).toPropertyMap());

      mmcStorage.addImage(dsHandle, image.getByteArray(), width, height, pixelDepth, coordinates, mdString);
   }

   @Override
   public Image getImage(Coords coords) throws IOException {
      // TODO: implement this
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public boolean hasImage(Coords coords) {
      // TODO: implement this
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public Image getAnyImage() {
      // TODO: implement this
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public Iterable<Coords> getUnorderedImageCoords() {
      // TODO: implement this
      throw new UnsupportedOperationException("Not implemented");
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
      // TODO: implement this
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public List<Image> getImagesIgnoringAxes(
           Coords coords, String... ignoreTheseAxes) throws IOException {
      // TODO: implement this
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public int getMaxIndex(String axis) {
      // TODO: implement this
      throw new UnsupportedOperationException("Not implemented");
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
      // TODO: implement this
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public int getNumImages() {
      // TODO: implement this
      throw new UnsupportedOperationException("Not implemented");
   }

   @Override
   public void close() throws IOException {
      mmcStorage.closeDataset(dsHandle);
   }
}






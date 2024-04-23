package org.micromanager.data.internal.mmcstore;

import com.google.common.eventbus.Subscribe;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import mmcorej.CMMCore;
import mmcorej.TaggedImage;
import mmcorej.org.json.JSONException;
import mmcorej.org.json.JSONObject;
import org.micromanager.data.*;
import org.micromanager.data.internal.*;
import org.micromanager.internal.propertymap.NonPropertyMapJSONFormats;
import org.micromanager.internal.utils.ReportingUtils;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;


/**
 * Implements Storage on MMCore storage device.
 */
public class MMCStorageAdapter implements Storage {

   private CMMCore mmcStorage;
   private final DefaultDatastore store;
   private SummaryMetadata summaryMetadata = (new DefaultSummaryMetadata.Builder()).build();
   private String dsHandle = "";

   /**
    * Constructor of the MMCStorageAdapter.
    *
    * @param store Micro-Manager data store that will be used
    * @param dir Where to write the data
    * @param amInWriteMode Whether we are writing
    * @throws IOException Close to inevitable with data storage
    */
   public MMCStorageAdapter(Datastore store, CMMCore mmc, String dir, Boolean amInWriteMode)
           throws IOException {
      // We must be notified of changes in the Datastore before everyone else,
      // so that others can read those changes out of the Datastore later.
      this.store = (DefaultDatastore) store;
      this.store.registerForEvents(this, 0);

      this.store.setSavePath(dir);
      this.store.setName(new File(dir).getName());

      mmcStorage = mmc;
      if (!amInWriteMode) {
         dsHandle = mmcStorage.acqLoadDatastore(dir);
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
    *
    * @param event The event gives access to the new SummaryMetadata.
    */
   @Subscribe
   public void onNewSummaryMetadata(DataProviderHasNewSummaryMetadataEvent event) {
      setSummaryMetadata(event.getSummaryMetadata());
   }

   /**
    * This is quite strange.  Only when we have SummaryMetadata, we can create the
    * Storage.
    *
    * @param summary To push to the storage.
    */
   public void setSummaryMetadata(SummaryMetadata summary) {
      try {
         String summaryMDString = NonPropertyMapJSONFormats.summaryMetadata().toJSON(
                 ((DefaultSummaryMetadata) summary).toPropertyMap());
         JSONObject jsonSummary;
         try {
            jsonSummary = new JSONObject(summaryMDString);
         } catch (JSONException e) {
            throw new RuntimeException("Problem with summary metadata");
         }
         Consumer<String> debugLogger = s -> ReportingUtils.logDebugMessage(s);
         storage_ = new NDTiffStorage(store.getSavePath(), store.getName(),
                 jsonSummary, 0, 0, false, 0,
                 SAVING_QUEUE_SIZE, debugLogger, false);
         try {
            summaryMetadata = DefaultSummaryMetadata.fromPropertyMap(
                     NonPropertyMapJSONFormats.summaryMetadata().fromJSON(
                              storage_.getSummaryMetadata().toString()));
         } catch (IOException e) {
            throw new RuntimeException(e);
         }
      } catch (Exception e) {
         ReportingUtils.logError(e, "Error setting new summary metadata");
      }
   }

   @Override
   public void freeze() throws IOException {
      mmcStorage.acqCloseDataset(dsHandle);
      store.unregisterForEvents(this);
   }

   @Override
   public void putImage(Image image) throws IOException {
      if (storage_ == null) {
         setSummaryMetadata(DefaultSummaryMetadata.getStandardSummaryMetadata());
      }
      if (summaryMetadata != null) {
         ImageSizeChecker.checkImageSizeInSummary(summaryMetadata, image);
      }
      boolean rgb = image.getNumComponents() > 1;
      HashMap<String, Object> axes = coordsToHashMap(image.getCoords());

      //TODO: This is getting the JSON metadata as a String, and then converting it into
      // A JSONObject again, and then it gets converted to string again in NDTiffStorage
      // Certainly inefficient, possibly performance limitiing depending on which
      // Thread this is called on.
      String mdString = NonPropertyMapJSONFormats.metadata().toJSON(
              ((DefaultMetadata) image.getMetadata()).toPropertyMap());

      JSONObject json = null;
      try {
         json = new JSONObject(mdString);
      } catch (JSONException e) {
         throw new RuntimeException(e);
      }
      // TODO: where to get the actual bit depth?
      int bitDepth = image.getBytesPerPixel() * 8;
      storage_.putImage(image.getRawPixels(), json, axes, rgb, bitDepth,
              image.getHeight(), image.getWidth());
   }

   @Override
   public Image getImage(Coords coords) throws IOException {
      if (storage_ == null) {
         return null;
      }
      TaggedImage ti = storage_.getImage(coordsToHashMap(coords));
      addEssentialImageMetadata(ti, coordsToHashMap(coords));
      return new DefaultImage(ti, hashMapToCoords(coordsToHashMap(coords)),
              studioMetadataFromJSON(ti.tags));
   }

   @Override
   public boolean hasImage(Coords coords) {
      if (storage_ == null) {
         return false;
      }
      return storage_.hasImage(coordsToHashMap(coords));
   }

   @Override
   public Image getAnyImage() {
      if (storage_.getAxesSet().size() == 0) {
         return null;
      }
      HashMap<String, Object> axes = storage_.getAxesSet().iterator().next();
      TaggedImage ti = storage_.getImage(axes);
      addEssentialImageMetadata(ti, axes);
      return new DefaultImage(ti, hashMapToCoords(axes), studioMetadataFromJSON(ti.tags));
   }

   @Override
   public Iterable<Coords> getUnorderedImageCoords() {
      return () -> {
         Stream<HashMap<String, Object>> axesStream = storage_.getAxesSet().stream();
         Stream<Coords> coordsStream = axesStream.map(MMCStorageAdapter::hashMapToCoords);
         return coordsStream.iterator();
      };
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
      List<Image> imageList = new LinkedList<>();
      if (storage_ == null) {
         return imageList;
      }
      HashMap<String, Object> ndTiffCoords = coordsToHashMap(coords);
      if (storage_.hasImage(ndTiffCoords)) {
         TaggedImage ti = addEssentialImageMetadata(storage_.getImage(ndTiffCoords), ndTiffCoords);
         Image img = new DefaultImage(ti, hashMapToCoords(ndTiffCoords),
                 studioMetadataFromJSON(ti.tags));
         imageList.add(img);
      }
      return imageList;
   }

   @Override
   public List<Image> getImagesIgnoringAxes(
           Coords coords, String... ignoreTheseAxes) throws IOException {
      // This is obviously wrong, but not quite sure what to do at this point....

      return getImagesMatching(coords);
   }

   @Override
   public int getMaxIndex(String axis) {
      if (storage_ == null || storage_.getAxesSet() == null || storage_.getAxesSet().isEmpty()) {
         return -1;
      }
      return storage_.getAxesSet().stream().map(new Function<HashMap<String, Object>, Integer>() {
         @Override
         public Integer apply(HashMap<String, Object> stringIntegerHashMap) {
            if (stringIntegerHashMap.containsKey(axis)) {
               return (Integer) stringIntegerHashMap.get(axis);
            }
            return -1;
         }
      }).reduce(Math::max).get();
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
      if (storage_ == null) {
         return null;
      }
      Coords.Builder builder = Coordinates.builder();
      for (String axis : getAxes()) {
         builder.index(axis,
                 storage_.getAxesSet().stream().map(stringIntegerHashMap -> {
                    if (stringIntegerHashMap.containsKey(axis)) {
                       return (Integer) stringIntegerHashMap.get(axis);
                    }
                    return -1;
                 }).reduce(Math::max).get());
      }
      return builder.build();
   }

   @Override
   public SummaryMetadata getSummaryMetadata() {
      if (storage_ == null) {
         return null;
      }
      try {
         return DefaultSummaryMetadata.fromPropertyMap(
                 NonPropertyMapJSONFormats.summaryMetadata().fromJSON(
                         storage_.getSummaryMetadata().toString()));
      } catch (IOException e) {
         throw new RuntimeException(e);
      }
   }

   @Override
   public int getNumImages() {
      if (storage_ == null) {
         return 0;
      }
      return storage_.getAxesSet().size();
   }

   @Override
   public void close() throws IOException {
      storage_.close();
   }

   /**
    * The DefaultImage converter expects to find width and height keys,
    * though the images fed in don't have them in metadata.
    * This function explicitly adds width and height keys.
    *
    * @param ti TaggedImage to which the width and height keys will be added.
    * @param axes List with axes.  What are these for?
    * @return TaggedImage with width and height metadata added.
    */
   private TaggedImage addEssentialImageMetadata(TaggedImage ti, HashMap<String, Object> axes) {
      EssentialImageMetadata essMD = storage_.getEssentialImageMetadata(axes);
      //Load essential metadata into the image metadata.
      try {
         ti.tags.put(PropertyKey.WIDTH.key(), essMD.width);
         ti.tags.put(PropertyKey.HEIGHT.key(), essMD.height);
         String pixType;
         if (essMD.bitDepth == 8 && essMD.rgb) {
            pixType = "RGB32";
         } else if (essMD.bitDepth == 8) {
            pixType = "GRAY8";
         } else {
            pixType = "GRAY16";
         }
         ti.tags.put(PropertyKey.PIXEL_TYPE.key(), pixType);
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
      return ti;
   }

   private Metadata studioMetadataFromJSON(JSONObject tags) {
      JsonElement je;
      try {
         je = new JsonParser().parse(tags.toString());
      } catch (Exception unlikely) {
         throw new IllegalArgumentException("Failed to parse JSON created from TaggedImage tags",
                 unlikely);
      }
      return DefaultMetadata.fromPropertyMap(
              NonPropertyMapJSONFormats.metadata().fromGson(je));
   }
}






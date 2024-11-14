package org.lumencor.targa;

import mmcorej.CMMCore;
import mmcorej.LongVector;
import mmcorej.StorageDataType;
import mmcorej.TaggedImage;

import java.util.Set;
import java.util.Vector;
import java.util.concurrent.CopyOnWriteArraySet;

public class AcqRunner extends Thread {
	private final Set<AcqRunnerListener> listeners_ = new CopyOnWriteArraySet<>();
	private final CMMCore core_;
	private final String location_;
	private final String name_;
	private final Vector<String> channels_;
	private final boolean timeLapse_;
	private final int timePoints_;
	private final int timeInterval_;
	private final Object stateMutex_;
	private boolean active_;
	private int current_;
	private int total_;

	/**
	 * Class constructor
	 * @parma core MMCore interface
	 * @param location Save location
	 * @param name Acauisition / Dataset name
	 * @param timelapse Run time-lapse acquisition
	 * @param timepoints Number of timepoints
	 * @param channels Channels list
	 * @param timeintervalms Time-lapse interval
	 */
	AcqRunner(CMMCore core, String location, String name, boolean timelapse, int timepoints, Vector<String> channels, int timeintervalms) {
		active_ = false;
		core_ = core;
		location_ = location;
		name_ = name;
		timeLapse_ = timelapse;
		timePoints_ = timepoints;
		timeInterval_ = timeintervalms;
		channels_ = channels;
		stateMutex_ = new Object();
	}

	/**
	 * Thread body
	 */
	@Override public void run() {
		active_ = true;
		current_ = 0;
		total_ = (channels_.isEmpty() ? 1 : channels_.size()) * timePoints_;
		if(total_ <= 0 || timeInterval_ < 0 || name_.isEmpty() || location_.isEmpty()) {
			System.out.println("Acquisition cancelled. Invalid parameters");
			return;
		}

		try {
			// Create the dataset
			LongVector shape = new LongVector();
			shape.add(0);
			shape.add(timePoints_);
			shape.add(channels_.isEmpty() ? 1 : channels_.size());
			shape.add((int)core_.getImageWidth());
			shape.add((int)core_.getImageHeight());
			String handle = core_.createDataset(location_, name_, shape, StorageDataType.StorageDataType_GRAY16, "");

			// Acquire images
			if(timeLapse_)
				runTimeLapse(handle);
			else
				runAcquisition(handle);

			// Close the dataset
			core_.closeDataset(handle);

			// Notify that the acquisition is complete
			notifyListenersComplete();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Cancel job
	 */
	public void cancel() {
		synchronized(stateMutex_) {
			if(!active_)
				return;
			active_ = false;
		}
	}

	/**
	 * Run time-lapse sequence acquisition
	 * @param handle Dataset UID
	 */
	protected void runTimeLapse(String handle) throws Exception {
		int numberOfChannels = channels_.isEmpty() ? 1 : channels_.size();
		for(int j = 0; j < timePoints_; j++) {
			long tpStart = System.nanoTime();
			for(int k = 0; k < numberOfChannels; k++) {
				// Check cancellation token
				synchronized(stateMutex_) {
					if(!active_)
						break;
				}
				core_.snapImage();
				TaggedImage img = core_.getTaggedImage();

				// create coordinates for the image
				LongVector coords = new LongVector();
				coords.add(0);
				coords.add(j);
				coords.add(k);
				coords.add(0);
				coords.add(0);

				// Add image index to the image metadata
				img.tags.put("Image-index", current_);

				// Add image to stream
				short[] bx = (short[])img.pix;
				core_.addImage(handle, bx.length, (short[])img.pix, coords, img.tags.toString());

				// Update acquisition progress
				current_++;
				notifyListenersStatusUpdate();
			}
			if(!active_)
				break;
			long tpEnd = System.nanoTime();
			int sleepMs = timeInterval_ - (int)((tpEnd - tpStart) / 1000000.0);
			Thread.sleep(sleepMs);
		}
	}

	/**
	 * Run continous sequence acquisition
	 * @param handle Dataset UID
	 */
	protected void runAcquisition(String handle) throws Exception {
		int numberOfChannels = channels_.isEmpty() ? 1 : channels_.size();
		core_.startSequenceAcquisition(total_, 0.0, true);
		for(int j = 0; j < timePoints_; j++) {
			if(core_.getBufferFreeCapacity() < numberOfChannels * 10)
				System.out.printf("\nWARNING!!! Low buffer space %d / %d\n\n", core_.getBufferFreeCapacity(), core_.getBufferTotalCapacity());
			for(int k = 0; k < numberOfChannels; k++) {

				if(core_.isBufferOverflowed()) {
					System.out.println("Buffer overflow!!");
					break;
				}

				// Check cancellation token
				synchronized(stateMutex_) {
					if(!active_)
						break;
				}
				while(core_.getRemainingImageCount() == 0) { }

				// fetch the image
				TaggedImage img = core_.popNextTaggedImage();

				// create coordinates for the image
				LongVector coords = new LongVector();
				coords.add(0);
				coords.add(j);
				coords.add(k);
				coords.add(0);
				coords.add(0);

				// Add image index to the image metadata
				img.tags.put("Image-index", current_);

				// Add image to stream
				short[] bx = (short[])img.pix;
				core_.addImage(handle, bx.length, (short[])img.pix, coords, img.tags.toString());

				// Update acquisition progress
				current_++;
				notifyListenersStatusUpdate();
			}
			if(core_.isBufferOverflowed() || !active_)
				break;
		}

		// we are done so close the dataset
		core_.stopSequenceAcquisition();
	}

	/**
	 * Add event listener
	 * @param listener Listener object
	 */
	public final synchronized void addListener(final AcqRunnerListener listener) {
		listeners_.add(listener);
	}

	/**
	 * Remove event listener
	 * @param listener Listener object
	 */
	public final synchronized void removeListener(final AcqRunnerListener listener) {
		listeners_.remove(listener);
	}

	/**
	 * Notify event listeners that the acquisition has been completed
	 */
	private synchronized void notifyListenersComplete() {
		for(AcqRunnerListener listener : listeners_)
			listener.notifyWorkCompleted();
	}

	/**
	 * Notify event listeners of acquisition status update
	 */
	private synchronized void notifyListenersStatusUpdate() {
		for(AcqRunnerListener listener : listeners_)
			listener.notifyStatusUpdate(current_, total_);
	}
}

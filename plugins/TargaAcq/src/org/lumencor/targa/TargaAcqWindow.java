package org.lumencor.targa;

import mmcorej.CMMCore;

import javax.swing.*;
import javax.swing.event.ListSelectionEvent;
import java.awt.*;
import java.awt.event.*;
import java.io.File;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Vector;
import java.util.prefs.Preferences;

import org.micromanager.internal.utils.FileDialogs;

/**
 * Targa acquisition plugin main window
 * @author Milos Jovanovic <milos@tehnocad.rs>
 */
public class TargaAcqWindow extends JFrame implements AcqRunnerListener {
	private final static String CFG_LOCATION = "DataDir";
	private final static String CFG_NAME = "NAME";
	private final static String CFG_TIMEPOINTS = "TimePoints";
	private final static String CFG_TIMELAPSE = "TimeLapse";
	private final static String CFG_TIMEINTERVAL = "TimeInterval";
	private final static String CFG_CHANNELS = "Channels";
	private final static String CFG_WNDX = "WndX";
	private final static String CFG_WNDY = "WndY";

	private final JTextField tbLocation_;
	private final JTextField tbName_;
	private final JFormattedTextField tbTimePoints_;
	private final JFormattedTextField tbTimeInterval_;
	private final JCheckBox cbTimeLapse_;
	private final JButton loadButton_;
	private final JButton chooseLocationButton_;
	private final JButton startAcqButton_;
	private final JButton stopAcqButton_;
	private final JButton channelAddButton_;
	private final JButton channelRemoveButton_;
	private final JButton channelUpButton_;
	private final JButton channelDownButton_;
	private final JList<String> listChannels_;
	private final JLabel labelDataSize_;
	private final JLabel labelDuration_;
	private final JLabel labelExposure_;
	private final JLabel labelFramerate_;
	private final JLabel labelImageSize_;
	private final JLabel labelPixelSize_;
	private final JProgressBar progressBar_;

	private final CMMCore core_;
	private final Vector<String> channels_;
	private String dataDir_;
	private String acqName_;
	private int timePoints_;
	private int timeIntervalMs_;
	private boolean runnerActive_;
	private AcqRunner worker_;

	/**
	 * Class constructor
	 * @param studio Micro-manager app handle
	 */
	TargaAcqWindow(CMMCore studio) {
		super();
		runnerActive_ = false;
		core_ = studio;
		channels_ = new Vector<>();

		// Set window properties
		super.addWindowListener(new WindowAdapter() {
			@Override
			public void windowClosing(final WindowEvent e) {
				saveSettings();
			}
		});
		super.setTitle("Targa Acquisition " + TargaAcqPlugin.VERSION_INFO);
		super.setIconImage(Toolkit.getDefaultToolkit().getImage(getClass().getResource("/org/micromanager/icons/microscope.gif")));
		super.setResizable(false);
		super.setPreferredSize(new Dimension(800, 500));
		super.setBounds(400, 200, 800, 500);
		super.pack();

		// Set layout manager
		SpringLayout layout = new SpringLayout();
		Container contentPane = super.getContentPane();
		contentPane.setLayout(layout);

		// Set common event handlers
		FocusListener focusListener = new FocusListener() {
			@Override
			public void focusGained(FocusEvent e) { }

			@Override
			public void focusLost(FocusEvent e) {
				applySettingsFromUI();
			}
		};

		NumberFormat integerFieldFormatter = NumberFormat.getIntegerInstance();
		integerFieldFormatter.setMaximumFractionDigits(0);

		// Add UI components
		// Add acquisition location label
		final JLabel labelDir = new JLabel("Project Directory");
		contentPane.add(labelDir);
		layout.putConstraint(SpringLayout.WEST, labelDir, 20, SpringLayout.WEST, contentPane);
		layout.putConstraint(SpringLayout.NORTH, labelDir, 12, SpringLayout.NORTH, contentPane);

		// Add acquisition location field
		tbLocation_ = new JTextField();
		tbLocation_.addFocusListener(focusListener);
		contentPane.add(tbLocation_);
		layout.putConstraint(SpringLayout.WEST, tbLocation_, 170, SpringLayout.WEST, contentPane);
		layout.putConstraint(SpringLayout.EAST, tbLocation_, -50, SpringLayout.EAST, contentPane);
		layout.putConstraint(SpringLayout.NORTH, tbLocation_, 10, SpringLayout.NORTH, contentPane);

		chooseLocationButton_ = new JButton("...");
		chooseLocationButton_.setToolTipText("Browse");
		chooseLocationButton_.setMargin(new Insets(2, 5, 2, 5));
		chooseLocationButton_.setFont(new Font("Dialog", Font.PLAIN, 12));
		chooseLocationButton_.addActionListener((final ActionEvent e) -> setSaveLocation());
		contentPane.add(chooseLocationButton_);
		layout.putConstraint(SpringLayout.WEST, chooseLocationButton_, 5, SpringLayout.EAST, tbLocation_);
		layout.putConstraint(SpringLayout.NORTH, chooseLocationButton_, 2, SpringLayout.NORTH, tbLocation_);

		// Add acquisition name label
		final JLabel labelName = new JLabel("File Name");
		contentPane.add(labelName);
		layout.putConstraint(SpringLayout.WEST, labelName, 20, SpringLayout.WEST, contentPane);
		layout.putConstraint(SpringLayout.NORTH, labelName, 42, SpringLayout.NORTH, contentPane);

		// Add acquisition name field
		tbName_ = new JTextField();
		tbName_.addFocusListener(focusListener);
		contentPane.add(tbName_);
		layout.putConstraint(SpringLayout.WEST, tbName_, 0, SpringLayout.WEST, tbLocation_);
		layout.putConstraint(SpringLayout.EAST, tbName_, 0, SpringLayout.EAST, tbLocation_);
		layout.putConstraint(SpringLayout.NORTH, tbName_, 40, SpringLayout.NORTH, contentPane);

		// Add load acquisition button
		loadButton_ = new JButton("Load Acquisition");
		loadButton_.setToolTipText("Load Existing Acquisition Data");
		loadButton_.setMargin(new Insets(5, 15, 5, 15));
		loadButton_.addActionListener((final ActionEvent e) -> loadAcquisition());
		contentPane.add(loadButton_);
		layout.putConstraint(SpringLayout.EAST, loadButton_, 0, SpringLayout.EAST, tbName_);
		layout.putConstraint(SpringLayout.NORTH, loadButton_, 10, SpringLayout.SOUTH, tbName_);

		// Add timepoints label
		final JLabel labelTimepoints = new JLabel("Timepoints");
		contentPane.add(labelTimepoints);
		layout.putConstraint(SpringLayout.WEST, labelTimepoints, 20, SpringLayout.WEST, contentPane);
		layout.putConstraint(SpringLayout.NORTH, labelTimepoints, 142, SpringLayout.NORTH, contentPane);

		// Add timepoints count text box
		tbTimePoints_ = new JFormattedTextField(integerFieldFormatter);
		tbTimePoints_.addFocusListener(focusListener);
		contentPane.add(tbTimePoints_);
		layout.putConstraint(SpringLayout.WEST, tbTimePoints_, 0, SpringLayout.WEST, tbLocation_);
		layout.putConstraint(SpringLayout.EAST, tbTimePoints_, 80, SpringLayout.WEST, tbTimePoints_);
		layout.putConstraint(SpringLayout.NORTH, tbTimePoints_, -2, SpringLayout.NORTH, labelTimepoints);

		// Add time lapse check box
		cbTimeLapse_ = new JCheckBox("Time Lapse");
		cbTimeLapse_.setHorizontalTextPosition(SwingConstants.LEADING);
		layout.putConstraint(SpringLayout.WEST, cbTimeLapse_, 50, SpringLayout.EAST, tbTimePoints_);
		layout.putConstraint(SpringLayout.NORTH, cbTimeLapse_, -3, SpringLayout.NORTH, labelTimepoints);
		contentPane.add(cbTimeLapse_);

		// Add time interval label
		final JLabel labelTimeInterval = new JLabel("Interval [ms]");
		contentPane.add(labelTimeInterval);
		layout.putConstraint(SpringLayout.WEST, labelTimeInterval, 50, SpringLayout.EAST, cbTimeLapse_);
		layout.putConstraint(SpringLayout.NORTH, labelTimeInterval, 0, SpringLayout.NORTH, labelTimepoints);

		// Add time interval text box
		tbTimeInterval_ = new JFormattedTextField(integerFieldFormatter);
		tbTimeInterval_.addFocusListener(focusListener);
		contentPane.add(tbTimeInterval_);
		layout.putConstraint(SpringLayout.WEST, tbTimeInterval_, 10, SpringLayout.EAST, labelTimeInterval);
		layout.putConstraint(SpringLayout.EAST, tbTimeInterval_, 80, SpringLayout.WEST, tbTimeInterval_);
		layout.putConstraint(SpringLayout.NORTH, tbTimeInterval_, 0, SpringLayout.NORTH, tbTimePoints_);

		// Add channels configuration
		final JLabel labelChannels = new JLabel("Channels");
		contentPane.add(labelChannels);
		layout.putConstraint(SpringLayout.WEST, labelChannels, 0, SpringLayout.WEST, labelTimepoints);
		layout.putConstraint(SpringLayout.NORTH, labelChannels, 20, SpringLayout.SOUTH, labelTimepoints);

		// Add channels list
		listChannels_ = new JList<>();
		listChannels_.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
		listChannels_.setLayoutOrientation(JList.VERTICAL);
		listChannels_.setVisibleRowCount(-1);
		listChannels_.addListSelectionListener((ListSelectionEvent e) -> updateChannelCommands());
		JScrollPane listScroller = new JScrollPane(listChannels_);
		listScroller.setPreferredSize(new Dimension(180, 150));
		contentPane.add(listScroller);
		layout.putConstraint(SpringLayout.WEST, listScroller, 0, SpringLayout.WEST, tbTimePoints_);
		layout.putConstraint(SpringLayout.NORTH, listScroller, -2, SpringLayout.NORTH, labelChannels);

		// Add channels list command buttons -> ADD
		channelAddButton_ = new JButton("+");
		channelAddButton_.setToolTipText("Add Channel");
		channelAddButton_.setMargin(new Insets(2, 5, 2, 5));
		channelAddButton_.setPreferredSize(new Dimension(48, 24));
		channelAddButton_.addActionListener((final ActionEvent e) -> addChannel());
		contentPane.add(channelAddButton_);
		layout.putConstraint(SpringLayout.WEST, channelAddButton_, 5, SpringLayout.EAST, listScroller);
		layout.putConstraint(SpringLayout.NORTH, channelAddButton_, 0, SpringLayout.NORTH, listScroller);

		// Add channels list command buttons -> REMOVE
		channelRemoveButton_ = new JButton("-");
		channelRemoveButton_.setToolTipText("Remove Channel");
		channelRemoveButton_.setMargin(new Insets(2, 5, 2, 5));
		channelRemoveButton_.setPreferredSize(new Dimension(48, 24));
		channelRemoveButton_.addActionListener((final ActionEvent e) -> removeChannel());
		channelRemoveButton_.setEnabled(false);
		contentPane.add(channelRemoveButton_);
		layout.putConstraint(SpringLayout.WEST, channelRemoveButton_, 0, SpringLayout.WEST, channelAddButton_);
		layout.putConstraint(SpringLayout.NORTH, channelRemoveButton_, 5, SpringLayout.SOUTH, channelAddButton_);

		// Add channels list command buttons -> MOVE UP
		channelUpButton_ = new JButton("UP");
		channelUpButton_.setToolTipText("Move Channel UP");
		channelUpButton_.setMargin(new Insets(2, 5, 2, 5));
		channelUpButton_.setPreferredSize(new Dimension(48, 24));
		channelUpButton_.addActionListener((final ActionEvent e) -> moveChannelUp());
		channelUpButton_.setEnabled(false);
		contentPane.add(channelUpButton_);
		layout.putConstraint(SpringLayout.WEST, channelUpButton_, 0, SpringLayout.WEST, channelAddButton_);
		layout.putConstraint(SpringLayout.NORTH, channelUpButton_, 5, SpringLayout.SOUTH, channelRemoveButton_);

		// Add channels list command buttons -> MOVE DOWN
		channelDownButton_ = new JButton("DN");
		channelDownButton_.setToolTipText("Move Channel Down");
		channelDownButton_.setMargin(new Insets(2, 5, 2, 5));
		channelDownButton_.setPreferredSize(new Dimension(48, 24));
		channelDownButton_.addActionListener((final ActionEvent e) -> moveChannelDown());
		channelDownButton_.setEnabled(false);
		contentPane.add(channelDownButton_);
		layout.putConstraint(SpringLayout.WEST, channelDownButton_, 0, SpringLayout.WEST, channelAddButton_);
		layout.putConstraint(SpringLayout.NORTH, channelDownButton_, 5, SpringLayout.SOUTH, channelUpButton_);

		// Add info labels
		labelDataSize_ = new JLabel("Dataset size: -");
		contentPane.add(labelDataSize_);
		layout.putConstraint(SpringLayout.WEST, labelDataSize_, 100, SpringLayout.EAST, channelAddButton_);
		layout.putConstraint(SpringLayout.NORTH, labelDataSize_, 0, SpringLayout.NORTH, listScroller);

		labelDuration_ = new JLabel("Dataset duration: -");
		contentPane.add(labelDuration_);
		layout.putConstraint(SpringLayout.WEST, labelDuration_, 0, SpringLayout.WEST, labelDataSize_);
		layout.putConstraint(SpringLayout.NORTH, labelDuration_, 10, SpringLayout.SOUTH, labelDataSize_);

		labelExposure_ = new JLabel("Camera exposure: -");
		contentPane.add(labelExposure_);
		layout.putConstraint(SpringLayout.WEST, labelExposure_, 0, SpringLayout.WEST, labelDataSize_);
		layout.putConstraint(SpringLayout.NORTH, labelExposure_, 10, SpringLayout.SOUTH, labelDuration_);

		labelFramerate_ = new JLabel("Frame rate: -");
		contentPane.add(labelFramerate_);
		layout.putConstraint(SpringLayout.WEST, labelFramerate_, 0, SpringLayout.WEST, labelDataSize_);
		layout.putConstraint(SpringLayout.NORTH, labelFramerate_, 10, SpringLayout.SOUTH, labelExposure_);

		labelImageSize_ = new JLabel("Image size: -");
		contentPane.add(labelImageSize_);
		layout.putConstraint(SpringLayout.WEST, labelImageSize_, 0, SpringLayout.WEST, labelDataSize_);
		layout.putConstraint(SpringLayout.NORTH, labelImageSize_, 10, SpringLayout.SOUTH, labelFramerate_);

		labelPixelSize_ = new JLabel("Pixel size: -");
		contentPane.add(labelPixelSize_);
		layout.putConstraint(SpringLayout.WEST, labelPixelSize_, 0, SpringLayout.WEST, labelDataSize_);
		layout.putConstraint(SpringLayout.NORTH, labelPixelSize_, 10, SpringLayout.SOUTH, labelImageSize_);

		// Add start acquisition button
		startAcqButton_ = new JButton("Start Acquisition");
		startAcqButton_.setToolTipText("Start Data Acquisition");
		startAcqButton_.setMargin(new Insets(5, 15, 5, 15));
		startAcqButton_.addActionListener((final ActionEvent e) -> startAcquisition());
		contentPane.add(startAcqButton_);
		layout.putConstraint(SpringLayout.WEST, startAcqButton_, 300, SpringLayout.WEST, contentPane);
		layout.putConstraint(SpringLayout.EAST, startAcqButton_, -300, SpringLayout.EAST, contentPane);
		layout.putConstraint(SpringLayout.SOUTH, startAcqButton_, -20, SpringLayout.SOUTH, contentPane);

		// Add stop acquisition button
		stopAcqButton_ = new JButton("Stop Acquisition");
		stopAcqButton_.setToolTipText("Start Data Acquisition");
		stopAcqButton_.setMargin(new Insets(5, 15, 5, 15));
		stopAcqButton_.addActionListener((final ActionEvent e) -> stopAcquisition());
		stopAcqButton_.setPreferredSize(new Dimension(170, 35));
		stopAcqButton_.setVisible(false);
		contentPane.add(stopAcqButton_);
		layout.putConstraint(SpringLayout.EAST, stopAcqButton_, -20, SpringLayout.EAST, contentPane);
		layout.putConstraint(SpringLayout.SOUTH, stopAcqButton_, -20, SpringLayout.SOUTH, contentPane);

		// Add progress bar
		progressBar_ = new JProgressBar();
		progressBar_.setValue(0);
		progressBar_.setStringPainted(true);
		progressBar_.setString("0 / 0 (0%)");
		progressBar_.setVisible(false);
		contentPane.add(progressBar_);
		layout.putConstraint(SpringLayout.WEST, progressBar_, 20, SpringLayout.WEST, contentPane);
		layout.putConstraint(SpringLayout.EAST, progressBar_, -40, SpringLayout.WEST, stopAcqButton_);
		layout.putConstraint(SpringLayout.SOUTH, progressBar_, -23, SpringLayout.SOUTH, contentPane);

		loadSettings();
		updateAcqInfo(true);
	}

	/**
	 * Release the window
	 */
	public void dispose() {
		saveSettings();
	}

	/**
	 * Save plugin settings
	 */
	protected void saveSettings() {
		Preferences prefs = Preferences.userRoot().node(this.getClass().getName());
		if(prefs == null)
			return;
		StringBuilder chlist = new StringBuilder();
		for(String s : channels_)
			chlist.append((chlist.length() == 0) ? "" : ",").append(s);

		prefs.put(CFG_LOCATION, dataDir_);
		prefs.put(CFG_NAME, acqName_);
		prefs.putInt(CFG_TIMEPOINTS, timePoints_);
		prefs.putBoolean(CFG_TIMELAPSE, cbTimeLapse_.isSelected());
		prefs.putInt(CFG_TIMEINTERVAL, timeIntervalMs_);
		prefs.put(CFG_CHANNELS, chlist.toString());
		prefs.putInt(CFG_WNDX, super.getBounds().x);
		prefs.putInt(CFG_WNDY, super.getBounds().y);
	}

	/**
	 * Load plugin settings
	 */
	protected void loadSettings() {
		Preferences prefs = Preferences.userRoot().node(this.getClass().getName());
		if(prefs == null)
			return;

		// Load values
		dataDir_ = prefs.get(CFG_LOCATION, "");
		acqName_ = prefs.get(CFG_NAME, "");
		timePoints_ = prefs.getInt(CFG_TIMEPOINTS, 1);
		timeIntervalMs_ = prefs.getInt(CFG_TIMEINTERVAL, 100);
		boolean tl = prefs.getBoolean(CFG_TIMELAPSE, false);
		int wndx = prefs.getInt(CFG_WNDX, super.getBounds().x);
		int wndy = prefs.getInt(CFG_WNDY, super.getBounds().y);
		String chlist = prefs.get(CFG_CHANNELS, "");
		channels_.clear();
		if(!chlist.isEmpty())
			channels_.addAll(Arrays.asList(chlist.split(",")));

		// Update UI
		tbLocation_.setText(dataDir_);
		tbName_.setText(acqName_);
		tbTimePoints_.setValue(timePoints_);
		cbTimeLapse_.setSelected(tl);
		tbTimeInterval_.setValue(timeIntervalMs_);
		listChannels_.setListData(channels_);
		super.setLocation(wndx, wndy);
	}

	/**
	 * Load existing data acquisition
	 */
	protected void loadAcquisition() {

	}

	/**
	 * Start data acquisition
	 */
	protected void startAcquisition() {
		runnerActive_ = true;
		worker_ = new AcqRunner(core_, dataDir_, acqName_, cbTimeLapse_.isSelected(), timePoints_, channels_, timeIntervalMs_);
		worker_.addListener(this);
		worker_.start();
		updateFormState();
		updateChannelCommands();
	}

	/**
	 * Stop / cancel data acquisition
	 */
	protected void stopAcquisition() {
		runnerActive_ = false;
		worker_.cancel();
		try {
			if(worker_.isAlive())
				worker_.join();
		}catch(InterruptedException e) {
			e.printStackTrace();
		}
		worker_ = null;
		updateFormState();
		updateChannelCommands();
	}

	/**
	 * Add channel
	 */
	protected void addChannel() {
		// Obtain channels list
		String[] channelSource = { "CYAN", "GREEN", "RED", "UV", "TEAL" }; // TODO: Obtain channels from the light engine device adapter
		Vector<String> allchannels = new Vector<>();
		for(String s : channelSource) {
			if(channels_.contains(s))
				continue;
			allchannels.add(s);
		}

		// Show channel selection dialog
		ChannelSelectionDlg wnd = new ChannelSelectionDlg(this, allchannels);
		wnd.setVisible(true);

		String schannel = wnd.getSelectedChannel();
		if(schannel == null || schannel.isEmpty())
			return;
		channels_.add(schannel);
		listChannels_.setListData(channels_);
		updateAcqInfo(false);
	}

	/**
	 * Remove selected channel
	 */
	protected void removeChannel() {
		if(listChannels_.getSelectedIndex() < 0 || listChannels_.getSelectedIndex() >= channels_.size())
			return;

		channels_.remove(listChannels_.getSelectedIndex());
		listChannels_.setListData(channels_);
		updateAcqInfo(false);
	}

	/**
	 * Move selected channel up
	 */
	protected void moveChannelUp() {
		int ind = listChannels_.getSelectedIndex();
		if(ind < 1 || ind >= channels_.size())
			return;

		// Switch channels
		String tmp = channels_.get(ind);
		channels_.set(ind, channels_.get(ind - 1));
		channels_.set(ind - 1, tmp);

		// Update UI
		listChannels_.setListData(channels_);
		listChannels_.setSelectedIndex(ind - 1);
	}

	/**
	 * Move selected channel down
	 */
	protected void moveChannelDown() {
		int ind = listChannels_.getSelectedIndex();
		if(ind < 0 || ind >= channels_.size() - 1)
			return;

		// Switch channels
		String tmp = channels_.get(ind);
		channels_.set(ind, channels_.get(ind + 1));
		channels_.set(ind + 1, tmp);

		// Update UI
		listChannels_.setListData(channels_);
		listChannels_.setSelectedIndex(ind + 1);
	}

	/**
	 * Update channel list command buttons
	 */
	protected void updateChannelCommands() {
		int ind = listChannels_.getSelectedIndex();
		channelAddButton_.setEnabled(!runnerActive_);
		channelRemoveButton_.setEnabled(!runnerActive_ && listChannels_.getSelectedIndex() >= 0 && !channels_.isEmpty());
		channelUpButton_.setEnabled(!runnerActive_ && listChannels_.getSelectedIndex() > 0 && !channels_.isEmpty());
		channelDownButton_.setEnabled(!runnerActive_ && listChannels_.getSelectedIndex() >= 0 && listChannels_.getSelectedIndex() < channels_.size() - 1);
	}

	/**
	 * Update UI state based on the acquisition status
	 */
	protected void updateFormState() {
		startAcqButton_.setVisible(!runnerActive_);
		stopAcqButton_.setVisible(runnerActive_);
		progressBar_.setVisible(runnerActive_);
		tbTimePoints_.setEnabled(!runnerActive_);
		tbTimeInterval_.setEnabled(!runnerActive_);
		cbTimeLapse_.setEnabled(!runnerActive_);
		tbLocation_.setEnabled(!runnerActive_);
		tbName_.setEnabled(!runnerActive_);
		loadButton_.setEnabled(!runnerActive_);
		chooseLocationButton_.setEnabled(!runnerActive_);
		listChannels_.setEnabled(!runnerActive_);
	}

	/**
	 * Update acquisition settings from UI elements
	 */
	protected void applySettingsFromUI() {
		acqName_ = tbName_.getText().replaceAll("[/\\\\*!':]", "-").trim();
		dataDir_ = tbLocation_.getText().trim();
		try {
			timePoints_ = Integer.parseInt(tbTimePoints_.getText());
		} catch(NumberFormatException e) {
			tbTimePoints_.setValue(timePoints_);
		}
		try {
			timeIntervalMs_ = Integer.parseInt(tbTimeInterval_.getText());
		} catch(NumberFormatException e) {
			tbTimeInterval_.setValue(timeIntervalMs_);
		}
		updateAcqInfo(false);
	}

	/**
	 * Update acquisition info labels
	 * @param init Is method call during initialization
	 */
	protected void updateAcqInfo(boolean init) {
		try {
			if(init)
				core_.snapImage();
			int bpp = (int)core_.getBytesPerPixel();
			int imgw = (int)core_.getImageWidth();
			int imgh = (int)core_.getImageHeight();
			double exp = core_.getExposure();
			double fps = exp == 0 ? 0 : 1000.0 / exp;
			double durms = exp * timePoints_;
			double dsizemb = imgw * imgh * bpp * timePoints_ * (channels_.isEmpty() ? 1 : channels_.size()) / (1024.0 * 1024.0);

			if(dsizemb < 1024.0)
				labelDataSize_.setText(String.format("Dataset size: %.1f MB", dsizemb));
			else
				labelDataSize_.setText(String.format("Dataset size: %.2f GB", dsizemb / 1024.0));
			if(durms < 100.0)
				labelDuration_.setText(String.format("Dataset duration: %.1f ms", durms));
			else
				labelDuration_.setText(String.format("Dataset duration: %.2f s", durms / 1000.0));
			labelExposure_.setText(String.format("Camera exposure: %.1f ms", exp));
			labelFramerate_.setText(String.format("Frame rate: %.1f FPS", fps));
			labelImageSize_.setText(String.format("Image size: %d x %d", imgw, imgh));
			labelPixelSize_.setText(String.format("Pixel size: %d bytes", bpp));
		} catch(Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Select and update data acquisition location
	 */
	protected void setSaveLocation() {
		File result = FileDialogs.openDir(this, "Please choose a directory root for image data", FileDialogs.MM_DATA_SET);
		if(result != null) {
			dataDir_ = result.getAbsolutePath().trim();
			tbLocation_.setText(dataDir_);
		}
	}

	/**
	 * Acquisition completed event handler
	 */
	@Override
	public void notifyWorkCompleted() {
		runnerActive_ = false;
		updateFormState();
		updateChannelCommands();
	}

	/**
	 * Acquisition status update event handler
	 */
	@Override
	public void notifyStatusUpdate(int curr, int total) {
		int perc = (int)Math.ceil(100.0 * (double)curr / total);
		progressBar_.setString(String.format("%d / %d (%d%%)", curr, total, perc));
	}

	/**
	 * Application entry point
	 * Should be used only for testing
	 * @param args Program arguments
	 */
	public static void main(String[] args) {
		// Parse program arguments
		String storageEngine = args.length > 0 ? args[0] : "bigtiff";
		if(!storageEngine.equals("zarr") && !storageEngine.equals("bigtiff")) {
			System.out.println("Invalid storage engine selected: " + storageEngine);
			return;
		}
		boolean directio = args.length > 1 && Integer.parseInt(args[1]) == 1;
		int flushCycle = args.length > 2 ? Integer.parseInt(args[2]) : 0;

		try {
			// Create micro-manager instance
			UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
			//MMStudio studio = new MMStudio(false, null);
			CMMCore core = new CMMCore();

			// Setup storage engine
			if(storageEngine.equals("zarr"))
				core.loadDevice("Store", "go2scope", "AcquireZarrStorage");
			else
				core.loadDevice("Store", "go2scope", "G2SBigTiffStorage");

			// Setup demo camera
			core.loadDevice("Camera", "DemoCamera", "DCam");

			// Initialize the system, this will in turn initialize each device
			core.initializeAllDevices();

			// Configure the camera device
			core.setProperty("Camera", "PixelType", "16bit");
			core.setProperty("Camera", "OnCameraCCDXSize", "4432");
			core.setProperty("Camera", "OnCameraCCDYSize", "2368");
			core.setExposure(5.0);

			// Configure the storage device
			if(storageEngine.equals("bigtiff")) {
				core.setProperty("Store", "DirectIO", directio ? 1 : 0);
				core.setProperty("Store", "FlushCycle", flushCycle);
			}

			// Create main window
			TargaAcqWindow window = new TargaAcqWindow(core);
			window.setVisible(true);
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}

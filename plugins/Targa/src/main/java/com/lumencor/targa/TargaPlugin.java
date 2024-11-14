package org.lumencor.targa;

import org.micromanager.MenuPlugin;
import org.micromanager.Studio;
import org.scijava.plugin.Plugin;
import org.scijava.plugin.SciJavaPlugin;

/**
 * Adapter between Micro-Manager 2.0 plugin API and the Targa acquisition plugin
 * @author Milos Jovanovic <milos@tehnocad.rs>
 */
@Plugin(type = MenuPlugin.class)
public class TargaPlugin implements MenuPlugin, SciJavaPlugin {
	public static final String VERSION_INFO = "1.0.0";
	private static final String COPYRIGHT_NOTICE = "Copyright (C) by Lumencor, 2024";
	private static final String DESCRIPTION = "Image acquisition for Targa Microscope";
	private static final String NAME = "Targa";

	private Studio studio_;
	private TargaAcqWindow frame_;

	@Override
	public void setContext(Studio studio) {
		studio_ = studio;
		studio_.events().registerForEvents(this);
	}

	@Override
	public String getSubMenu() {
		return "Acquisition Tools";
	}

	@Override
	public String getCopyright() {
		return COPYRIGHT_NOTICE;
	}

	@Override
	public String getHelpText() {
		return DESCRIPTION;
	}

	@Override
	public String getName() {
		return NAME;
	}

	@Override
	public String getVersion() {
		return VERSION_INFO;
	}

	@Override
	public void onPluginSelected() {
		if(frame_ == null)
			frame_ = new TargaAcqWindow(studio_.getCMMCore());
		frame_.setVisible(true);
	}
}

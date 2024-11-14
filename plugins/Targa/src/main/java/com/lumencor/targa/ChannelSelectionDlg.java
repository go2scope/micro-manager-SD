package org.lumencor.targa;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.util.Vector;

public class ChannelSelectionDlg extends JDialog {
	private String selectedChannel;
	private final JComboBox<String> channelSelector_;

	/**
	 * Class constructor
	 * @param parent Parent window
	 * @param achannels Available channels
	 */
	ChannelSelectionDlg(JFrame parent, Vector<String> achannels) {
		super(parent, "Channel Selection", true);
		setPreferredSize(new Dimension(500, 200));
		setLocationRelativeTo(parent);
		setLocation(parent.getLocation().x + 150, parent.getLocation().y + 120);
		pack();

		// Set layout manager
		SpringLayout layout = new SpringLayout();
		Container contentPane = super.getContentPane();
		contentPane.setLayout(layout);

		// Add UI components
		// Add Selection label
		JLabel labelChannels = new JLabel("Channel:");
		contentPane.add(labelChannels);
		layout.putConstraint(SpringLayout.WEST, labelChannels, 15, SpringLayout.WEST, contentPane);
		layout.putConstraint(SpringLayout.NORTH, labelChannels, 10, SpringLayout.NORTH, contentPane);

		// Add channel dropdown
		channelSelector_ = new JComboBox<>(achannels);
		contentPane.add(channelSelector_);
		layout.putConstraint(SpringLayout.WEST, channelSelector_, 10, SpringLayout.WEST, contentPane);
		layout.putConstraint(SpringLayout.EAST, channelSelector_, -10, SpringLayout.EAST, contentPane);
		layout.putConstraint(SpringLayout.NORTH, channelSelector_, 5, SpringLayout.SOUTH, labelChannels);

		// Add OK button
		JButton okButton = new JButton("OK");
		okButton.setMargin(new Insets(5, 15, 5, 15));
		okButton.addActionListener((final ActionEvent e) -> { selectedChannel = (String)channelSelector_.getSelectedItem(); this.setVisible(false); });
		okButton.setPreferredSize(new Dimension(130, 30));
		contentPane.add(okButton);
		layout.putConstraint(SpringLayout.WEST, okButton, 100, SpringLayout.WEST, contentPane);
		layout.putConstraint(SpringLayout.SOUTH, okButton, -20, SpringLayout.SOUTH, contentPane);

		// Add cancel button
		JButton cancelButton = new JButton("Cancel");
		cancelButton.setMargin(new Insets(5, 15, 5, 15));
		cancelButton.addActionListener((final ActionEvent e) -> { selectedChannel = ""; this.setVisible(false); });
		cancelButton.setPreferredSize(new Dimension(130, 30));
		contentPane.add(cancelButton);
		layout.putConstraint(SpringLayout.EAST, cancelButton, -100, SpringLayout.EAST, contentPane);
		layout.putConstraint(SpringLayout.SOUTH, cancelButton, -20, SpringLayout.SOUTH, contentPane);
	}

	/**
	 * Get selected channel
	 * @return Selected channel
	 */
	public String getSelectedChannel() { return selectedChannel; }
}

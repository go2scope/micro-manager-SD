package org.lumencor.targa;

public interface AcqRunnerListener {
	void notifyWorkCompleted();
	void notifyStatusUpdate(int curr, int total);
}

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Threadsafe helper class for showing progress in terminal
 */
public class ProgressHandler {

    AtomicInteger progressAbsolute;
    final int totalSize, stepPercent;
    int progressPercent;
    int lastMsgSize = 0;

    public ProgressHandler(int totalSize, int stepPercent) {
        this.progressAbsolute = new AtomicInteger();
        this.totalSize = totalSize;
        this.stepPercent = stepPercent;
        this.progressPercent = 0;
    }

    public void updateProgress() {
        progressAbsolute.incrementAndGet();
        if ((100.0 * progressAbsolute.get() / totalSize) - progressPercent >= stepPercent) {
            progressPercent += stepPercent;
            printProgress();
        }
    }

    public void printProgress() {
        deleteLastMsg();
        String msg = progressPercent + "%";
        lastMsgSize = msg.length();
        System.out.print(msg);
    }

    private void deleteLastMsg() {
        for (int i = 0; i < lastMsgSize; i++) {
            System.out.print("\b");
        }
    }

    public void finish() {
        deleteLastMsg();
        System.out.println("Done.");
    }
}

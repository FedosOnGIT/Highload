package nadutkin.app.replicas;

public class ControllerStatus {
    public Long timestamp;
    public byte[] answer;

    public ControllerStatus() {
        this.timestamp = -1L;
        this.answer = null;
    }

    public ControllerStatus(Long timestamp, byte[] answer) {
        this.timestamp = timestamp;
        this.answer = answer == null ? null : answer.clone();
    }
}

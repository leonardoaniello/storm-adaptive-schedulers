package cpuinfo;

public class CoreInfo {
	
	public static final String ID_PROPERTY = "processor";
	public static final String MODEL_NAME_PROPERTY = "model name";
	public static final String SPEED_PROPERTY = "cpu MHz";

	private final int id;
	private final String modelName;
	private final long speed;
	
	public CoreInfo(int id, String modelName, long speed) {
		this.id = id;
		this.modelName = modelName;
		this.speed = speed;
	}

	public int getId() {
		return id;
	}

	public String getModelName() {
		return modelName;
	}

	public long getSpeed() {
		return speed;
	}
	
	@Override
	public String toString() {
		return "ID: " + id + ", model: " + modelName + ", speed: " + speed + " Hz";
	}
}

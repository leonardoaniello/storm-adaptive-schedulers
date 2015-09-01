package cpuinfo;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class CPUInfo {
	
	private static final String CPU_INFO_FILE = "/proc/cpuinfo";
	@SuppressWarnings("unused")
	private static final String DEBUG_CPU_INFO_FILE = "d:/cpuinfo.txt";
	
	private Map<Integer, CoreInfo> cores;
	private long totalSpeed;
	
	private static CPUInfo instance = null;
	
	public synchronized static CPUInfo getInstance() {
		if (instance == null)
			instance = new CPUInfo();
		return instance;
	}
	
	public int getNumberOfCores() {
		return cores.keySet().size();
	}
	
	public CoreInfo getCoreInfo(int processor_id) {
		return cores.get(processor_id);
	}
	
	public long getTotalSpeed() {
		if (totalSpeed == -1) {
			totalSpeed = 0;
			for (CoreInfo core : cores.values())
				totalSpeed += core.getSpeed();
		}
		return totalSpeed;
	}
	
	private CPUInfo() {
		cores = new HashMap<Integer, CoreInfo>();
		totalSpeed = -1;
		try {
			loadInfo();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void loadInfo() throws Exception {
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(CPU_INFO_FILE)));
		String line = null;
		int processor = -1;
		String model_name = "";
		long speed = 0;
		while ( (line = br.readLine()) != null ) {
			if (line.indexOf(':') > -1) {
				String key = getKey(line);
				String value = getValue(line);
				
				if (key.equals(CoreInfo.ID_PROPERTY)) {
					if (processor > -1) {
						cores.put(processor, new CoreInfo(processor, model_name, speed));
						processor = -1;
						model_name = "";
						speed = 0;
					}
					processor = Integer.parseInt(value);
				}
				
				if (key.equals(CoreInfo.MODEL_NAME_PROPERTY))
					model_name = value;
				
				if (key.equals(CoreInfo.SPEED_PROPERTY))
					// speed is in MHz, need to multiply by 1024 * 1024
					speed = (long)Float.parseFloat(value) * 1024 * 1024;
			}
		}
		cores.put(processor, new CoreInfo(processor, model_name, speed));
		br.close();
	}
	
	private String getKey(String line) {
		return line.substring(0, line.indexOf(':')).trim();
	}
	
	private String getValue(String line) {
		return line.substring(line.indexOf(':') + 1).trim();
	}
}

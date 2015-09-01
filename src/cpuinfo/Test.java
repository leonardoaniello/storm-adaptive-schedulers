package cpuinfo;

public class Test {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		
		System.out.println("Number of cores: " + CPUInfo.getInstance().getNumberOfCores());
		System.out.println("Totale speed: " + CPUInfo.getInstance().getTotalSpeed() + " Hz");
		System.out.println("Cores:");
		for (int i = 0; i < CPUInfo.getInstance().getNumberOfCores(); i++)
			System.out.println(CPUInfo.getInstance().getCoreInfo(i));
	}

}

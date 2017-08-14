package py4jserver;

import py4j.GatewayServer;

public class ServerEntryPoint {

	public void ServerEntryPoint() {
	}

	public ServerLemmatiser getLemmatiser() 
		throws Exception
	{
		ServerLemmatiser output = new ServerLemmatiser();
		output.initialise();
		return output; 
	}

	public static void main(String[] args) {
		GatewayServer gatewayServer = new GatewayServer(new ServerEntryPoint());
		gatewayServer.start();
		System.out.println("Gateway Server Started");
	}
}

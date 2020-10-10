import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.*;
import java.util.*;

class MulticastGroup{
    private String ip_address;
    private Integer client_port;
    private Boolean online_status;
    private PrintWriter printWriter_to_send_message;
    private Instant register_timing;
    private Instant disconnect_timing;

    public String getIp_address() {
        return ip_address;
    }

    public void setIp_address(String ip_address) {
        this.ip_address = ip_address;
    }

    public Integer getClient_port() {
        return client_port;
    }

    public void setClient_port(Integer client_port) {
        this.client_port = client_port;
    }

    public Boolean getOnline_status() {
        return online_status;
    }

    public void setOnline_status(Boolean online_status) {
        this.online_status = online_status;
    }

    public PrintWriter getPrintWriter_to_send_message() {
        return printWriter_to_send_message;
    }

    public void setPrintWriter_to_send_message(PrintWriter printWriter_to_send_message) {
        this.printWriter_to_send_message = printWriter_to_send_message;
    }

    public Instant getRegister_timing() {
        return register_timing;
    }

    public void setRegister_timing(Instant register_timing) {
        this.register_timing = register_timing;
    }

    //constructor
    MulticastGroup(String ip_address, Integer client_port, Boolean online_status, PrintWriter printWriter_to_send_message, Instant register_timing){
        this.ip_address = ip_address;
        this.client_port = client_port;
        this.online_status = online_status;
        this.printWriter_to_send_message = printWriter_to_send_message;
        this.register_timing = register_timing;
    }

    public Instant getDisconnect_timing() {
        return disconnect_timing;
    }

    public void setDisconnect_timing(Instant disconnect_timing) {
        this.disconnect_timing = disconnect_timing;
    }
}


public class Coordinator {
    //Have a global variable for multicast group
    static Map<Integer, MulticastGroup> partcipantEntry = new HashMap<Integer, MulticastGroup>();
    //have a global variable for message timings
    static Map<Instant, String> backupMessage = new HashMap<Instant, String>();
    //persistence time threshold (td)
//    static int td;
    static long td;
    public static void main(String[] args) {
        //check if command line inputs are correct
        if (args.length != 1) {
            System.err.println(
                    "Insert in the format given ahead: Participant <Config-file>");
            System.exit(1);
        }

        //Config file
        String  configFile = args[0];

        //check if file exists
        String path = System.getProperty("user.dir") + "/" + configFile;
        Path pathofFile = Paths.get(path);
        if (!Files.exists(pathofFile)) {
            System.out.println("File does not exits.");
            System.exit(1);
        }

        int port = 0;
        File file = new File(configFile);
        try {
            Scanner scanner = new Scanner(new File(path));
            //Port number
            port = Integer.parseInt(scanner.nextLine());

            //set persistence time threshold (td) globally
            Coordinator.td = Integer.parseInt(scanner.nextLine());
            scanner.close();
        }
        catch (Exception e){
            e.printStackTrace();
        }


        //Start Coordinator on given port number
        ServerSocket coordinatorSocket = null;
        try{
            coordinatorSocket = new ServerSocket(port);
        } catch (IOException e) {
            e.printStackTrace();
        }

        //Wait for participants to connect on the port
        while (true){
            //Accept the connection from participant
            Socket participantSocket = null;
            try{
                participantSocket = coordinatorSocket.accept();
            } catch (IOException e) {
                e.printStackTrace();
            }
            //Create thread for each participant
            Thread participantThread = new ManageParicipant(participantSocket, coordinatorSocket);
            //Start the thread
            participantThread.start();
        }
    }
}

class ManageParicipant extends Thread{//change the name to ManageParticipant
    private Socket participantSocket;
    private ServerSocket coordinatorSocket;
    private int participant_id;
    //Constructor
    public ManageParicipant (Socket participantSocket, ServerSocket coordinatorSocket){
        this.participantSocket = participantSocket;
        this.coordinatorSocket = coordinatorSocket;
    }

    public int getParticipant_id() {
        return participant_id;
    }

    public void setParticipant_id(int participant_id) {
        this.participant_id = participant_id;
    }

    @Override
    public void run() {
        try {
            handleParticipantCommands();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //method to print members of multicast group
    public void print_members(){
        for (Map.Entry<Integer, MulticastGroup> entry : Coordinator.partcipantEntry.entrySet()) {
            System.out.println("Participant Id :"+entry.getKey());
            MulticastGroup multicastGroup2 = entry.getValue();
            System.out.println(" his port:"+multicastGroup2.getClient_port());
            System.out.println(" ip address:"+multicastGroup2.getIp_address());
            System.out.println(" online:"+multicastGroup2.getOnline_status());
            System.out.println();
        }
    }

    public void  handleParticipantCommands() throws IOException {
        PrintWriter toParticipant = null;
        BufferedReader fromParticipant = null;
        try {
            //Printwriter with autoflushing
            toParticipant = new PrintWriter(this.participantSocket.getOutputStream(), true);
            //read input from client
            fromParticipant = new BufferedReader(new InputStreamReader(this.participantSocket.getInputStream()));
        }catch (IOException e) {
            e.printStackTrace();
        }
        //send ack of connection
        toParticipant.println("connected");
        toParticipant.println("Initiating conversation");

        //accepts input from client
        String participantCommad;
        while ((participantCommad= fromParticipant.readLine()) != null){
            //handle register command
            if (participantCommad.equals("register")){
                //get the timing of registration
                Instant register_time = Instant.now();
                //receive the portnumber on which participant will listen for incoming messages
                int portnumber = Integer.parseInt(fromParticipant.readLine());
                //receive unique id of the participant
                int unique_id = Integer.parseInt(fromParticipant.readLine());
                //check if participant is already registered
                if (Coordinator.partcipantEntry.containsKey(unique_id)){
                    toParticipant.println("not_unique");
                    continue;
                }
                //Iterate through all the participants and check if another participant is using the same port.
                String port_flag = "null";
                for (Map.Entry<Integer, MulticastGroup> entry : Coordinator.partcipantEntry.entrySet()) {
                    MulticastGroup multicastGroupVal = entry.getValue();
                    if(multicastGroupVal.getClient_port() == portnumber){
                        port_flag = "used_port";
                    }
                }
                if (port_flag.equals("used_port")){
                    toParticipant.println("used_port");
                    continue;
                }
                //respond successful authorization
                toParticipant.println("success");

                //store the unique id in global variable
                setParticipant_id(unique_id);

                //receive ip address of the participant
                String ip_address_participant = fromParticipant.readLine();

                //create a socket for sending multicast messages to the participant
                Socket partcipantSock = new Socket(ip_address_participant, portnumber);
                //now create a printwriter for sending message to participant
                PrintWriter multicastSend = new PrintWriter(partcipantSock.getOutputStream(),true);
                //send Thread B a ack message
                multicastSend.println("initiaing convo");
                //Store the participant in the Multicast group
                MulticastGroup participantDetails = new MulticastGroup(ip_address_participant, portnumber,true,multicastSend, register_time);
                Coordinator.partcipantEntry.put(unique_id, participantDetails);
                //Send ack back that participant is successfully registered
                toParticipant.println("registered");
                continue;
            }

            //handle reconnect command
            if (participantCommad.equals("reconnect")){
                //TODO in progress
                //receive new portnumber on which participant will listen for incoming messages
                int portnumber = Integer.parseInt(fromParticipant.readLine());
                //check if the participant was already registered
                if(getParticipant_id() == 0){
                    toParticipant.println("not_registered");
                    continue;
                }
                //check if the participant is already disconnected?
                //fetch the client record using the globally stored unique id
                MulticastGroup val = Coordinator.partcipantEntry.get(getParticipant_id());
                if (val.getOnline_status()){
                    toParticipant.println("not_disconnected");
                    continue;
                }
                //check if the port is already in use by another participant
                if (val.getClient_port().equals(portnumber)){
                    toParticipant.println("used_port");
                    continue;
                }
                toParticipant.println("reconnected");
                //save the time when we received reconnect
                Instant end = Instant.now();
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                //create a socket for sending multicast messages to the participant
                Socket partcipantSock = new Socket(val.getIp_address(), portnumber);
                //now create a printwriter for sending message to participant
                PrintWriter multicastSend = new PrintWriter(partcipantSock.getOutputStream(),true);
                //send Thread B a ack message
                multicastSend.println("initiaing convo");
                //update the participant in the Multicast group
                val.setOnline_status(true);
                //update the new port number on which client is listening now.
                val.setClient_port(portnumber);
                //Updat the printwriter used to send message on the new port
                val.setPrintWriter_to_send_message(multicastSend);
                //send the ack of reconnection
//                toParticipant.println("Reconnected successfully");
                //Check the messages which need to be sent to this participant
                //get all the keys from backupMessage
                Set<Instant> keys = Coordinator.backupMessage.keySet();
                //Check the difference for all the keys and check if its within td
                for (Instant key: keys) {
                    Duration timeElapsed = Duration.between(key, end);
                    if (timeElapsed.getSeconds() < Coordinator.td){
                        //check if the registration timing of participant is prior to the message timing.
                        if (val.getRegister_timing().isBefore(key)){
                            //check if the messages in after disconnect time
                            if (val.getDisconnect_timing().isBefore(key)){
                                //fetch the message using key
                                String message = Coordinator.backupMessage.get(key);
                                //send the message to the participant
                                val.getPrintWriter_to_send_message().println(message);
                            }
                        }
                    }
                }
                continue;
            }

            //handle deregister command
            if (participantCommad.equals("deregister")){
                //check if participant is already registered
                if(getParticipant_id() == 0){
                    toParticipant.println("not_registered");
                    continue;
                }
                //first check if the participant is online
                MulticastGroup m = Coordinator.partcipantEntry.get(getParticipant_id());
                if (m.getOnline_status()){
                    //send quit indicating thread B to stop its processing
                    m.getPrintWriter_to_send_message().println("quit");
                }
                //remove the participant from multicast group
                Coordinator.partcipantEntry.remove(getParticipant_id());
                //set participant id back to 0;
                setParticipant_id(0);
                //send ack of successful deregistration
                toParticipant.println("deregistered");
                continue;
            }
            //handle disconnect command
            if (participantCommad.equals("disconnect")){
                //get the time of disconnect
                Instant disconnect_time = Instant.now();
                //store it in the global variable
                    //fetch the client record using the globally stored unique id
                    MulticastGroup val = Coordinator.partcipantEntry.get(getParticipant_id());
                 val.setDisconnect_timing(disconnect_time);
                //check if the participant is registered or not
                if(getParticipant_id() == 0){
                    toParticipant.println("not_registered");
                    continue;
                } else {
                    //check if the participant is online or not
                    MulticastGroup m = Coordinator.partcipantEntry.get(getParticipant_id());
                    if (m.getOnline_status()){
                        //send quit indicating thread B to stop its processing
                        m.getPrintWriter_to_send_message().println("quit");
                        //set online status as false indicating disconnected
                        m.setOnline_status(false);
                        //set the port to 0 indicating release of port
                        m.setClient_port(0);
                    }
                    //send ack of successful disconnection
                    toParticipant.println("disconnected");
                    continue;
                }
            }
            //handle msend command
            if (participantCommad.equals("msend")){
                //receive the whole user input and only get the message part
                String str = fromParticipant.readLine();
                String message0 = str.substring(6);
                String message =  System.lineSeparator() + message0;
                //get the time of receiving message
                Instant time = Instant.now();
                //store the message and time in the backup variable
                Coordinator.backupMessage.put(time, message);
                //send the ack receipt to participant
                toParticipant.println("message received by coordinator");
                //Iterate through all the participants and send message to online one's.
                for (Map.Entry<Integer, MulticastGroup> entry : Coordinator.partcipantEntry.entrySet()) {
                    MulticastGroup multicastGroupVal = entry.getValue();
                    if(multicastGroupVal.getOnline_status()){
                        entry.getValue().getPrintWriter_to_send_message().println(message);
                    }
                }
                continue;
            }
            //remaining garbage values
            toParticipant.println("garbage");
        }
    }

}

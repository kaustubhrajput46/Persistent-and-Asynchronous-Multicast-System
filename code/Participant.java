import javax.imageio.IIOException;
import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Scanner;
import java.util.Set;

public class Participant {
    public static long thread_id;
    //thread flag
    public static boolean thread_flag = false;
    public static void main(String[] args) throws IOException {
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

        String host = null;
        String logFile = "";
        int unique_id = 0;
        int port = 0;
        try {
            Scanner scanner = new Scanner(new File(path));
            //Participant unique id
            unique_id = Integer.parseInt(scanner.nextLine());
            //log file for the participant
            logFile = scanner.nextLine();

            //host ip and port no of the coordinator
            String line = scanner.nextLine();
            String[] str = line.split(" ");
            host = str[0];
            port = Integer.parseInt(str[1]);

            scanner.close();
        }
        catch (Exception e){
            e.printStackTrace();
        }

        //connect with Coordinator using host and port using main thread.
        Socket socket = null;
        try {
            //connect to port
            socket = new Socket(host, port);
        } catch (ConnectException e) {
            System.out.println("Connection refused: connect.\nCheck if server is up and listening.");
            System.exit(1);
        } catch (UnknownHostException e) {
            System.out.println("Unknown host.");
            System.exit(1);
        } catch (IOException e){
            System.out.println("IOException");
            System.exit(1);
        }

        //Setup communication with Coordinator
        //Printwriter with autoflushing
        PrintWriter toCoordinator = new PrintWriter(socket.getOutputStream(), true);
        //read input from coordinator
        BufferedReader fromCoordinator = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        //read user commands from command line
        BufferedReader readCmd = new BufferedReader(new InputStreamReader(System.in));

        //wait for ack from server for connection
        System.out.println(fromCoordinator.readLine());
        //Get ready to send messages to the coordinator
        String coordReply;
        String userInput;
        while ((coordReply = fromCoordinator.readLine()) != "null"){
            //Print the response from coordinator
            if(!coordReply.equals("garbage"))
                System.out.println(coordReply);
            // accept user input
            System.out.print(unique_id+"> ");
            userInput = readCmd.readLine();

            String[] str = userInput.split(" ");
            //Handle single word commands :Disconnect, Deregister
            if (str[0].equals("msend")){
                //send msend first to inform the coordinator
                toCoordinator.println(str[0]);
                //send the whole whole user input
                toCoordinator.println(userInput);
                //wait for acknowledgement from the Coordinator
                continue;
            } else
            if(str.length == 1){

                //provide input to coordinator
                if(str[0].equals("deregister")){
                    //send deregister to the coordinator
                    toCoordinator.println(str[0]);

                    //get the response of authentication at coord side
                    String status = fromCoordinator.readLine();
                    if (status.equals("not_registered")){
                        System.out.println("Member is not a part of Multicast Group. Try registering first?");
                        toCoordinator.println("garbage");
                        continue;
                    }
                    //wait for ack
                    if (status.equals("deregistered")){
                        System.out.println("Participant has been deregistered successfully");
                        toCoordinator.println("garbage");
                        continue;
                    }
                    continue;
                }
                if(str[0].equals("disconnect")){
                    //TODO in progress
                    //send disconnect to the coordinator
                    toCoordinator.println(str[0]);
                    //get the response of authentication at coord side
                    String status = fromCoordinator.readLine();
                    if (status.equals("not_registered")){
                        System.out.println("Member is not a part of Multicast Group. Try registering first?");
                        toCoordinator.println("garbage");
                        continue;
                    }
                    //wait for ack
                    if (status.equals("disconnected")){
                        System.out.println("Participant has been disconnected successfully");
                        toCoordinator.println("garbage");
                        continue;
                    }
                    continue;
                } else {
                    toCoordinator.println("garbage");
                    continue;
                }
            }
            else
                //handle two word commands :register , reconnect
                if (str.length == 2){
                    //code for register [portnumber]
                    if(str[0].equals("register")){
                        Integer portnumber = Integer.parseInt(str[1]);

                        //send register command to the Coordinator
                        toCoordinator.println(str[0]);
                        //send portnumber to the Coordinator
                        toCoordinator.println(portnumber);
                        //send unique id of the participant
                        toCoordinator.println(unique_id);
                        //receive the confirmation from Coordinator that the id is unique
                        String confirm = fromCoordinator.readLine();
                        if (confirm.equals("not_unique")){
                            System.out.println("The participant is already registered.");
                            toCoordinator.println("garbage");
                            continue;
                        } else
                        //check if the port is in use by another participant
                        if (confirm.equals("used_port")){
                            System.out.println("Port is already in use by another client. Maybe use different port?");
                            toCoordinator.println("garbage");
                            continue;
                        }

                        //Create a thread B, which will listen on [portnumber] via server socket
                        Thread threadB = new ThreadB(portnumber, logFile);
                        //start the thread
                        threadB.start();
//                        //store the thread Id globally
//                        thread_id = threadB.getId();
//                        //set the thread flag true indicating thread is up and running
//                        thread_flag = true;

                        //send ip address of the participant
                        InetAddress ip = InetAddress.getLocalHost();
                        String ip_address = ip.getHostAddress();
                        toCoordinator.println(ip_address);
                        continue;
                    }
                    if (str[0].equals("reconnect")){
                        //TODO In progress
                        //send reconnect to the coordinator
                        toCoordinator.println(str[0]);
                        Integer portnumber = Integer.parseInt(str[1]);
                        //send new port number
                        toCoordinator.println(portnumber);
                        //check if already registered
                        String status = fromCoordinator.readLine();
                        if (status.equals("not_registered")){
                            System.out.println("Member is not a part of Multicast Group. Try registering first?");
                            toCoordinator.println("garbage");
                            continue;
                        } else
                        //check if already disconnected
                        {
                            if (status.equals("not_disconnected")){
                                System.out.println("Member is a part of Multicast Group. Try disconnecting first?");
                                toCoordinator.println("garbage");
                                continue;
                            }
                        }
                        //check if the port is already in use
                        if (status.equals("used_port")){
                            System.out.println("The port is already in use by another participant. Try using another port?");
                            toCoordinator.println("garbage");
                            continue;
                        }
                        //Create a thread B, which will listen on [portnumber] via server socket
                        Thread threadB = new ThreadB(portnumber, logFile);
                        //start the thread
                        threadB.start();
                        if (status.equals("reconnected")){
                            System.out.println("Reconnected.");
                            toCoordinator.println("garbage");
                            continue;
                        }
//                        //store the thread Id globally
//                        thread_id = threadB.getId();
//                        //set the thread flag true indicating thread is up and running
//                        thread_flag = true;
                        //wait for ack
                        //TODO do we not need continue over here?
                    }
                    else {
                        toCoordinator.println("garbage");
                        continue;
                    }

                } else toCoordinator.println("garbage");
        }
    }
}
class ThreadB extends Thread {
    private int portnumber;
    private String logFile;
    public ThreadB(Integer portnumber, String logFile){
        this.portnumber = portnumber;
        this.logFile = logFile;
    }
    @Override
    public void run() {
        try {
            receiveMessage();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void receiveMessage() throws IOException {
        //Get the log file
        File file = new File(System.getProperty("user.dir") + "/" + logFile);

        //create a socket and start listening on the socket for incoming messages from Coordinator
        ServerSocket participantSocket = new ServerSocket(this.portnumber);
        //wait for the connection from coordinator
        Socket coordSocket = participantSocket.accept();
        //read input from coordinator
        BufferedReader messageFromCoordinator = new BufferedReader(new InputStreamReader(coordSocket.getInputStream()));
        //keep accepting messages until "quit" is received
        String message;
        //ignore the initiating convo message
        messageFromCoordinator.readLine();
        while ((message = messageFromCoordinator.readLine()) != "quit"){
            if (message.equals("quit")) {
                coordSocket.close();
                participantSocket.close();
                return;
            }
            //fbr to append to log file and then close writer.
            BufferedWriter fbr = new BufferedWriter(new FileWriter(file, true));
            fbr.write(message);
            fbr.close();
        }
    }
}
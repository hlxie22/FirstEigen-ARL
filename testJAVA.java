import java.net.*;
import java.io.*;

public class testJAVA {
    public static void main(String[] args) throws IOException {
        System.out.println("[SERVER] Listening on 8080...");
        ServerSocket ss = new ServerSocket(8080);
        Socket s = ss.accept();
        System.out.println("[SERVER] Client connected");
        InputStreamReader in = new InputStreamReader(s.getInputStream());
        BufferedReader bf = new BufferedReader(in);
        String str = bf.readLine();
        System.out.println("[SERVER] Client said: " + str);
        PrintWriter pr = new PrintWriter(s.getOutputStream());
        pr.println("[SERVER] Test response");
        pr.flush();
    }
}
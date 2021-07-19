package bhanu.mctest2;

import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.script.ScriptException;
import java.io.*;

@SpringBootApplication
public class consume_df {

    public static void main(String[] args) throws ScriptException, FileNotFoundException {
        String PythonPath = "C:\\Users\\bhanu\\anaconda3\\python.exe ";
        display_dataframe(PythonPath);
    }

    private static String resolvePyFilePath(String filename) {
        File file = new File("src/main/resources/" + filename);
        return file.getAbsolutePath();
    }

    public static String display_dataframe(String pythonPath) throws FileNotFoundException, ScriptException {

        try {
            String WinCmd = pythonPath + resolvePyFilePath("mastercard_code_challenge.py");
            Process p = Runtime.getRuntime().exec(WinCmd);
            BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String dataFrames = reader.readLine();
            return "<SPARKDF>" + dataFrames + "</SPARKDF>";
//        while((s = reader.readLine())!=null){
//            System.out.println(s);
        }
        catch(IOException e) {
            e.printStackTrace();
            return new String("<FAIL>Failed</FAIL>");
        }

    }
}
//
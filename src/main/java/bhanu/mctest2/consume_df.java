package bhanu.mctest2;

import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.script.*;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.StringWriter;

@SpringBootApplication
public class consume_df {
    public static void main(String[] args) throws ScriptException, FileNotFoundException {
        display_dataframe();
    }

    public static String display_dataframe() throws FileNotFoundException, ScriptException {
        StringWriter writer = new StringWriter();
        ScriptContext context = new SimpleScriptContext();
        context.setWriter(writer);

        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine engine = manager.getEngineByName("python");
        engine.eval(new FileReader(resolvePyFilePath("mastercard_code_challenge.py")), context);
        String dfs = writer.toString();
        return dfs;

    }
    private static String resolvePyFilePath(String filename) {
        File file = new File("src/main/resources/" + filename);
        return file.getAbsolutePath();
    }
}
//
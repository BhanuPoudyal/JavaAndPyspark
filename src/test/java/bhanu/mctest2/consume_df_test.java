package bhanu.mctest2;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.SimpleScriptContext;
import java.io.File;
import java.io.FileReader;
import java.io.StringWriter;
//
@RunWith(SpringRunner.class)
@SpringBootTest
public class consume_df_test {
    @Test
    public void display_dataframe() throws Exception {
        StringWriter writer = new StringWriter();
        ScriptContext context = new SimpleScriptContext();
        context.setWriter(writer);

        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine engine = manager.getEngineByName("python");
        engine.eval(new FileReader(resolvePyFilePath("mastercard_code_challenge.py")), context);
    }

    private String resolvePyFilePath(String filename) {
        File file = new File("src/resources/" + filename);
        return file.getAbsolutePath();
    }
}



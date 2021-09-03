package hotels.services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesService {

    private Properties properties= new Properties();
    private final Logger LOGGER = LoggerFactory.getLogger(getClass());

    public PropertiesService() {
        try (InputStream fileInputStream = getClass().getResourceAsStream("/application.properties")) {
            properties.load(fileInputStream);
        } catch (IOException e) {
            LOGGER.error("cant load application.properties");
            e.printStackTrace();
        }
    }

    public String getProperty(String name) {
        return properties.getProperty(name);
    }
}
